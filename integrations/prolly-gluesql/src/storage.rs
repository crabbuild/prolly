use {
    crate::{
        error::glue_error,
        layout::{
            all_indexes_prefix, all_rows_prefix, all_schemas_prefix, all_sequences_prefix,
            branch_root_name, decode_record, encode_record, function_key, functions_prefix,
            index_key, index_prefix, index_value_prefix, key_kind, metadata_key, metadata_prefix,
            row_key, row_key_parts, row_key_payload, row_prefix, schema_key, sequence_key,
            KIND_FUNCTION, KIND_ROW, KIND_SCHEMA,
        },
        Error, Result,
    },
    async_trait::async_trait,
    futures::stream,
    gluesql_core::{
        ast::{ColumnUniqueOption, IndexOperator, OrderByExpr, Statement},
        chrono::Utc,
        data::{CustomFunction as GlueFunction, Key, Schema, SchemaIndex, SchemaIndexOrd, Value},
        error::Result as GlueResult,
        executor::evaluate_stateless,
        plan::{fetch_schema_map, plan_index, plan_join, plan_primary_key, validate},
        store::{
            AlterTable, CustomFunction, CustomFunctionMut, DataRow, Index, IndexMut, MetaIter,
            Metadata, Planner, RowIter, Store as GlueStore, StoreMut, Transaction,
        },
    },
    prolly::{prefix_range, Config, ManifestStore, Mutation, Prolly, Store, Tree},
    serde::{de::DeserializeOwned, Deserialize, Serialize},
    std::{
        cmp::Ordering,
        collections::{BTreeMap, BTreeSet, HashMap},
    },
};

/// An immutable database state returned by versioning APIs.
#[derive(Clone, Debug, PartialEq)]
pub struct Version {
    branch: String,
    id: Option<VersionId>,
    tree: Box<Tree>,
}

/// A stable, printable identifier for an immutable database state.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct VersionId(String);

impl VersionId {
    /// Return the lowercase hexadecimal content identifier.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for VersionId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Logical, schema-aware changes between two complete database states.
///
/// Physical Prolly entries for indexes, sequences, and metadata are omitted.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Diff {
    /// Table catalog changes, including secondary-index definitions.
    pub schemas: Vec<SchemaChange>,
    /// Inserted, removed, and modified SQL rows.
    pub rows: Vec<RowChange>,
    /// Created, removed, and modified custom SQL functions.
    pub functions: Vec<FunctionChange>,
}

impl Diff {
    /// Return whether both database states are logically equivalent.
    pub fn is_empty(&self) -> bool {
        self.schemas.is_empty() && self.rows.is_empty() && self.functions.is_empty()
    }

    /// Return the total number of logical changes.
    pub fn len(&self) -> usize {
        self.schemas.len() + self.rows.len() + self.functions.len()
    }
}

/// A table-schema change between two database states.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum SchemaChange {
    /// A table was created.
    Added { schema: Schema },
    /// A table was dropped.
    Removed { schema: Schema },
    /// A table definition or its indexes changed.
    Modified { before: Schema, after: Schema },
}

/// A decoded row change between two database states.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum RowChange {
    /// A row was inserted.
    Added {
        table: String,
        key: Key,
        row: DataRow,
    },
    /// A row was deleted.
    Removed {
        table: String,
        key: Key,
        row: DataRow,
    },
    /// A row retained its key but its values changed.
    Modified {
        table: String,
        key: Key,
        before: DataRow,
        after: DataRow,
    },
}

/// A custom-function change between two database states.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum FunctionChange {
    /// A custom function was created.
    Added { function: GlueFunction },
    /// A custom function was dropped.
    Removed { function: GlueFunction },
    /// A custom function definition changed.
    Modified {
        before: GlueFunction,
        after: GlueFunction,
    },
}

/// Outcome of merging an incoming version into the selected branch.
#[derive(Clone, Debug, PartialEq)]
pub enum MergeResult {
    /// The selected branch now points to the merged database state.
    Applied {
        /// Newly published version, or the unchanged head for a no-op merge.
        version: Version,
        /// Logical changes applied to the selected branch head.
        changes: Diff,
    },
    /// The branch was not changed because manual resolution is required.
    Conflicted {
        /// All logical conflicts found during the merge.
        conflicts: Vec<MergeConflict>,
    },
}

impl MergeResult {
    /// Return whether the merge completed without logical conflicts.
    pub fn is_applied(&self) -> bool {
        matches!(self, Self::Applied { .. })
    }

    /// Return conflicts when the merge requires manual resolution.
    pub fn conflicts(&self) -> &[MergeConflict] {
        match self {
            Self::Applied { .. } => &[],
            Self::Conflicted { conflicts } => conflicts,
        }
    }
}

/// A decoded SQL-level conflict produced by a three-way merge.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum MergeConflict {
    /// Both sides changed the same table definition differently.
    Schema {
        table: String,
        base: Option<Schema>,
        current: Option<Schema>,
        incoming: Option<Schema>,
    },
    /// Both sides changed the same row differently.
    Row {
        table: String,
        key: Key,
        base: Option<DataRow>,
        current: Option<DataRow>,
        incoming: Option<DataRow>,
    },
    /// Both sides changed the same custom function differently.
    Function {
        name: String,
        base: Option<GlueFunction>,
        current: Option<GlueFunction>,
        incoming: Option<GlueFunction>,
    },
    /// Individually valid changes combine into an invalid SQL database state.
    Constraint {
        table: String,
        key: Option<Key>,
        reason: String,
    },
}

fn decode_schema_change(change: prolly::Diff) -> Result<SchemaChange> {
    match change {
        prolly::Diff::Added { val, .. } => Ok(SchemaChange::Added {
            schema: decode_record(&val)?,
        }),
        prolly::Diff::Removed { val, .. } => Ok(SchemaChange::Removed {
            schema: decode_record(&val)?,
        }),
        prolly::Diff::Changed { old, new, .. } => Ok(SchemaChange::Modified {
            before: decode_record(&old)?,
            after: decode_record(&new)?,
        }),
    }
}

fn decode_row_change(change: prolly::Diff) -> Result<RowChange> {
    match change {
        prolly::Diff::Added { key, val } => {
            let (table, key) = decode_row_identity(&key)?;
            Ok(RowChange::Added {
                table,
                key,
                row: DataRow::from(decode_record::<StoredRow>(&val)?),
            })
        }
        prolly::Diff::Removed { key, val } => {
            let (table, key) = decode_row_identity(&key)?;
            Ok(RowChange::Removed {
                table,
                key,
                row: DataRow::from(decode_record::<StoredRow>(&val)?),
            })
        }
        prolly::Diff::Changed { key, old, new } => {
            let (table, key) = decode_row_identity(&key)?;
            Ok(RowChange::Modified {
                table,
                key,
                before: DataRow::from(decode_record::<StoredRow>(&old)?),
                after: DataRow::from(decode_record::<StoredRow>(&new)?),
            })
        }
    }
}

fn decode_row_identity(physical_key: &[u8]) -> Result<(String, Key)> {
    let (table, encoded_key) = row_key_parts(physical_key)?;
    Ok((table, bincode::deserialize(encoded_key)?))
}

fn decode_function_change(change: prolly::Diff) -> Result<FunctionChange> {
    match change {
        prolly::Diff::Added { val, .. } => Ok(FunctionChange::Added {
            function: decode_record(&val)?,
        }),
        prolly::Diff::Removed { val, .. } => Ok(FunctionChange::Removed {
            function: decode_record(&val)?,
        }),
        prolly::Diff::Changed { old, new, .. } => Ok(FunctionChange::Modified {
            before: decode_record(&old)?,
            after: decode_record(&new)?,
        }),
    }
}

fn schema_change_name(change: &SchemaChange) -> &str {
    match change {
        SchemaChange::Added { schema } | SchemaChange::Removed { schema } => &schema.table_name,
        SchemaChange::Modified { after, .. } => &after.table_name,
    }
}

fn row_change_identity(change: &RowChange) -> (&str, &Key) {
    match change {
        RowChange::Added { table, key, .. }
        | RowChange::Removed { table, key, .. }
        | RowChange::Modified { table, key, .. } => (table, key),
    }
}

fn function_change_name(change: &FunctionChange) -> &str {
    match change {
        FunctionChange::Added { function } | FunctionChange::Removed { function } => {
            &function.func_name
        }
        FunctionChange::Modified { after, .. } => &after.func_name,
    }
}

impl Version {
    fn new(branch: String, tree: Tree) -> Self {
        let id = tree.root.as_ref().map(|root| {
            VersionId(
                root.as_bytes()
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect(),
            )
        });
        Self {
            branch,
            id,
            tree: Box::new(tree),
        }
    }

    /// Return the branch from which this state was resolved.
    pub fn branch(&self) -> &str {
        &self.branch
    }

    /// Return the content-derived identifier, or `None` for an empty state.
    pub fn id(&self) -> Option<&VersionId> {
        self.id.as_ref()
    }
}

struct ActiveTransaction {
    base: Option<Tree>,
    tree: Tree,
    dirty: bool,
    functions_before: HashMap<String, GlueFunction>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
enum StoredRow {
    Vec(Vec<Value>),
    Map(BTreeMap<String, Value>),
}

impl From<DataRow> for StoredRow {
    fn from(row: DataRow) -> Self {
        match row {
            DataRow::Vec(values) => Self::Vec(values),
            DataRow::Map(values) => Self::Map(values),
        }
    }
}

impl From<StoredRow> for DataRow {
    fn from(row: StoredRow) -> Self {
        match row {
            StoredRow::Vec(values) => Self::Vec(values),
            StoredRow::Map(values) => Self::Map(values),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct IndexEntry {
    primary_key: Key,
    index_value: Value,
    row: StoredRow,
}

#[derive(Default)]
struct LogicalState {
    schemas: BTreeMap<String, Schema>,
    rows: BTreeMap<(String, Key), StoredRow>,
    functions: BTreeMap<String, GlueFunction>,
}

fn merge_maps<K, V, F>(
    base: &BTreeMap<K, V>,
    current: &BTreeMap<K, V>,
    incoming: &BTreeMap<K, V>,
    mut conflict: F,
) -> (BTreeMap<K, V>, Vec<MergeConflict>)
where
    K: Clone + Ord,
    V: Clone + PartialEq,
    F: FnMut(&K, Option<&V>, Option<&V>, Option<&V>) -> MergeConflict,
{
    let keys = base
        .keys()
        .chain(current.keys())
        .chain(incoming.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut merged = BTreeMap::new();
    let mut conflicts = Vec::new();
    for key in keys {
        let base_value = base.get(&key);
        let current_value = current.get(&key);
        let incoming_value = incoming.get(&key);
        let selected = if current_value == incoming_value {
            current_value
        } else if current_value == base_value {
            incoming_value
        } else if incoming_value == base_value {
            current_value
        } else {
            conflicts.push(conflict(&key, base_value, current_value, incoming_value));
            current_value
        };
        if let Some(value) = selected {
            merged.insert(key, value.clone());
        }
    }
    (merged, conflicts)
}

/// A GlueSQL storage engine whose complete database state is one Prolly tree.
pub struct ProllyStorage<S>
where
    S: Store + ManifestStore,
{
    engine: Prolly<S>,
    branch: String,
    head_name: Vec<u8>,
    transaction: Option<ActiveTransaction>,
    functions: HashMap<String, GlueFunction>,
}

impl<S> ProllyStorage<S>
where
    S: Store + ManifestStore,
{
    /// Open a logical database on the default `main` branch.
    pub fn new(store: S) -> Result<Self> {
        Self::with_branch(store, "main")
    }

    /// Open a logical database on the selected branch.
    pub fn with_branch(store: S, branch: impl Into<String>) -> Result<Self> {
        let branch = branch.into();
        let head_name = branch_root_name(&branch)?;
        let engine = Prolly::new(store, Config::default());
        let mut storage = Self {
            engine,
            branch,
            head_name,
            transaction: None,
            functions: HashMap::new(),
        };
        storage.reload_function_cache()?;
        Ok(storage)
    }

    /// Return the currently selected branch.
    pub fn branch(&self) -> &str {
        &self.branch
    }

    /// Resolve the selected branch to its immutable database state.
    pub fn head(&self) -> Result<Option<Version>> {
        Ok(self
            .engine
            .load_named_root(&self.head_name)?
            .map(|tree| Version::new(self.branch.clone(), tree)))
    }

    /// Refresh connection-local state from the selected branch head.
    ///
    /// Table data and schemas are resolved from the current branch on every
    /// operation. GlueSQL's custom-function trait returns borrowed values, so
    /// functions are cached per connection and must be refreshed after another
    /// connection creates or drops one.
    pub fn refresh(&mut self) -> Result<()> {
        if self.transaction.is_some() {
            return Err(Error::TransactionState(
                "cannot refresh during a transaction",
            ));
        }
        self.reload_function_cache()
    }

    /// Create `name` at the currently selected branch state.
    pub fn create_branch(&self, name: &str) -> Result<Version> {
        let source = self
            .head()?
            .map_or_else(|| self.engine.create(), |version| *version.tree);
        let target_name = branch_root_name(name)?;
        match self
            .engine
            .compare_and_swap_named_root(&target_name, None, Some(&source))?
        {
            prolly::NamedRootUpdate::Applied => Ok(Version::new(name.to_owned(), source)),
            prolly::NamedRootUpdate::Conflict { .. } => {
                Err(Error::Branch(format!("branch {name:?} already exists")))
            }
        }
    }

    /// Switch this connection to an existing branch.
    pub fn checkout_branch(&mut self, name: &str) -> Result<()> {
        if self.transaction.is_some() {
            return Err(Error::TransactionState(
                "cannot switch branches during a transaction",
            ));
        }
        let head_name = branch_root_name(name)?;
        if self.engine.load_named_root(&head_name)?.is_none() {
            return Err(Error::Branch(format!("branch {name:?} does not exist")));
        }
        self.branch = name.to_owned();
        self.head_name = head_name;
        self.reload_function_cache()
    }

    /// Start a transaction pinned to an arbitrary immutable database version.
    ///
    /// A later write can commit only when the selected branch still points to
    /// this exact version. A read-only transaction may always be rolled back.
    pub fn checkout(&mut self, version: &Version) -> Result<()> {
        if self.transaction.is_some() {
            return Err(Error::TransactionState(
                "cannot checkout a version during a transaction",
            ));
        }
        self.branch.clone_from(&version.branch);
        self.head_name = branch_root_name(&version.branch)?;
        self.functions = self.load_functions_from_tree(&version.tree)?;
        self.transaction = Some(ActiveTransaction {
            base: Some(version.tree.as_ref().clone()),
            tree: version.tree.as_ref().clone(),
            dirty: false,
            functions_before: self.functions.clone(),
        });
        Ok(())
    }

    /// Compare two immutable database states as decoded SQL changes.
    ///
    /// Secondary-index entries, sequences, metadata records, and other
    /// physical storage details are intentionally hidden.
    pub fn diff(&self, base: &Version, other: &Version) -> Result<Diff> {
        let mut result = Diff::default();
        for change in self.engine.diff(&base.tree, &other.tree)? {
            match key_kind(change.key()) {
                Some(KIND_SCHEMA) => result.schemas.push(decode_schema_change(change)?),
                Some(KIND_ROW) => result.rows.push(decode_row_change(change)?),
                Some(KIND_FUNCTION) => {
                    result.functions.push(decode_function_change(change)?);
                }
                _ => {}
            }
        }
        result
            .schemas
            .sort_by(|left, right| schema_change_name(left).cmp(schema_change_name(right)));
        result
            .rows
            .sort_by(|left, right| row_change_identity(left).cmp(&row_change_identity(right)));
        result
            .functions
            .sort_by(|left, right| function_change_name(left).cmp(function_change_name(right)));
        Ok(result)
    }

    /// Merge `incoming` into the selected branch using `base` as their common ancestor.
    ///
    /// The current branch head is the left side of the three-way merge. Logical
    /// schema, row, and function conflicts are returned as decoded SQL values.
    /// Secondary indexes, sequences, and metadata remain private implementation
    /// details and are rebuilt before the new version is atomically published.
    ///
    /// The selected branch is changed only when there are no conflicts and its
    /// head has not advanced since the merge began.
    pub async fn merge(&mut self, base: &Version, incoming: &Version) -> Result<MergeResult> {
        if self.transaction.is_some() {
            return Err(Error::TransactionState("cannot merge during a transaction"));
        }

        let current_root = self.load_head_tree()?;
        let current_tree = current_root.clone().unwrap_or_else(|| self.engine.create());
        let current = Version::new(self.branch.clone(), current_tree.clone());

        if current.tree == incoming.tree || incoming.tree == base.tree {
            self.reload_function_cache()?;
            return Ok(MergeResult::Applied {
                version: current,
                changes: Diff::default(),
            });
        }
        if current.tree == base.tree {
            return self.publish_merge(current_root, current, incoming.tree.as_ref().clone());
        }

        let base_state = self.load_logical_state(&base.tree)?;
        let current_state = self.load_logical_state(&current.tree)?;
        let incoming_state = self.load_logical_state(&incoming.tree)?;
        let (merged_state, mut conflicts) =
            Self::merge_logical_states(&base_state, &current_state, &incoming_state);
        if conflicts.is_empty() {
            conflicts = Self::validate_logical_state(&merged_state);
        }
        if !conflicts.is_empty() {
            return Ok(MergeResult::Conflicted { conflicts });
        }

        let merged_tree = self
            .materialize_logical_state(&base.tree, &current.tree, &incoming.tree, &merged_state)
            .await?;
        self.publish_merge(current_root, current, merged_tree)
    }

    /// Atomically move the selected branch to an earlier or otherwise pinned version.
    pub fn reset(&mut self, version: &Version) -> Result<()> {
        if self.transaction.is_some() {
            return Err(Error::TransactionState(
                "cannot reset a branch during a transaction",
            ));
        }
        let current = self.load_head_tree()?;
        match self.engine.compare_and_swap_named_root(
            &self.head_name,
            current.as_ref(),
            Some(&version.tree),
        )? {
            prolly::NamedRootUpdate::Applied => self.reload_function_cache(),
            prolly::NamedRootUpdate::Conflict { .. } => Err(Error::SerializationConflict),
        }
    }

    fn publish_merge(
        &mut self,
        expected_root: Option<Tree>,
        current: Version,
        merged_tree: Tree,
    ) -> Result<MergeResult> {
        if current.tree.as_ref() == &merged_tree {
            return Ok(MergeResult::Applied {
                version: current,
                changes: Diff::default(),
            });
        }
        let merged = Version::new(self.branch.clone(), merged_tree);
        let changes = self.diff(&current, &merged)?;
        match self.engine.compare_and_swap_named_root(
            &self.head_name,
            expected_root.as_ref(),
            Some(&merged.tree),
        )? {
            prolly::NamedRootUpdate::Applied => {
                self.reload_function_cache()?;
                Ok(MergeResult::Applied {
                    version: merged,
                    changes,
                })
            }
            prolly::NamedRootUpdate::Conflict { .. } => Err(Error::SerializationConflict),
        }
    }

    fn load_logical_state(&self, tree: &Tree) -> Result<LogicalState> {
        let schemas = self
            .scan_tree_records::<Schema>(tree, &all_schemas_prefix())?
            .into_iter()
            .map(|(_, schema)| (schema.table_name.clone(), schema))
            .collect();
        let rows = self
            .scan_tree_records::<StoredRow>(tree, &all_rows_prefix())?
            .into_iter()
            .map(|(physical_key, row)| Ok((decode_row_identity(&physical_key)?, row)))
            .collect::<Result<_>>()?;
        let functions = self
            .scan_tree_records::<GlueFunction>(tree, &functions_prefix())?
            .into_iter()
            .map(|(_, function)| (function.func_name.to_uppercase(), function))
            .collect();
        Ok(LogicalState {
            schemas,
            rows,
            functions,
        })
    }

    fn merge_logical_states(
        base: &LogicalState,
        current: &LogicalState,
        incoming: &LogicalState,
    ) -> (LogicalState, Vec<MergeConflict>) {
        let (schemas, mut conflicts) = merge_maps(
            &base.schemas,
            &current.schemas,
            &incoming.schemas,
            |table, base, current, incoming| MergeConflict::Schema {
                table: table.clone(),
                base: base.cloned(),
                current: current.cloned(),
                incoming: incoming.cloned(),
            },
        );
        let (rows, row_conflicts) = merge_maps(
            &base.rows,
            &current.rows,
            &incoming.rows,
            |(table, key), base, current, incoming| MergeConflict::Row {
                table: table.clone(),
                key: key.clone(),
                base: base.cloned().map(DataRow::from),
                current: current.cloned().map(DataRow::from),
                incoming: incoming.cloned().map(DataRow::from),
            },
        );
        conflicts.extend(row_conflicts);
        let (functions, function_conflicts) = merge_maps(
            &base.functions,
            &current.functions,
            &incoming.functions,
            |name, base, current, incoming| MergeConflict::Function {
                name: name.clone(),
                base: base.cloned(),
                current: current.cloned(),
                incoming: incoming.cloned(),
            },
        );
        conflicts.extend(function_conflicts);
        (
            LogicalState {
                schemas,
                rows,
                functions,
            },
            conflicts,
        )
    }

    fn validate_logical_state(state: &LogicalState) -> Vec<MergeConflict> {
        let mut conflicts = Vec::new();
        let mut unique_values = BTreeMap::<(String, String, Key), Key>::new();

        for ((table, key), row) in &state.rows {
            let Some(schema) = state.schemas.get(table) else {
                conflicts.push(MergeConflict::Constraint {
                    table: table.clone(),
                    key: Some(key.clone()),
                    reason: "row belongs to a table that was removed".to_owned(),
                });
                continue;
            };
            let (Some(column_defs), StoredRow::Vec(values)) = (&schema.column_defs, row) else {
                if !matches!((&schema.column_defs, row), (None, StoredRow::Map(_))) {
                    conflicts.push(MergeConflict::Constraint {
                        table: table.clone(),
                        key: Some(key.clone()),
                        reason: "row representation does not match the merged schema".to_owned(),
                    });
                }
                continue;
            };
            if values.len() != column_defs.len() {
                conflicts.push(MergeConflict::Constraint {
                    table: table.clone(),
                    key: Some(key.clone()),
                    reason: format!(
                        "row has {} values but the merged schema has {} columns",
                        values.len(),
                        column_defs.len()
                    ),
                });
                continue;
            }
            for (column, value) in column_defs.iter().zip(values) {
                if let Err(error) = value
                    .validate_type(&column.data_type)
                    .and_then(|()| value.validate_null(column.nullable))
                {
                    conflicts.push(MergeConflict::Constraint {
                        table: table.clone(),
                        key: Some(key.clone()),
                        reason: format!("column {:?}: {error}", column.name),
                    });
                    continue;
                }
                let Some(unique) = column.unique else {
                    continue;
                };
                let Ok(value_key) = Key::try_from(value.clone()) else {
                    conflicts.push(MergeConflict::Constraint {
                        table: table.clone(),
                        key: Some(key.clone()),
                        reason: format!(
                            "unique column {:?} cannot be represented as a key",
                            column.name
                        ),
                    });
                    continue;
                };
                if unique == (ColumnUniqueOption { is_primary: true }) && &value_key != key {
                    conflicts.push(MergeConflict::Constraint {
                        table: table.clone(),
                        key: Some(key.clone()),
                        reason: format!(
                            "primary-key column {:?} does not match the stored row key",
                            column.name
                        ),
                    });
                }
                if matches!(value_key, Key::None) {
                    continue;
                }
                let identity = (table.clone(), column.name.clone(), value_key);
                if let Some(existing_key) = unique_values.insert(identity, key.clone()) {
                    if existing_key != *key {
                        conflicts.push(MergeConflict::Constraint {
                            table: table.clone(),
                            key: Some(key.clone()),
                            reason: format!(
                                "unique column {:?} duplicates row key {existing_key:?}",
                                column.name
                            ),
                        });
                    }
                }
            }
        }

        for ((table, key), row) in &state.rows {
            let Some(schema) = state.schemas.get(table) else {
                continue;
            };
            let (Some(column_defs), StoredRow::Vec(values)) = (&schema.column_defs, row) else {
                continue;
            };
            for foreign_key in &schema.foreign_keys {
                let Some(column_index) = column_defs
                    .iter()
                    .position(|column| column.name == foreign_key.referencing_column_name)
                else {
                    conflicts.push(MergeConflict::Constraint {
                        table: table.clone(),
                        key: Some(key.clone()),
                        reason: format!(
                            "foreign key {:?} references a missing local column",
                            foreign_key.name
                        ),
                    });
                    continue;
                };
                let Some(value) = values.get(column_index) else {
                    continue;
                };
                if matches!(value, Value::Null) {
                    continue;
                }
                let Ok(referenced_key) = Key::try_from(value.clone()) else {
                    conflicts.push(MergeConflict::Constraint {
                        table: table.clone(),
                        key: Some(key.clone()),
                        reason: format!(
                            "foreign-key column {:?} cannot be represented as a key",
                            foreign_key.referencing_column_name
                        ),
                    });
                    continue;
                };
                if !state.rows.contains_key(&(
                    foreign_key.referenced_table_name.clone(),
                    referenced_key.clone(),
                )) {
                    conflicts.push(MergeConflict::Constraint {
                        table: table.clone(),
                        key: Some(key.clone()),
                        reason: format!(
                            "foreign key {:?} cannot find {:?} in table {:?}",
                            foreign_key.name, referenced_key, foreign_key.referenced_table_name
                        ),
                    });
                }
            }
        }

        conflicts
    }

    async fn materialize_logical_state(
        &self,
        base: &Tree,
        current: &Tree,
        incoming: &Tree,
        state: &LogicalState,
    ) -> Result<Tree> {
        let merged =
            self.engine
                .merge_prefix(base, current, incoming, &all_schemas_prefix(), None)?;
        let merged = self
            .engine
            .merge_prefix(base, &merged, incoming, &all_rows_prefix(), None)?;
        let merged =
            self.engine
                .merge_prefix(base, &merged, incoming, &functions_prefix(), None)?;
        let mut mutations = BTreeMap::<Vec<u8>, Mutation>::new();
        for prefix in [
            all_sequences_prefix(),
            metadata_prefix(),
            all_indexes_prefix(),
        ] {
            let (start, end) = prefix_range(&prefix);
            for entry in self.engine.range(&merged, &start, end.as_deref())? {
                let (key, _) = entry?;
                mutations.insert(key.clone(), Mutation::Delete { key });
            }
        }

        for schema in state.schemas.values() {
            let metadata_key = metadata_key(&schema.table_name);
            let metadata = self
                .read_tree_record::<BTreeMap<String, Value>>(current, &metadata_key)?
                .or(self.read_tree_record(incoming, &metadata_key)?)
                .or(self.read_tree_record(base, &metadata_key)?)
                .unwrap_or_else(|| {
                    BTreeMap::from([(
                        "CREATED".to_owned(),
                        Value::Timestamp(Utc::now().naive_utc()),
                    )])
                });
            mutations.insert(
                metadata_key.clone(),
                Mutation::Upsert {
                    key: metadata_key,
                    val: encode_record(&metadata)?,
                },
            );

            let sequence_key = sequence_key(&schema.table_name);
            let mut sequence = [base, current, incoming]
                .into_iter()
                .map(|tree| self.read_tree_record::<i64>(tree, &sequence_key))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .flatten()
                .max()
                .unwrap_or(0);
            sequence = state
                .rows
                .keys()
                .filter_map(|(table, key)| {
                    (table == &schema.table_name)
                        .then_some(key)
                        .and_then(|key| match key {
                            Key::I64(value) => Some(*value),
                            _ => None,
                        })
                })
                .fold(sequence, i64::max);
            if sequence > 0 {
                mutations.insert(
                    sequence_key.clone(),
                    Mutation::Upsert {
                        key: sequence_key,
                        val: encode_record(&sequence)?,
                    },
                );
            }

            for ((table, primary_key), stored_row) in &state.rows {
                if table != &schema.table_name {
                    continue;
                }
                let row = DataRow::from(stored_row.clone());
                for index in &schema.indexes {
                    let mutation = Self::index_mutation(schema, index, primary_key, &row, false)
                        .await
                        .map_err(|error| Error::Merge(error.to_string()))?;
                    mutations.insert(mutation.key().to_vec(), mutation);
                }
            }
        }

        Ok(self
            .engine
            .batch(&merged, mutations.into_values().collect())?)
    }

    fn load_head_tree(&self) -> Result<Option<Tree>> {
        Ok(self.engine.load_named_root(&self.head_name)?)
    }

    fn current_tree(&self) -> Result<Tree> {
        if let Some(transaction) = &self.transaction {
            return Ok(transaction.tree.clone());
        }
        Ok(self
            .load_head_tree()?
            .unwrap_or_else(|| self.engine.create()))
    }

    fn read_record<T: DeserializeOwned>(&self, key: &[u8]) -> Result<Option<T>> {
        self.engine
            .get(&self.current_tree()?, key)?
            .map(|bytes| decode_record(&bytes))
            .transpose()
    }

    fn read_tree_record<T: DeserializeOwned>(&self, tree: &Tree, key: &[u8]) -> Result<Option<T>> {
        self.engine
            .get(tree, key)?
            .map(|bytes| decode_record(&bytes))
            .transpose()
    }

    fn scan_tree_records<T: DeserializeOwned>(
        &self,
        tree: &Tree,
        prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, T)>> {
        let (start, end) = prefix_range(prefix);
        self.engine
            .range(tree, &start, end.as_deref())?
            .map(|entry| {
                let (key, bytes) = entry?;
                Ok((key, decode_record(&bytes)?))
            })
            .collect()
    }

    fn scan_records<T: DeserializeOwned>(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, T)>> {
        let (start, end) = prefix_range(prefix);
        self.scan_record_range(&start, end.as_deref())
    }

    fn scan_record_range<T: DeserializeOwned>(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, T)>> {
        let tree = self.current_tree()?;
        self.engine
            .range(&tree, start, end)?
            .map(|entry| {
                let (key, bytes) = entry?;
                Ok((key, decode_record(&bytes)?))
            })
            .collect()
    }

    fn delete_prefix_mutations(&self, prefix: &[u8]) -> Result<Vec<Mutation>> {
        let (start, end) = prefix_range(prefix);
        let tree = self.current_tree()?;
        self.engine
            .range(&tree, &start, end.as_deref())?
            .map(|entry| {
                let (key, _) = entry?;
                Ok(Mutation::Delete { key })
            })
            .collect()
    }

    fn apply(&mut self, mutations: Vec<Mutation>) -> Result<()> {
        if mutations.is_empty() {
            return Ok(());
        }
        let transaction = self
            .transaction
            .as_mut()
            .ok_or(Error::TransactionState("write outside a transaction"))?;
        transaction.tree = self.engine.batch(&transaction.tree, mutations)?;
        transaction.dirty = true;
        Ok(())
    }

    fn reload_function_cache(&mut self) -> Result<()> {
        let tree = self
            .load_head_tree()?
            .unwrap_or_else(|| self.engine.create());
        self.functions = self.load_functions_from_tree(&tree)?;
        Ok(())
    }

    fn load_functions_from_tree(&self, tree: &Tree) -> Result<HashMap<String, GlueFunction>> {
        let prefix = functions_prefix();
        let (start, end) = prefix_range(&prefix);
        self.engine
            .range(tree, &start, end.as_deref())?
            .map(|entry| {
                let (_, bytes) = entry?;
                let function: GlueFunction = decode_record(&bytes)?;
                Ok((function.func_name.to_uppercase(), function))
            })
            .collect()
    }

    fn fetch_schema_inner(&self, table_name: &str) -> Result<Option<Schema>> {
        self.read_record(&schema_key(table_name))
    }

    fn require_schema_inner(&self, table_name: &str) -> GlueResult<Schema> {
        self.fetch_schema_inner(table_name)
            .map_err(glue_error)?
            .ok_or_else(|| {
                glue_error(format!(
                    "table {table_name:?} is missing from the database catalog"
                ))
            })
    }

    fn encoded_primary_key(key: &Key) -> GlueResult<Vec<u8>> {
        bincode::serialize(key).map_err(glue_error)
    }

    fn fetch_data_inner(&self, table_name: &str, key: &Key) -> GlueResult<Option<DataRow>> {
        let encoded_key = Self::encoded_primary_key(key)?;
        Ok(self
            .read_record::<StoredRow>(&row_key(table_name, &encoded_key))
            .map_err(glue_error)?
            .map(DataRow::from))
    }

    fn scan_data_inner(&self, table_name: &str) -> GlueResult<Vec<(Key, DataRow)>> {
        let mut rows = self
            .scan_records::<StoredRow>(&row_prefix(table_name))
            .map_err(glue_error)?
            .into_iter()
            .map(|(physical_key, row)| {
                let key_bytes = row_key_payload(table_name, &physical_key).map_err(glue_error)?;
                let key: Key = bincode::deserialize(key_bytes).map_err(glue_error)?;
                Ok((key, DataRow::from(row)))
            })
            .collect::<GlueResult<Vec<_>>>()?;
        rows.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(rows)
    }

    async fn index_value(schema: &Schema, index: &SchemaIndex, row: &DataRow) -> GlueResult<Value> {
        let columns = schema.column_defs.as_ref().map(|columns| {
            columns
                .iter()
                .map(|column| column.name.clone())
                .collect::<Vec<_>>()
        });
        evaluate_stateless(Some(row.as_context(columns.as_deref())), &index.expr)
            .await?
            .try_into()
    }

    async fn index_mutation(
        schema: &Schema,
        index: &SchemaIndex,
        primary_key: &Key,
        row: &DataRow,
        delete: bool,
    ) -> GlueResult<Mutation> {
        let index_value = Self::index_value(schema, index, row).await?;
        let comparison = index_value.to_cmp_be_bytes()?;
        let identity = bincode::serialize(&index_value).map_err(glue_error)?;
        let encoded_primary_key = Self::encoded_primary_key(primary_key)?;
        let key = index_key(
            &schema.table_name,
            &index.name,
            &comparison,
            &identity,
            &encoded_primary_key,
        );
        if delete {
            Ok(Mutation::Delete { key })
        } else {
            Ok(Mutation::Upsert {
                key,
                val: encode_record(&IndexEntry {
                    primary_key: primary_key.clone(),
                    index_value,
                    row: row.clone().into(),
                })
                .map_err(glue_error)?,
            })
        }
    }

    fn remove_table_mutations(
        &self,
        table_name: &str,
        schema: &Schema,
    ) -> GlueResult<Vec<Mutation>> {
        let mut mutations = self
            .delete_prefix_mutations(&row_prefix(table_name))
            .map_err(glue_error)?;
        for index in &schema.indexes {
            mutations.extend(
                self.delete_prefix_mutations(&index_prefix(table_name, &index.name))
                    .map_err(glue_error)?,
            );
        }
        mutations.extend([
            Mutation::Delete {
                key: schema_key(table_name),
            },
            Mutation::Delete {
                key: sequence_key(table_name),
            },
            Mutation::Delete {
                key: metadata_key(table_name),
            },
        ]);
        Ok(mutations)
    }
}

impl ProllyStorage<prolly::MemStore> {
    /// Create an in-memory database suitable for tests and ephemeral use.
    pub fn in_memory() -> Result<Self> {
        Self::new(prolly::MemStore::new())
    }
}

#[cfg(feature = "sqlite")]
pub type SqliteProllyStorage = ProllyStorage<prolly_store_sqlite::SqliteStore>;

#[cfg(feature = "sqlite")]
impl ProllyStorage<prolly_store_sqlite::SqliteStore> {
    /// Open or create a durable SQLite-backed Prolly SQL database.
    pub fn open_sqlite(path: impl AsRef<std::path::Path>) -> Result<Self> {
        Self::new(prolly_store_sqlite::SqliteStore::open(path)?)
    }

    /// Open or create a durable SQLite-backed database on the selected branch.
    pub fn open_sqlite_with_branch(
        path: impl AsRef<std::path::Path>,
        branch: impl Into<String>,
    ) -> Result<Self> {
        Self::with_branch(prolly_store_sqlite::SqliteStore::open(path)?, branch)
    }
}

#[async_trait]
impl<S> GlueStore for ProllyStorage<S>
where
    S: Store + ManifestStore,
{
    async fn fetch_schema(&self, table_name: &str) -> GlueResult<Option<Schema>> {
        self.fetch_schema_inner(table_name).map_err(glue_error)
    }

    async fn fetch_all_schemas(&self) -> GlueResult<Vec<Schema>> {
        let mut schemas = self
            .scan_records::<Schema>(&all_schemas_prefix())
            .map_err(glue_error)?
            .into_iter()
            .map(|(_, schema)| schema)
            .collect::<Vec<_>>();
        schemas.sort_by(|left, right| left.table_name.cmp(&right.table_name));
        Ok(schemas)
    }

    async fn fetch_data(&self, table_name: &str, key: &Key) -> GlueResult<Option<DataRow>> {
        self.fetch_data_inner(table_name, key)
    }

    async fn scan_data<'a>(&'a self, table_name: &str) -> GlueResult<RowIter<'a>> {
        Ok(Box::pin(stream::iter(
            self.scan_data_inner(table_name)?.into_iter().map(Ok),
        )))
    }
}

#[async_trait]
impl<S> StoreMut for ProllyStorage<S>
where
    S: Store + ManifestStore,
{
    async fn insert_schema(&mut self, schema: &Schema) -> GlueResult<()> {
        let created = BTreeMap::from([(
            "CREATED".to_owned(),
            Value::Timestamp(Utc::now().naive_utc()),
        )]);
        self.apply(vec![
            Mutation::Upsert {
                key: schema_key(&schema.table_name),
                val: encode_record(schema).map_err(glue_error)?,
            },
            Mutation::Upsert {
                key: metadata_key(&schema.table_name),
                val: encode_record(&created).map_err(glue_error)?,
            },
        ])
        .map_err(glue_error)
    }

    async fn delete_schema(&mut self, table_name: &str) -> GlueResult<()> {
        let Some(schema) = self.fetch_schema_inner(table_name).map_err(glue_error)? else {
            return Ok(());
        };
        let mutations = self.remove_table_mutations(table_name, &schema)?;
        self.apply(mutations).map_err(glue_error)
    }

    async fn append_data(&mut self, table_name: &str, rows: Vec<DataRow>) -> GlueResult<()> {
        self.require_schema_inner(table_name)?;
        let sequence_key = sequence_key(table_name);
        let mut sequence = self
            .read_record::<i64>(&sequence_key)
            .map_err(glue_error)?
            .unwrap_or(0);
        let mut keyed = Vec::with_capacity(rows.len());
        for row in rows {
            sequence = sequence
                .checked_add(1)
                .ok_or_else(|| glue_error(Error::SequenceOverflow(table_name.to_owned())))?;
            keyed.push((Key::I64(sequence), row));
        }
        self.insert_data(table_name, keyed).await?;
        self.apply(vec![Mutation::Upsert {
            key: sequence_key,
            val: encode_record(&sequence).map_err(glue_error)?,
        }])
        .map_err(glue_error)
    }

    async fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, DataRow)>) -> GlueResult<()> {
        let schema = self.require_schema_inner(table_name)?;
        let mut mutations = Vec::new();
        for (primary_key, row) in rows {
            if let Some(old_row) = self.fetch_data_inner(table_name, &primary_key)? {
                for index in &schema.indexes {
                    mutations.push(
                        Self::index_mutation(&schema, index, &primary_key, &old_row, true).await?,
                    );
                }
            }
            let encoded_key = Self::encoded_primary_key(&primary_key)?;
            mutations.push(Mutation::Upsert {
                key: row_key(table_name, &encoded_key),
                val: encode_record(&StoredRow::from(row.clone())).map_err(glue_error)?,
            });
            for index in &schema.indexes {
                mutations
                    .push(Self::index_mutation(&schema, index, &primary_key, &row, false).await?);
            }
        }
        self.apply(mutations).map_err(glue_error)
    }

    async fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> GlueResult<()> {
        let schema = self.require_schema_inner(table_name)?;
        let mut mutations = Vec::new();
        for primary_key in keys {
            if let Some(old_row) = self.fetch_data_inner(table_name, &primary_key)? {
                for index in &schema.indexes {
                    mutations.push(
                        Self::index_mutation(&schema, index, &primary_key, &old_row, true).await?,
                    );
                }
                mutations.push(Mutation::Delete {
                    key: row_key(table_name, &Self::encoded_primary_key(&primary_key)?),
                });
            }
        }
        self.apply(mutations).map_err(glue_error)
    }
}

#[async_trait]
impl<S> Transaction for ProllyStorage<S>
where
    S: Store + ManifestStore,
{
    async fn begin(&mut self, autocommit: bool) -> GlueResult<bool> {
        match (&self.transaction, autocommit) {
            (Some(_), true) => return Ok(false),
            (Some(_), false) => {
                return Err(glue_error(Error::TransactionState(
                    "nested transactions are not supported",
                )))
            }
            (None, _) => {}
        }
        let base = self.load_head_tree().map_err(glue_error)?;
        let tree = base.clone().unwrap_or_else(|| self.engine.create());
        self.transaction = Some(ActiveTransaction {
            base,
            tree,
            dirty: false,
            functions_before: self.functions.clone(),
        });
        Ok(autocommit)
    }

    async fn rollback(&mut self) -> GlueResult<()> {
        if let Some(transaction) = self.transaction.take() {
            self.functions = transaction.functions_before;
        }
        Ok(())
    }

    async fn commit(&mut self) -> GlueResult<()> {
        let Some(transaction) = self.transaction.take() else {
            return Ok(());
        };
        if !transaction.dirty {
            return Ok(());
        }
        let update = self
            .engine
            .compare_and_swap_named_root(
                &self.head_name,
                transaction.base.as_ref(),
                Some(&transaction.tree),
            )
            .map_err(glue_error)?;
        match update {
            prolly::NamedRootUpdate::Applied => Ok(()),
            prolly::NamedRootUpdate::Conflict { .. } => {
                self.functions = transaction.functions_before;
                Err(glue_error(Error::SerializationConflict))
            }
        }
    }
}

#[async_trait]
impl<S> AlterTable for ProllyStorage<S> where S: Store + ManifestStore {}

#[async_trait]
impl<S> Metadata for ProllyStorage<S>
where
    S: Store + ManifestStore,
{
    async fn scan_table_meta(&self) -> GlueResult<MetaIter> {
        let prefix = metadata_prefix();
        let entries = self
            .scan_records::<BTreeMap<String, Value>>(&prefix)
            .map_err(glue_error)?
            .into_iter()
            .map(|(key, metadata)| {
                let tail = key
                    .strip_prefix(prefix.as_slice())
                    .ok_or_else(|| glue_error("metadata key escaped its prefix"))?;
                if tail.len() < 8 {
                    return Err(glue_error("truncated metadata table segment"));
                }
                let length = usize::try_from(u64::from_be_bytes([
                    tail[0], tail[1], tail[2], tail[3], tail[4], tail[5], tail[6], tail[7],
                ]))
                .map_err(glue_error)?;
                let end = 8_usize
                    .checked_add(length)
                    .ok_or_else(|| glue_error("metadata table name length overflow"))?;
                let name = tail
                    .get(8..end)
                    .ok_or_else(|| glue_error("truncated metadata table name"))?;
                let name = String::from_utf8(name.to_vec()).map_err(glue_error)?;
                Ok((name, metadata))
            })
            .collect::<GlueResult<Vec<_>>>()?;
        Ok(Box::new(entries.into_iter().map(Ok)))
    }
}

#[async_trait]
impl<S> CustomFunction for ProllyStorage<S>
where
    S: Store + ManifestStore,
{
    async fn fetch_function<'a>(
        &'a self,
        function_name: &str,
    ) -> GlueResult<Option<&'a GlueFunction>> {
        Ok(self.functions.get(&function_name.to_uppercase()))
    }

    async fn fetch_all_functions<'a>(&'a self) -> GlueResult<Vec<&'a GlueFunction>> {
        let mut functions = self.functions.values().collect::<Vec<_>>();
        functions.sort_by(|left, right| left.func_name.cmp(&right.func_name));
        Ok(functions)
    }
}

#[async_trait]
impl<S> CustomFunctionMut for ProllyStorage<S>
where
    S: Store + ManifestStore,
{
    async fn insert_function(&mut self, function: GlueFunction) -> GlueResult<()> {
        let name = function.func_name.to_uppercase();
        self.apply(vec![Mutation::Upsert {
            key: function_key(&name),
            val: encode_record(&function).map_err(glue_error)?,
        }])
        .map_err(glue_error)?;
        self.functions.insert(name, function);
        Ok(())
    }

    async fn delete_function(&mut self, function_name: &str) -> GlueResult<()> {
        let name = function_name.to_uppercase();
        self.apply(vec![Mutation::Delete {
            key: function_key(&name),
        }])
        .map_err(glue_error)?;
        self.functions.remove(&name);
        Ok(())
    }
}

#[async_trait]
impl<S> IndexMut for ProllyStorage<S>
where
    S: Store + ManifestStore,
{
    async fn create_index(
        &mut self,
        table_name: &str,
        index_name: &str,
        column: &OrderByExpr,
    ) -> GlueResult<()> {
        let mut schema = self.require_schema_inner(table_name)?;
        if schema.indexes.iter().any(|index| index.name == index_name) {
            return Err(gluesql_core::store::IndexError::IndexNameAlreadyExists(
                index_name.to_owned(),
            )
            .into());
        }
        let index = SchemaIndex {
            name: index_name.to_owned(),
            expr: column.expr.clone(),
            order: SchemaIndexOrd::Both,
            created: Utc::now().naive_utc(),
        };
        let rows = self.scan_data_inner(table_name)?;
        let mut mutations = Vec::with_capacity(rows.len() + 1);
        for (key, row) in &rows {
            mutations.push(Self::index_mutation(&schema, &index, key, row, false).await?);
        }
        schema.indexes.push(index);
        mutations.push(Mutation::Upsert {
            key: schema_key(table_name),
            val: encode_record(&schema).map_err(glue_error)?,
        });
        self.apply(mutations).map_err(glue_error)
    }

    async fn drop_index(&mut self, table_name: &str, index_name: &str) -> GlueResult<()> {
        let Some(mut schema) = self.fetch_schema_inner(table_name).map_err(glue_error)? else {
            return Err(
                gluesql_core::store::IndexError::TableNotFound(table_name.to_owned()).into(),
            );
        };
        let before = schema.indexes.len();
        schema.indexes.retain(|index| index.name != index_name);
        if schema.indexes.len() == before {
            return Err(gluesql_core::store::IndexError::IndexNameDoesNotExist(
                index_name.to_owned(),
            )
            .into());
        }
        let mut mutations = self
            .delete_prefix_mutations(&index_prefix(table_name, index_name))
            .map_err(glue_error)?;
        mutations.push(Mutation::Upsert {
            key: schema_key(table_name),
            val: encode_record(&schema).map_err(glue_error)?,
        });
        self.apply(mutations).map_err(glue_error)
    }
}

#[async_trait]
impl<S> Index for ProllyStorage<S>
where
    S: Store + ManifestStore,
{
    async fn scan_indexed_data<'a>(
        &'a self,
        table_name: &str,
        index_name: &str,
        asc: Option<bool>,
        comparison: Option<(&IndexOperator, Value)>,
    ) -> GlueResult<RowIter<'a>> {
        let schema = self.require_schema_inner(table_name)?;
        if !schema.indexes.iter().any(|index| index.name == index_name) {
            return Err(gluesql_core::store::IndexError::IndexNameDoesNotExist(
                index_name.to_owned(),
            )
            .into());
        }
        let full_prefix = index_prefix(table_name, index_name);
        let (full_start, full_end) = prefix_range(&full_prefix);
        let (start, end) = if let Some((operator, expected)) = comparison.as_ref() {
            let exact_prefix =
                index_value_prefix(table_name, index_name, &expected.to_cmp_be_bytes()?);
            let (_, exact_end) = prefix_range(&exact_prefix);
            let exact_end = exact_end.ok_or_else(|| {
                glue_error(Error::Corrupt(
                    "index comparison prefix has no finite upper bound".to_owned(),
                ))
            })?;
            match operator {
                IndexOperator::Eq => (exact_prefix, Some(exact_end)),
                IndexOperator::Gt => (exact_end, full_end),
                IndexOperator::GtEq => (exact_prefix, full_end),
                IndexOperator::Lt => (full_start, Some(exact_prefix)),
                IndexOperator::LtEq => (full_start, Some(exact_end)),
            }
        } else {
            (full_start, full_end)
        };
        let mut entries = self
            .scan_record_range::<IndexEntry>(&start, end.as_deref())
            .map_err(glue_error)?
            .into_iter()
            .map(|(_, entry)| entry)
            .collect::<Vec<_>>();
        if let Some((operator, expected)) = comparison {
            entries.retain(|entry| compare_index_value(&entry.index_value, operator, &expected));
        }
        if asc == Some(false) {
            entries.reverse();
        }
        Ok(Box::pin(stream::iter(entries.into_iter().map(|entry| {
            Ok((entry.primary_key, DataRow::from(entry.row)))
        }))))
    }
}

fn compare_index_value(value: &Value, operator: &IndexOperator, expected: &Value) -> bool {
    let ordering = compare_index_order(value, expected);
    match operator {
        IndexOperator::Eq => ordering == Ordering::Equal,
        IndexOperator::Gt => ordering == Ordering::Greater,
        IndexOperator::GtEq => matches!(ordering, Ordering::Greater | Ordering::Equal),
        IndexOperator::Lt => ordering == Ordering::Less,
        IndexOperator::LtEq => matches!(ordering, Ordering::Less | Ordering::Equal),
    }
}

fn compare_index_order(left: &Value, right: &Value) -> std::cmp::Ordering {
    match (left, right) {
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Null, _) => std::cmp::Ordering::Greater,
        (_, Value::Null) => std::cmp::Ordering::Less,
        _ => left
            .evaluate_cmp(right)
            .unwrap_or(std::cmp::Ordering::Equal),
    }
}

#[async_trait]
impl<S> Planner for ProllyStorage<S>
where
    S: Store + ManifestStore,
{
    async fn plan(&self, statement: Statement) -> GlueResult<Statement> {
        let schema_map = fetch_schema_map(self, &statement).await?;
        validate(&schema_map, &statement)?;
        let statement = plan_primary_key(&schema_map, statement);
        let statement = plan_index(&schema_map, statement);
        Ok(plan_join(&schema_map, statement))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gluesql_core::prelude::{Glue, Payload};

    #[tokio::test]
    async fn sql_round_trip_and_rollback() {
        let storage = ProllyStorage::in_memory().unwrap();
        let mut glue = Glue::new(storage);
        glue.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);")
            .await
            .unwrap();
        glue.execute("INSERT INTO users VALUES (1, 'Ada'), (2, 'Grace');")
            .await
            .unwrap();
        let selected = glue
            .execute("SELECT name FROM users ORDER BY id;")
            .await
            .unwrap();
        assert!(matches!(&selected[0], Payload::Select { rows, .. } if rows.len() == 2));

        glue.execute("START TRANSACTION;").await.unwrap();
        glue.execute("UPDATE users SET name = 'Changed' WHERE id = 1;")
            .await
            .unwrap();
        glue.execute("ROLLBACK;").await.unwrap();
        let selected = glue
            .execute("SELECT name FROM users WHERE id = 1;")
            .await
            .unwrap();
        assert!(matches!(
            &selected[0],
            Payload::Select { rows, .. } if rows == &vec![vec![Value::Str("Ada".to_owned())]]
        ));
    }

    #[tokio::test]
    async fn branch_isolation() {
        let storage = ProllyStorage::in_memory().unwrap();
        let mut glue = Glue::new(storage);
        glue.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT);")
            .await
            .unwrap();
        glue.execute("INSERT INTO t VALUES (1, 'main');")
            .await
            .unwrap();
        glue.storage.create_branch("feature").unwrap();
        glue.storage.checkout_branch("feature").unwrap();
        glue.execute("UPDATE t SET value = 'feature' WHERE id = 1;")
            .await
            .unwrap();
        glue.storage.checkout_branch("main").unwrap();
        let selected = glue.execute("SELECT value FROM t;").await.unwrap();
        assert!(matches!(
            &selected[0],
            Payload::Select { rows, .. } if rows == &vec![vec![Value::Str("main".to_owned())]]
        ));
    }

    #[tokio::test]
    async fn secondary_index_tracks_updates() {
        let storage = ProllyStorage::in_memory().unwrap();
        let mut glue = Glue::new(storage);
        glue.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT);")
            .await
            .unwrap();
        glue.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b');")
            .await
            .unwrap();
        glue.execute("CREATE INDEX idx_value ON t (value);")
            .await
            .unwrap();
        glue.execute("UPDATE t SET value = 'c' WHERE id = 1;")
            .await
            .unwrap();
        let selected = glue
            .execute("SELECT id FROM t WHERE value = 'c';")
            .await
            .unwrap();
        assert!(matches!(
            &selected[0],
            Payload::Select { rows, .. } if rows == &vec![vec![Value::I64(1)]]
        ));
    }

    #[tokio::test]
    async fn historical_checkout_diff_and_reset() {
        let storage = ProllyStorage::in_memory().unwrap();
        let mut glue = Glue::new(storage);
        glue.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT);")
            .await
            .unwrap();
        glue.execute("INSERT INTO t VALUES (1, 'before');")
            .await
            .unwrap();
        let before = glue.storage.head().unwrap().unwrap();

        glue.execute("UPDATE t SET value = 'after' WHERE id = 1;")
            .await
            .unwrap();
        let after = glue.storage.head().unwrap().unwrap();
        assert_ne!(before.id(), after.id());
        assert_eq!(before.branch(), "main");
        assert_eq!(before.id().unwrap().as_str().len(), 64);
        let diff = glue.storage.diff(&before, &after).unwrap();
        assert_eq!(diff.schemas, Vec::<SchemaChange>::new());
        assert_eq!(diff.functions, Vec::<FunctionChange>::new());
        assert_eq!(
            diff.rows,
            vec![RowChange::Modified {
                table: "t".to_owned(),
                key: Key::I64(1),
                before: DataRow::Vec(vec![Value::I64(1), Value::Str("before".to_owned()),]),
                after: DataRow::Vec(vec![Value::I64(1), Value::Str("after".to_owned()),]),
            }]
        );

        glue.storage.checkout(&before).unwrap();
        let selected = glue.execute("SELECT value FROM t;").await.unwrap();
        assert!(matches!(
            &selected[0],
            Payload::Select { rows, .. }
                if rows == &vec![vec![Value::Str("before".to_owned())]]
        ));
        glue.execute("ROLLBACK;").await.unwrap();

        glue.storage.reset(&before).unwrap();
        let selected = glue.execute("SELECT value FROM t;").await.unwrap();
        assert!(matches!(
            &selected[0],
            Payload::Select { rows, .. }
                if rows == &vec![vec![Value::Str("before".to_owned())]]
        ));
    }

    #[tokio::test]
    async fn clean_merge_rebuilds_indexes_and_advances_sequences() {
        let storage = ProllyStorage::in_memory().unwrap();
        let mut glue = Glue::new(storage);
        glue.execute("CREATE TABLE notes (value TEXT NOT NULL);")
            .await
            .unwrap();
        glue.execute("INSERT INTO notes VALUES ('base');")
            .await
            .unwrap();
        let base = glue.storage.head().unwrap().unwrap();
        glue.storage.create_branch("feature").unwrap();

        glue.execute("CREATE INDEX notes_value ON notes (value);")
            .await
            .unwrap();
        glue.storage.checkout_branch("feature").unwrap();
        glue.execute("INSERT INTO notes VALUES ('incoming');")
            .await
            .unwrap();
        let incoming = glue.storage.head().unwrap().unwrap();

        glue.storage.checkout_branch("main").unwrap();
        let result = glue.storage.merge(&base, &incoming).await.unwrap();
        let MergeResult::Applied { version, changes } = result else {
            panic!("disjoint changes should merge cleanly");
        };
        assert_eq!(version.branch(), "main");
        assert_eq!(changes.rows.len(), 1);
        assert!(changes.schemas.is_empty());

        let selected = glue
            .execute("SELECT value FROM notes WHERE value = 'incoming';")
            .await
            .unwrap();
        assert!(matches!(
            &selected[0],
            Payload::Select { rows, .. }
                if rows == &vec![vec![Value::Str("incoming".to_owned())]]
        ));

        glue.execute("INSERT INTO notes VALUES ('after');")
            .await
            .unwrap();
        let keys = glue
            .storage
            .scan_data_inner("notes")
            .unwrap()
            .into_iter()
            .map(|(key, _)| key)
            .collect::<Vec<_>>();
        assert_eq!(keys, vec![Key::I64(1), Key::I64(2), Key::I64(3)]);
    }

    #[tokio::test]
    async fn merge_returns_typed_row_conflicts_without_moving_head() {
        let storage = ProllyStorage::in_memory().unwrap();
        let mut glue = Glue::new(storage);
        glue.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT);")
            .await
            .unwrap();
        glue.execute("INSERT INTO t VALUES (1, 'base');")
            .await
            .unwrap();
        let base = glue.storage.head().unwrap().unwrap();
        glue.storage.create_branch("feature").unwrap();

        glue.execute("UPDATE t SET value = 'current' WHERE id = 1;")
            .await
            .unwrap();
        let current = glue.storage.head().unwrap().unwrap();
        glue.storage.checkout_branch("feature").unwrap();
        glue.execute("UPDATE t SET value = 'incoming' WHERE id = 1;")
            .await
            .unwrap();
        let incoming = glue.storage.head().unwrap().unwrap();

        glue.storage.checkout_branch("main").unwrap();
        let result = glue.storage.merge(&base, &incoming).await.unwrap();
        assert!(!result.is_applied());
        assert!(matches!(
            result.conflicts(),
            [MergeConflict::Row {
                table,
                key: Key::I64(1),
                base: Some(DataRow::Vec(base)),
                current: Some(DataRow::Vec(current)),
                incoming: Some(DataRow::Vec(incoming)),
            }] if table == "t"
                && base[1] == Value::Str("base".to_owned())
                && current[1] == Value::Str("current".to_owned())
                && incoming[1] == Value::Str("incoming".to_owned())
        ));
        assert_eq!(glue.storage.head().unwrap().unwrap().id(), current.id());
    }

    #[tokio::test]
    async fn merge_reports_cross_branch_unique_constraint_conflicts() {
        let storage = ProllyStorage::in_memory().unwrap();
        let mut glue = Glue::new(storage);
        glue.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL UNIQUE);")
            .await
            .unwrap();
        let base = glue.storage.head().unwrap().unwrap();
        glue.storage.create_branch("feature").unwrap();

        glue.execute("INSERT INTO users VALUES (1, 'same@example.com');")
            .await
            .unwrap();
        let current = glue.storage.head().unwrap().unwrap();
        glue.storage.checkout_branch("feature").unwrap();
        glue.execute("INSERT INTO users VALUES (2, 'same@example.com');")
            .await
            .unwrap();
        let incoming = glue.storage.head().unwrap().unwrap();

        glue.storage.checkout_branch("main").unwrap();
        let result = glue.storage.merge(&base, &incoming).await.unwrap();
        assert!(matches!(
            result.conflicts(),
            [MergeConflict::Constraint { table, reason, .. }]
                if table == "users" && reason.contains("unique column")
        ));
        assert_eq!(glue.storage.head().unwrap().unwrap().id(), current.id());
    }

    #[tokio::test]
    async fn merge_reports_cross_branch_foreign_key_conflicts() {
        let storage = ProllyStorage::in_memory().unwrap();
        let mut glue = Glue::new(storage);
        glue.execute("CREATE TABLE parents (id INTEGER PRIMARY KEY);")
            .await
            .unwrap();
        glue.execute(
            "CREATE TABLE children (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER,
                FOREIGN KEY (parent_id) REFERENCES parents (id)
            );",
        )
        .await
        .unwrap();
        glue.execute("INSERT INTO parents VALUES (1);")
            .await
            .unwrap();
        let base = glue.storage.head().unwrap().unwrap();
        glue.storage.create_branch("feature").unwrap();

        glue.execute("DELETE FROM parents WHERE id = 1;")
            .await
            .unwrap();
        let current = glue.storage.head().unwrap().unwrap();
        glue.storage.checkout_branch("feature").unwrap();
        glue.execute("INSERT INTO children VALUES (1, 1);")
            .await
            .unwrap();
        let incoming = glue.storage.head().unwrap().unwrap();

        glue.storage.checkout_branch("main").unwrap();
        let result = glue.storage.merge(&base, &incoming).await.unwrap();
        assert!(matches!(
            result.conflicts(),
            [MergeConflict::Constraint { table, reason, .. }]
                if table == "children" && reason.contains("foreign key")
        ));
        assert_eq!(glue.storage.head().unwrap().unwrap().id(), current.id());
    }

    #[tokio::test]
    async fn merge_reports_schema_and_function_conflicts() {
        let storage = ProllyStorage::in_memory().unwrap();
        let mut glue = Glue::new(storage);
        glue.execute("CREATE TABLE t (id INTEGER PRIMARY KEY);")
            .await
            .unwrap();
        glue.execute("CREATE FUNCTION transform(n INT) RETURN n;")
            .await
            .unwrap();
        let base = glue.storage.head().unwrap().unwrap();
        glue.storage.create_branch("feature").unwrap();

        glue.execute("ALTER TABLE t ADD COLUMN current_value TEXT NULL;")
            .await
            .unwrap();
        glue.execute("DROP FUNCTION transform;").await.unwrap();
        glue.execute("CREATE FUNCTION transform(n INT) RETURN n + 1;")
            .await
            .unwrap();
        glue.storage.checkout_branch("feature").unwrap();
        glue.execute("ALTER TABLE t ADD COLUMN incoming_value TEXT NULL;")
            .await
            .unwrap();
        glue.execute("DROP FUNCTION transform;").await.unwrap();
        glue.execute("CREATE FUNCTION transform(n INT) RETURN n + 2;")
            .await
            .unwrap();
        let incoming = glue.storage.head().unwrap().unwrap();

        glue.storage.checkout_branch("main").unwrap();
        let result = glue.storage.merge(&base, &incoming).await.unwrap();
        assert_eq!(result.conflicts().len(), 2);
        assert!(result.conflicts().iter().any(
            |conflict| matches!(conflict, MergeConflict::Schema { table, .. } if table == "t")
        ));
        assert!(result.conflicts().iter().any(
            |conflict| matches!(conflict, MergeConflict::Function { name, .. } if name == "TRANSFORM")
        ));
    }

    #[tokio::test]
    async fn merge_publication_rejects_a_stale_branch_head() {
        let store = std::sync::Arc::new(prolly::MemStore::new());
        let mut primary = Glue::new(ProllyStorage::new(store.clone()).unwrap());
        primary
            .execute("CREATE TABLE t (id INTEGER PRIMARY KEY);")
            .await
            .unwrap();
        let base = primary.storage.head().unwrap().unwrap();
        primary.storage.create_branch("feature").unwrap();
        primary.storage.checkout_branch("feature").unwrap();
        primary.execute("INSERT INTO t VALUES (1);").await.unwrap();
        let incoming = primary.storage.head().unwrap().unwrap();
        primary.storage.checkout_branch("main").unwrap();

        let expected_root = primary.storage.load_head_tree().unwrap();
        let expected = Version::new("main".to_owned(), expected_root.clone().unwrap());
        let mut concurrent = Glue::new(ProllyStorage::new(store).unwrap());
        concurrent
            .execute("INSERT INTO t VALUES (2);")
            .await
            .unwrap();

        assert!(matches!(
            primary
                .storage
                .publish_merge(expected_root, expected, *incoming.tree),
            Err(Error::SerializationConflict)
        ));
        assert_eq!(
            concurrent.storage.head().unwrap().unwrap().id(),
            primary.storage.head().unwrap().unwrap().id()
        );
        assert_ne!(primary.storage.head().unwrap().unwrap().id(), base.id());
    }

    #[tokio::test]
    async fn merge_fast_forwards_and_rejects_active_transactions() {
        let storage = ProllyStorage::in_memory().unwrap();
        let mut glue = Glue::new(storage);
        glue.execute("CREATE TABLE t (id INTEGER PRIMARY KEY);")
            .await
            .unwrap();
        let base = glue.storage.head().unwrap().unwrap();
        glue.storage.create_branch("feature").unwrap();
        glue.storage.checkout_branch("feature").unwrap();
        glue.execute("INSERT INTO t VALUES (1);").await.unwrap();
        let incoming = glue.storage.head().unwrap().unwrap();

        glue.storage.checkout_branch("main").unwrap();
        let result = glue.storage.merge(&base, &incoming).await.unwrap();
        assert!(result.is_applied());
        assert_eq!(glue.storage.head().unwrap().unwrap().id(), incoming.id());

        glue.execute("START TRANSACTION;").await.unwrap();
        assert!(matches!(
            glue.storage.merge(&base, &incoming).await,
            Err(Error::TransactionState("cannot merge during a transaction"))
        ));
        glue.execute("ROLLBACK;").await.unwrap();
    }

    #[tokio::test]
    async fn logical_diff_hides_physical_index_and_metadata_entries() {
        let storage = ProllyStorage::in_memory().unwrap();
        let mut glue = Glue::new(storage);
        glue.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT);")
            .await
            .unwrap();
        glue.execute("INSERT INTO t VALUES (1, 'one');")
            .await
            .unwrap();
        let before = glue.storage.head().unwrap().unwrap();

        glue.execute("CREATE INDEX value_idx ON t (value);")
            .await
            .unwrap();
        glue.execute("CREATE FUNCTION plus_one(n INT) RETURN n + 1;")
            .await
            .unwrap();
        let after = glue.storage.head().unwrap().unwrap();
        let diff = glue.storage.diff(&before, &after).unwrap();

        assert!(diff.rows.is_empty());
        assert_eq!(diff.schemas.len(), 1);
        assert!(matches!(
            &diff.schemas[0],
            SchemaChange::Modified { before, after }
                if before.indexes.is_empty()
                    && after.indexes.len() == 1
                    && after.indexes[0].name == "value_idx"
        ));
        assert_eq!(diff.functions.len(), 1);
        assert!(matches!(
            &diff.functions[0],
            FunctionChange::Added { function } if function.func_name == "plus_one"
        ));
        assert_eq!(diff.len(), 2);
    }
}
