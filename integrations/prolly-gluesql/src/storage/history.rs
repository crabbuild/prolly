use {
    super::{Diff, ProllyStorage, Version, VersionId},
    crate::{layout::branch_root_name, Error, Result},
    prolly::{Cid, ManifestStore, Mutation, NamedRootUpdate, Store, Tree},
    serde::{de::DeserializeOwned, Deserialize, Serialize},
    std::{
        collections::{BTreeMap, HashMap, HashSet, VecDeque},
        str::FromStr,
        time::{SystemTime, UNIX_EPOCH},
    },
};

const HISTORY_ROOT_NAME: &[u8] = b"\0prolly-gluesql/v1/history";
const COMMIT_ROOT_PREFIX: &[u8] = b"\0prolly-gluesql/v1/commits/";
const HISTORY_KEY_PREFIX: &[u8] = b"\0prolly-gluesql-history\x01";
const COMMIT_KEY_KIND: u8 = 1;
const REF_KEY_KIND: u8 = 2;
const HISTORY_MAGIC: &[u8; 4] = b"PGHG";
const HISTORY_FORMAT_VERSION: u16 = 1;
const MAX_GRAPH_VISITS: usize = 1_000_000;

/// Application-defined metadata attached to commits and refs.
///
/// Byte keys and values keep the graph business-neutral and allow applications
/// to layer JSON, signatures, review state, or domain-specific codecs above it.
pub type CommitMetadata = BTreeMap<Vec<u8>, Vec<u8>>;

/// A stable, content-derived commit identifier.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CommitId(String);

impl CommitId {
    /// Return the lowercase hexadecimal SHA-256 identifier.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn from_record(record: &[u8]) -> Self {
        Self(hex_encode(Cid::from_bytes(record).as_bytes()))
    }

    fn as_cid(&self) -> Result<Cid> {
        Ok(Cid(parse_hex_32(&self.0, "commit")?))
    }
}

impl std::fmt::Display for CommitId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for CommitId {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        parse_hex_32(value, "commit")?;
        Ok(Self(value.to_ascii_lowercase()))
    }
}

impl FromStr for VersionId {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        parse_hex_32(value, "version")?;
        Ok(Self(value.to_ascii_lowercase()))
    }
}

/// Identity recorded on a commit.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitActor {
    pub name: Option<String>,
    pub email: Option<String>,
    pub id: Option<String>,
    pub metadata: CommitMetadata,
}

impl CommitActor {
    /// Create an actor identified by a display name.
    pub fn named(name: impl Into<String>) -> Self {
        Self {
            name: Some(name.into()),
            ..Self::default()
        }
    }
}

/// An immutable commit in the database history graph.
#[derive(Clone, Debug)]
pub struct Commit {
    pub id: CommitId,
    pub version: Version,
    pub parents: Vec<CommitId>,
    pub author: Option<CommitActor>,
    pub committer: Option<CommitActor>,
    pub message: String,
    pub created_at_millis: u64,
    pub metadata: CommitMetadata,
    /// Longest parent path from a root commit; root commits have generation 0.
    pub generation: u64,
}

impl PartialEq for Commit {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Commit {}

/// Options for creating a commit from the selected branch head.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitOptions {
    pub message: String,
    pub author: Option<CommitActor>,
    pub committer: Option<CommitActor>,
    /// `None` uses the selected branch's current commit as the parent.
    /// `Some` supports explicit root and merge commits.
    pub parents: Option<Vec<CommitId>>,
    pub created_at_millis: Option<u64>,
    pub metadata: CommitMetadata,
    pub allow_empty: bool,
}

impl CommitOptions {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            author: None,
            committer: None,
            parents: None,
            created_at_millis: None,
            metadata: CommitMetadata::new(),
            allow_empty: false,
        }
    }

    pub fn author(mut self, author: CommitActor) -> Self {
        self.author = Some(author);
        self
    }

    pub fn committer(mut self, committer: CommitActor) -> Self {
        self.committer = Some(committer);
        self
    }

    pub fn parents(mut self, parents: impl IntoIterator<Item = CommitId>) -> Self {
        self.parents = Some(parents.into_iter().collect());
        self
    }

    pub fn created_at_millis(mut self, created_at_millis: u64) -> Self {
        self.created_at_millis = Some(created_at_millis);
        self
    }

    pub fn metadata(mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    pub fn allow_empty(mut self, allow_empty: bool) -> Self {
        self.allow_empty = allow_empty;
        self
    }
}

/// A typed target that can resolve to an immutable database version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DatabaseRef {
    Branch(String),
    Ref(String),
    Commit(CommitId),
    Version(VersionId),
}

/// A durable named commit ref such as `refs/heads/main` or `refs/tags/v1`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitRef {
    pub name: String,
    pub target: CommitId,
    pub generation: u64,
    pub updated_at_millis: u64,
    pub metadata: CommitMetadata,
}

/// Result of a compare-and-swap ref update.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RefUpdate {
    Applied { reference: Option<CommitRef> },
    Conflict { current: Option<CommitRef> },
}

impl RefUpdate {
    pub fn is_applied(&self) -> bool {
        matches!(self, Self::Applied { .. })
    }

    pub fn current(&self) -> Option<&CommitRef> {
        match self {
            Self::Applied { reference } => reference.as_ref(),
            Self::Conflict { current } => current.as_ref(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct StoredCommit {
    tree: Tree,
    parents: Vec<CommitId>,
    author: Option<CommitActor>,
    committer: Option<CommitActor>,
    message: String,
    created_at_millis: u64,
    metadata: CommitMetadata,
    generation: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct StoredRef {
    target: CommitId,
    generation: u64,
    updated_at_millis: u64,
    metadata: CommitMetadata,
}

impl<S> ProllyStorage<S>
where
    S: Store + ManifestStore,
{
    /// Record the selected branch head as a new commit.
    pub fn commit(&self, message: impl Into<String>) -> Result<Commit> {
        self.commit_with(CommitOptions::new(message))
    }

    /// Record the selected branch head with explicit authorship, parents, and metadata.
    pub fn commit_with(&self, options: CommitOptions) -> Result<Commit> {
        if self.transaction.is_some() {
            return Err(Error::TransactionState(
                "cannot commit graph history during a SQL transaction",
            ));
        }
        let version = self
            .head()?
            .unwrap_or_else(|| Version::new(self.branch.clone(), self.engine.create()));
        let (history_before, history) = self.history_tree()?;
        let branch_ref = branch_ref_name(&self.branch)?;
        let current_ref = self.ref_from_tree(&history, &branch_ref)?;
        let parents = options.parents.clone().unwrap_or_else(|| {
            current_ref
                .as_ref()
                .map(|reference| vec![reference.target.clone()])
                .unwrap_or_default()
        });
        ensure_unique_parents(&parents)?;

        let mut parent_commits = Vec::with_capacity(parents.len());
        for parent in &parents {
            parent_commits.push(
                self.commit_from_tree(&history, parent)?.ok_or_else(|| {
                    Error::History(format!("parent commit {parent} does not exist"))
                })?,
            );
        }
        if !options.allow_empty
            && parent_commits.len() == 1
            && parent_commits[0].version.tree == version.tree
        {
            return Err(Error::History("nothing to commit".to_owned()));
        }

        let stored = StoredCommit {
            tree: version.tree.as_ref().clone(),
            parents,
            author: options.author,
            committer: options.committer,
            message: options.message,
            created_at_millis: options.created_at_millis.unwrap_or_else(now_millis),
            metadata: options.metadata,
            generation: parent_commits
                .iter()
                .map(|commit| commit.generation)
                .max()
                .map_or(0, |generation| generation.saturating_add(1)),
        };
        let encoded_commit = encode_history_record(COMMIT_KEY_KIND, &stored)?;
        let id = CommitId::from_record(&encoded_commit);
        let reference = StoredRef {
            target: id.clone(),
            generation: current_ref
                .as_ref()
                .map_or(0, |reference| reference.generation.saturating_add(1)),
            updated_at_millis: stored.created_at_millis,
            metadata: current_ref
                .map(|reference| reference.metadata)
                .unwrap_or_default(),
        };
        let next = self.engine.batch(
            &history,
            vec![
                Mutation::Upsert {
                    key: commit_key(&id)?,
                    val: encoded_commit,
                },
                Mutation::Upsert {
                    key: ref_key(&branch_ref),
                    val: encode_history_record(REF_KEY_KIND, &reference)?,
                },
            ],
        )?;

        self.retain_commit_tree(&id, &version.tree)?;
        match self.engine.compare_and_swap_named_root(
            HISTORY_ROOT_NAME,
            history_before.as_ref(),
            Some(&next),
        )? {
            NamedRootUpdate::Applied => Ok(commit_from_stored(id, stored, self.branch.clone())),
            NamedRootUpdate::Conflict { .. } => Err(Error::HistoryConflict),
        }
    }

    /// Load a commit by ID.
    pub fn get_commit(&self, id: &CommitId) -> Result<Option<Commit>> {
        let (_, history) = self.history_tree()?;
        self.commit_from_tree(&history, id)
    }

    /// Resolve the current commit at `branch`, if that branch has history.
    pub fn branch_tip(&self, branch: &str) -> Result<Option<Commit>> {
        let reference = self.resolve_ref(&branch_ref_name(branch)?)?;
        reference
            .map(|reference| self.require_commit(&reference.target))
            .transpose()
    }

    /// Resolve a branch, named ref, commit ID, or version ID to a database version.
    pub fn resolve(&self, reference: &DatabaseRef) -> Result<Version> {
        match reference {
            DatabaseRef::Branch(branch) => {
                let name = branch_root_name(branch)?;
                self.engine
                    .load_named_root(&name)?
                    .map(|tree| Version::new(branch.clone(), tree))
                    .ok_or_else(|| Error::Branch(format!("branch {branch:?} does not exist")))
            }
            DatabaseRef::Ref(name) => {
                let reference = self
                    .resolve_ref(name)?
                    .ok_or_else(|| Error::History(format!("ref {name:?} does not exist")))?;
                self.require_commit(&reference.target)
                    .map(|commit| commit.version)
            }
            DatabaseRef::Commit(id) => self.require_commit(id).map(|commit| commit.version),
            DatabaseRef::Version(id) => self.resolve_version_id(id),
        }
    }

    /// Create a SQL branch from another branch, a named ref, a commit, or a version ID.
    pub fn create_branch_from(&self, name: &str, source: &DatabaseRef) -> Result<Version> {
        let version = self.resolve(source)?;
        let source_commit = self.commit_id_for_ref(source)?;
        if let Some(id) = source_commit.as_ref() {
            let target_ref = branch_ref_name(name)?;
            if self.resolve_ref(&target_ref)?.is_some() {
                return Err(Error::Branch(format!(
                    "commit ref for branch {name:?} already exists"
                )));
            }
            self.require_commit(id)?;
        }

        let target_name = branch_root_name(name)?;
        match self
            .engine
            .compare_and_swap_named_root(&target_name, None, Some(&version.tree))?
        {
            NamedRootUpdate::Applied => {}
            NamedRootUpdate::Conflict { .. } => {
                return Err(Error::Branch(format!("branch {name:?} already exists")));
            }
        }

        if let Some(id) = source_commit {
            let target_ref = branch_ref_name(name)?;
            match self.create_ref(&target_ref, &id)? {
                RefUpdate::Applied { .. } => {}
                RefUpdate::Conflict { .. } => {
                    self.rollback_new_branch(&target_name, &version.tree)?;
                    return Err(Error::HistoryConflict);
                }
            }
        }
        Ok(Version::new(name.to_owned(), version.tree.as_ref().clone()))
    }

    /// Resolve a named commit ref.
    pub fn resolve_ref(&self, name: &str) -> Result<Option<CommitRef>> {
        validate_ref_name(name)?;
        let (_, history) = self.history_tree()?;
        self.ref_from_tree(&history, name)
    }

    /// List commit refs under `prefix` in byte-lexicographic name order.
    pub fn list_refs(&self, prefix: &str) -> Result<Vec<CommitRef>> {
        if !prefix.is_empty() {
            validate_ref_prefix(prefix)?;
        }
        let (_, history) = self.history_tree()?;
        let key_prefix = ref_key(prefix);
        let end = prolly::prefix_range(&key_prefix).1;
        let mut references = Vec::new();
        for entry in self.engine.range(&history, &key_prefix, end.as_deref())? {
            let (key, value) = entry?;
            let name = String::from_utf8(
                key.strip_prefix(ref_key("").as_slice())
                    .ok_or_else(|| Error::Corrupt("history ref key escaped its prefix".to_owned()))?
                    .to_vec(),
            )
            .map_err(|_| Error::Corrupt("history ref name is not UTF-8".to_owned()))?;
            let stored: StoredRef = decode_history_record(&value, REF_KEY_KIND)?;
            references.push(commit_ref_from_stored(name, stored));
        }
        Ok(references)
    }

    /// Create a named ref only when it does not already exist.
    pub fn create_ref(&self, name: &str, target: &CommitId) -> Result<RefUpdate> {
        self.compare_and_swap_ref(name, None, Some(target), CommitMetadata::new())
    }

    /// Atomically update or delete a named ref.
    pub fn compare_and_swap_ref(
        &self,
        name: &str,
        expected: Option<&CommitId>,
        replacement: Option<&CommitId>,
        metadata: CommitMetadata,
    ) -> Result<RefUpdate> {
        validate_ref_name(name)?;
        let (history_before, history) = self.history_tree()?;
        let current = self.ref_from_tree(&history, name)?;
        if current.as_ref().map(|reference| &reference.target) != expected {
            return Ok(RefUpdate::Conflict { current });
        }
        if let Some(target) = replacement {
            self.commit_from_tree(&history, target)?
                .ok_or_else(|| Error::History(format!("target commit {target} does not exist")))?;
        }
        let timestamp = now_millis();
        let replacement_record = replacement.map(|target| StoredRef {
            target: target.clone(),
            generation: current
                .as_ref()
                .map_or(0, |reference| reference.generation.saturating_add(1)),
            updated_at_millis: timestamp,
            metadata,
        });
        let mutation = match replacement_record.as_ref() {
            Some(reference) => Mutation::Upsert {
                key: ref_key(name),
                val: encode_history_record(REF_KEY_KIND, reference)?,
            },
            None => Mutation::Delete { key: ref_key(name) },
        };
        let next = self.engine.batch(&history, vec![mutation])?;
        match self.engine.compare_and_swap_named_root(
            HISTORY_ROOT_NAME,
            history_before.as_ref(),
            Some(&next),
        )? {
            NamedRootUpdate::Applied => Ok(RefUpdate::Applied {
                reference: replacement_record
                    .map(|reference| commit_ref_from_stored(name.to_owned(), reference)),
            }),
            NamedRootUpdate::Conflict { .. } => {
                let current = self.resolve_ref(name)?;
                Ok(RefUpdate::Conflict { current })
            }
        }
    }

    /// Return direct parent IDs in their recorded order.
    pub fn parents(&self, id: &CommitId) -> Result<Vec<CommitId>> {
        Ok(self.require_commit(id)?.parents)
    }

    /// Traverse a commit and its ancestors breadth-first, preserving parent order.
    pub fn log(&self, start: &CommitId, max_count: usize) -> Result<Vec<Commit>> {
        if max_count == 0 {
            return Ok(Vec::new());
        }
        let (_, history) = self.history_tree()?;
        let mut queue = VecDeque::from([start.clone()]);
        let mut seen = HashSet::new();
        let mut output = Vec::new();
        while let Some(id) = queue.pop_front() {
            if !seen.insert(id.clone()) {
                continue;
            }
            graph_limit(seen.len())?;
            let commit = self
                .commit_from_tree(&history, &id)?
                .ok_or_else(|| Error::History(format!("commit {id} does not exist")))?;
            queue.extend(commit.parents.iter().cloned());
            output.push(commit);
            if output.len() == max_count {
                break;
            }
        }
        Ok(output)
    }

    /// Return whether `ancestor` is reachable from `descendant`.
    pub fn is_ancestor(&self, ancestor: &CommitId, descendant: &CommitId) -> Result<bool> {
        if ancestor == descendant {
            self.require_commit(ancestor)?;
            return Ok(true);
        }
        let (_, history) = self.history_tree()?;
        let mut queue = VecDeque::from([descendant.clone()]);
        let mut seen = HashSet::new();
        while let Some(id) = queue.pop_front() {
            if !seen.insert(id.clone()) {
                continue;
            }
            graph_limit(seen.len())?;
            let commit = self
                .commit_from_tree(&history, &id)?
                .ok_or_else(|| Error::History(format!("commit {id} does not exist")))?;
            for parent in commit.parents {
                if &parent == ancestor {
                    return Ok(true);
                }
                queue.push_back(parent);
            }
        }
        Ok(false)
    }

    /// Find one deterministic best common ancestor of two commits.
    pub fn merge_base(&self, left: &CommitId, right: &CommitId) -> Result<Option<Commit>> {
        let (_, history) = self.history_tree()?;
        let left_distances = self.ancestor_distances(&history, left)?;
        let right_distances = self.ancestor_distances(&history, right)?;
        let mut candidates = Vec::new();
        for (id, left_distance) in left_distances {
            if let Some(right_distance) = right_distances.get(&id) {
                let commit = self
                    .commit_from_tree(&history, &id)?
                    .ok_or_else(|| Error::History(format!("commit {id} does not exist")))?;
                candidates.push((
                    commit.generation,
                    left_distance.saturating_add(*right_distance),
                    id,
                    commit,
                ));
            }
        }
        candidates.sort_by(|left, right| {
            right
                .0
                .cmp(&left.0)
                .then_with(|| left.1.cmp(&right.1))
                .then_with(|| left.2.cmp(&right.2))
        });
        Ok(candidates.into_iter().next().map(|candidate| candidate.3))
    }

    /// Return the typed logical SQL diff between two commits.
    pub fn diff_commits(&self, base: &CommitId, target: &CommitId) -> Result<Diff> {
        let base = self.require_commit(base)?;
        let target = self.require_commit(target)?;
        self.diff(&base.version, &target.version)
    }

    fn require_commit(&self, id: &CommitId) -> Result<Commit> {
        self.get_commit(id)?
            .ok_or_else(|| Error::History(format!("commit {id} does not exist")))
    }

    fn history_tree(&self) -> Result<(Option<Tree>, Tree)> {
        let stored = self.engine.load_named_root(HISTORY_ROOT_NAME)?;
        let tree = stored.clone().unwrap_or_else(|| self.engine.create());
        Ok((stored, tree))
    }

    fn commit_from_tree(&self, history: &Tree, id: &CommitId) -> Result<Option<Commit>> {
        let Some(record) = self.engine.get(history, &commit_key(id)?)? else {
            return Ok(None);
        };
        if CommitId::from_record(&record) != *id {
            return Err(Error::Corrupt(format!(
                "commit record does not match key {id}"
            )));
        }
        let stored: StoredCommit = decode_history_record(&record, COMMIT_KEY_KIND)?;
        Ok(Some(commit_from_stored(
            id.clone(),
            stored,
            self.branch.clone(),
        )))
    }

    fn ref_from_tree(&self, history: &Tree, name: &str) -> Result<Option<CommitRef>> {
        let Some(record) = self.engine.get(history, &ref_key(name))? else {
            return Ok(None);
        };
        let stored: StoredRef = decode_history_record(&record, REF_KEY_KIND)?;
        Ok(Some(commit_ref_from_stored(name.to_owned(), stored)))
    }

    fn retain_commit_tree(&self, id: &CommitId, tree: &Tree) -> Result<()> {
        let name = commit_root_name(id);
        match self
            .engine
            .compare_and_swap_named_root(&name, None, Some(tree))?
        {
            NamedRootUpdate::Applied => Ok(()),
            NamedRootUpdate::Conflict { current } if current.as_ref() == Some(tree) => Ok(()),
            NamedRootUpdate::Conflict { .. } => Err(Error::Corrupt(format!(
                "commit retention root {id} points to another version"
            ))),
        }
    }

    fn resolve_version_id(&self, id: &VersionId) -> Result<Version> {
        let tree = Tree {
            root: Some(Cid(parse_hex_32(id.as_str(), "version")?)),
            config: self.engine.config().clone(),
        };
        self.engine.mark_reachable(std::slice::from_ref(&tree))?;
        Ok(Version::new(self.branch.clone(), tree))
    }

    fn commit_id_for_ref(&self, reference: &DatabaseRef) -> Result<Option<CommitId>> {
        match reference {
            DatabaseRef::Branch(branch) => self
                .resolve_ref(&branch_ref_name(branch)?)
                .map(|reference| reference.map(|reference| reference.target)),
            DatabaseRef::Ref(name) => self
                .resolve_ref(name)
                .map(|reference| reference.map(|reference| reference.target)),
            DatabaseRef::Commit(id) => Ok(Some(id.clone())),
            DatabaseRef::Version(_) => Ok(None),
        }
    }

    fn rollback_new_branch(&self, name: &[u8], tree: &Tree) -> Result<()> {
        match self
            .engine
            .compare_and_swap_named_root(name, Some(tree), None)?
        {
            NamedRootUpdate::Applied => Ok(()),
            NamedRootUpdate::Conflict { .. } => Err(Error::Branch(
                "branch was created but its commit ref could not be initialized".to_owned(),
            )),
        }
    }

    fn ancestor_distances(
        &self,
        history: &Tree,
        start: &CommitId,
    ) -> Result<HashMap<CommitId, usize>> {
        let mut distances = HashMap::new();
        let mut queue = VecDeque::from([(start.clone(), 0_usize)]);
        while let Some((id, distance)) = queue.pop_front() {
            if distances.contains_key(&id) {
                continue;
            }
            distances.insert(id.clone(), distance);
            graph_limit(distances.len())?;
            let commit = self
                .commit_from_tree(history, &id)?
                .ok_or_else(|| Error::History(format!("commit {id} does not exist")))?;
            queue.extend(
                commit
                    .parents
                    .into_iter()
                    .map(|parent| (parent, distance.saturating_add(1))),
            );
        }
        Ok(distances)
    }
}

fn commit_from_stored(id: CommitId, stored: StoredCommit, branch: String) -> Commit {
    Commit {
        id,
        version: Version::new(branch, stored.tree),
        parents: stored.parents,
        author: stored.author,
        committer: stored.committer,
        message: stored.message,
        created_at_millis: stored.created_at_millis,
        metadata: stored.metadata,
        generation: stored.generation,
    }
}

fn commit_ref_from_stored(name: String, stored: StoredRef) -> CommitRef {
    CommitRef {
        name,
        target: stored.target,
        generation: stored.generation,
        updated_at_millis: stored.updated_at_millis,
        metadata: stored.metadata,
    }
}

fn commit_key(id: &CommitId) -> Result<Vec<u8>> {
    let mut key = history_key(COMMIT_KEY_KIND);
    key.extend_from_slice(id.as_cid()?.as_bytes());
    Ok(key)
}

fn ref_key(name: &str) -> Vec<u8> {
    let mut key = history_key(REF_KEY_KIND);
    key.extend_from_slice(name.as_bytes());
    key
}

fn history_key(kind: u8) -> Vec<u8> {
    let mut key = HISTORY_KEY_PREFIX.to_vec();
    key.push(kind);
    key
}

fn commit_root_name(id: &CommitId) -> Vec<u8> {
    let mut name = COMMIT_ROOT_PREFIX.to_vec();
    name.extend_from_slice(id.as_str().as_bytes());
    name
}

fn branch_ref_name(branch: &str) -> Result<String> {
    branch_root_name(branch)?;
    Ok(format!("refs/heads/{branch}"))
}

fn validate_ref_name(name: &str) -> Result<()> {
    let Some(value) = name.strip_prefix("refs/") else {
        return Err(Error::History(format!(
            "ref name {name:?} must start with \"refs/\""
        )));
    };
    branch_root_name(value)
        .map(|_| ())
        .map_err(|_| Error::History(format!("invalid ref name {name:?}")))
}

fn validate_ref_prefix(prefix: &str) -> Result<()> {
    if prefix == "refs/" {
        return Ok(());
    }
    validate_ref_name(prefix.trim_end_matches('/'))
}

fn encode_history_record<T: Serialize>(kind: u8, value: &T) -> Result<Vec<u8>> {
    let payload = bincode::serialize(value)?;
    let mut record = Vec::with_capacity(7 + payload.len());
    record.extend_from_slice(HISTORY_MAGIC);
    record.extend_from_slice(&HISTORY_FORMAT_VERSION.to_be_bytes());
    record.push(kind);
    record.extend_from_slice(&payload);
    Ok(record)
}

fn decode_history_record<T: DeserializeOwned>(bytes: &[u8], expected_kind: u8) -> Result<T> {
    if bytes.len() < 7 || &bytes[..4] != HISTORY_MAGIC {
        return Err(Error::Corrupt(
            "missing commit graph record header".to_owned(),
        ));
    }
    let version = u16::from_be_bytes([bytes[4], bytes[5]]);
    if version != HISTORY_FORMAT_VERSION {
        return Err(Error::UnsupportedFormat(format!(
            "commit graph record version {version}; expected {HISTORY_FORMAT_VERSION}"
        )));
    }
    if bytes[6] != expected_kind {
        return Err(Error::Corrupt(
            "commit graph record kind mismatch".to_owned(),
        ));
    }
    Ok(bincode::deserialize(&bytes[7..])?)
}

fn ensure_unique_parents(parents: &[CommitId]) -> Result<()> {
    let unique = parents.iter().collect::<HashSet<_>>();
    if unique.len() == parents.len() {
        Ok(())
    } else {
        Err(Error::History(
            "a commit cannot list the same parent more than once".to_owned(),
        ))
    }
}

fn graph_limit(visited: usize) -> Result<()> {
    if visited <= MAX_GRAPH_VISITS {
        Ok(())
    } else {
        Err(Error::History(format!(
            "commit graph traversal exceeded {MAX_GRAPH_VISITS} commits"
        )))
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or_default()
}

fn parse_hex_32(value: &str, kind: &str) -> Result<[u8; 32]> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(Error::History(format!(
            "invalid {kind} id {value:?}; expected 64 hexadecimal characters"
        )));
    }
    let mut bytes = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        bytes[index] = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
    }
    Ok(bytes)
}

fn hex_nibble(byte: u8) -> Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(Error::History("invalid hexadecimal identifier".to_owned())),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{gluesql_core::prelude::Value, Glue, Payload, ProllyStorage, RowChange},
    };

    #[tokio::test]
    async fn records_linear_history_and_exposes_typed_graph_queries() {
        let mut glue = Glue::new(ProllyStorage::in_memory().unwrap());
        glue.execute("CREATE TABLE counters (id INTEGER PRIMARY KEY, value INTEGER NOT NULL);")
            .await
            .unwrap();
        glue.execute("INSERT INTO counters VALUES (1, 0);")
            .await
            .unwrap();

        let first = glue
            .storage
            .commit_with(
                CommitOptions::new("initialize counters")
                    .author(CommitActor::named("Ada"))
                    .created_at_millis(1_000)
                    .metadata(b"source", b"example"),
            )
            .unwrap();
        assert!(glue
            .storage
            .diff(&first.version, &glue.storage.head().unwrap().unwrap())
            .unwrap()
            .is_empty());
        assert!(first.parents.is_empty());
        assert_eq!(first.generation, 0);
        assert_eq!(first.author.as_ref().unwrap().name.as_deref(), Some("Ada"));

        let empty = glue.storage.commit("nothing changed").unwrap_err();
        assert!(empty.to_string().contains("nothing to commit"));

        glue.execute("UPDATE counters SET value = 1 WHERE id = 1;")
            .await
            .unwrap();
        let second = glue
            .storage
            .commit_with(CommitOptions::new("increment counter").created_at_millis(2_000))
            .unwrap();
        assert_eq!(second.parents, vec![first.id.clone()]);
        assert_eq!(second.generation, 1);
        assert!(glue.storage.is_ancestor(&first.id, &second.id).unwrap());

        let log = glue.storage.log(&second.id, 10).unwrap();
        assert_eq!(
            log.iter().map(|commit| &commit.id).collect::<Vec<_>>(),
            vec![&second.id, &first.id]
        );
        let changes = glue.storage.diff_commits(&first.id, &second.id).unwrap();
        assert!(matches!(
            changes.rows.as_slice(),
            [RowChange::Modified { table, .. }] if table == "counters"
        ));

        let parsed: CommitId = second.id.to_string().parse().unwrap();
        assert_eq!(parsed, second.id);
        let parsed_version: VersionId = second.version.id().unwrap().to_string().parse().unwrap();
        assert_eq!(&parsed_version, second.version.id().unwrap());
        assert_eq!(
            glue.storage.branch_tip("main").unwrap().unwrap().id,
            second.id
        );
        assert_eq!(glue.storage.list_refs("refs/heads/").unwrap().len(), 1);
    }

    #[tokio::test]
    async fn branches_resolve_refs_commits_versions_and_preserve_merge_parents() {
        let mut glue = Glue::new(ProllyStorage::in_memory().unwrap());
        glue.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, message TEXT NOT NULL);")
            .await
            .unwrap();
        glue.execute("INSERT INTO events VALUES (1, 'base');")
            .await
            .unwrap();
        let base = glue
            .storage
            .commit_with(CommitOptions::new("base").created_at_millis(1_000))
            .unwrap();

        glue.storage
            .create_branch_from("feature", &DatabaseRef::Commit(base.id.clone()))
            .unwrap();
        glue.execute("INSERT INTO events VALUES (2, 'main');")
            .await
            .unwrap();
        let main = glue
            .storage
            .commit_with(CommitOptions::new("main work").created_at_millis(2_000))
            .unwrap();

        glue.storage.checkout_branch("feature").unwrap();
        glue.execute("INSERT INTO events VALUES (3, 'feature');")
            .await
            .unwrap();
        let feature = glue
            .storage
            .commit_with(CommitOptions::new("feature work").created_at_millis(3_000))
            .unwrap();
        assert_eq!(feature.parents, vec![base.id.clone()]);
        assert_eq!(
            glue.storage
                .merge_base(&main.id, &feature.id)
                .unwrap()
                .unwrap()
                .id,
            base.id
        );

        assert!(glue
            .storage
            .create_ref("refs/tags/base", &base.id)
            .unwrap()
            .is_applied());
        let conflict = glue
            .storage
            .compare_and_swap_ref(
                "refs/tags/base",
                Some(&main.id),
                Some(&feature.id),
                CommitMetadata::new(),
            )
            .unwrap();
        assert!(matches!(conflict, RefUpdate::Conflict { .. }));
        glue.storage
            .create_branch_from("tagged", &DatabaseRef::Ref("refs/tags/base".to_owned()))
            .unwrap();
        glue.storage
            .create_branch_from(
                "versioned",
                &DatabaseRef::Version(main.version.id().unwrap().clone()),
            )
            .unwrap();

        glue.storage.checkout_branch("main").unwrap();
        assert!(glue
            .storage
            .merge(&base.version, &feature.version)
            .await
            .unwrap()
            .is_applied());
        let merge = glue
            .storage
            .commit_with(
                CommitOptions::new("merge feature")
                    .parents([main.id.clone(), feature.id.clone()])
                    .created_at_millis(4_000),
            )
            .unwrap();
        assert_eq!(merge.parents, vec![main.id.clone(), feature.id.clone()]);
        assert!(glue.storage.is_ancestor(&main.id, &merge.id).unwrap());
        assert!(glue.storage.is_ancestor(&feature.id, &merge.id).unwrap());

        glue.storage
            .create_branch_from("from-main", &DatabaseRef::Branch("main".to_owned()))
            .unwrap();
        assert_eq!(
            glue.storage.branch_tip("from-main").unwrap().unwrap().id,
            merge.id
        );

        glue.storage.checkout_branch("tagged").unwrap();
        assert_eq!(selected_row_count(&mut glue).await, 1);
        glue.storage.checkout_branch("versioned").unwrap();
        assert_eq!(selected_row_count(&mut glue).await, 2);
        glue.storage.checkout_branch("from-main").unwrap();
        assert_eq!(selected_row_count(&mut glue).await, 3);
    }

    #[tokio::test]
    async fn commit_identity_is_independent_of_the_branch_name() {
        let mut glue = Glue::new(ProllyStorage::in_memory().unwrap());
        glue.execute("CREATE TABLE values (id INTEGER PRIMARY KEY);")
            .await
            .unwrap();
        let base = glue
            .storage
            .commit_with(CommitOptions::new("base").created_at_millis(1_000))
            .unwrap();
        glue.storage
            .create_branch_from("feature", &DatabaseRef::Commit(base.id.clone()))
            .unwrap();

        let options = || {
            CommitOptions::new("shared checkpoint")
                .created_at_millis(2_000)
                .allow_empty(true)
        };
        let main = glue.storage.commit_with(options()).unwrap();
        glue.storage.checkout_branch("feature").unwrap();
        let feature = glue.storage.commit_with(options()).unwrap();
        assert_eq!(main.id, feature.id);
    }

    async fn selected_row_count(glue: &mut Glue<ProllyStorage<prolly::MemStore>>) -> usize {
        let payloads = glue
            .execute("SELECT id FROM events ORDER BY id;")
            .await
            .unwrap();
        let Payload::Select { rows, .. } = &payloads[0] else {
            panic!("expected SELECT payload");
        };
        assert!(rows
            .iter()
            .all(|row| matches!(row.as_slice(), [Value::I64(_)])));
        rows.len()
    }
}
