use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::{AttributeValue, Error, Item, Result};

const MAX_EXPRESSION_BYTES: usize = 4 * 1024;
const MAX_EXPRESSION_NESTING: usize = 64;
const MAX_CONDITION_AST_DEPTH: usize = 512;
const MAX_PLACEHOLDER_BYTES: usize = 255;
const MAX_BINDING_BYTES: usize = 2 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Condition {
    AttributeExists(AttributePath),
    AttributeNotExists(AttributePath),
    Equals {
        name: AttributePath,
        value: AttributeValue,
    },
    Comparison {
        name: AttributePath,
        operator: ComparisonOperator,
        value: AttributeValue,
    },
    Between {
        name: AttributePath,
        lower: AttributeValue,
        upper: AttributeValue,
    },
    In {
        name: AttributePath,
        values: Vec<AttributeValue>,
    },
    BeginsWith {
        name: AttributePath,
        value: AttributeValue,
    },
    Contains {
        name: AttributePath,
        value: AttributeValue,
    },
    AttributeType {
        name: AttributePath,
        kind: String,
    },
    SizeComparison {
        name: AttributePath,
        operator: ComparisonOperator,
        value: AttributeValue,
    },
    And(Box<Condition>, Box<Condition>),
    Or(Box<Condition>, Box<Condition>),
    Not(Box<Condition>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ComparisonOperator {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
}

impl Condition {
    pub fn evaluate(&self, item: Option<&Item>) -> Result<bool> {
        self.validate_depth(0)?;
        self.evaluate_at_depth(item, 0)
    }

    fn validate_depth(&self, depth: usize) -> Result<()> {
        if depth > MAX_CONDITION_AST_DEPTH {
            return Err(Error::Validation(format!(
                "condition AST depth exceeds {MAX_CONDITION_AST_DEPTH} levels"
            )));
        }
        match self {
            Self::And(left, right) | Self::Or(left, right) => {
                left.validate_depth(depth + 1)?;
                right.validate_depth(depth + 1)
            }
            Self::Not(condition) => condition.validate_depth(depth + 1),
            _ => Ok(()),
        }
    }

    fn evaluate_at_depth(&self, item: Option<&Item>, depth: usize) -> Result<bool> {
        if depth > MAX_CONDITION_AST_DEPTH {
            return Err(Error::Validation(format!(
                "condition AST depth exceeds {MAX_CONDITION_AST_DEPTH} levels"
            )));
        }
        Ok(match self {
            Self::AttributeExists(name) => lookup(item, name)?.is_some(),
            Self::AttributeNotExists(name) => lookup(item, name)?.is_none(),
            Self::Equals { name, value } => {
                lookup(item, name)? == Some(&crate::canonicalize_attribute_value(value)?)
            }
            Self::Comparison {
                name,
                operator,
                value,
            } => match lookup(item, name)? {
                Some(left) => compare_values(left, value, *operator)?,
                None => false,
            },
            Self::Between { name, lower, upper } => match lookup(item, name)? {
                Some(value) => {
                    compare_values(value, lower, ComparisonOperator::GreaterThanOrEqual)?
                        && compare_values(value, upper, ComparisonOperator::LessThanOrEqual)?
                }
                None => false,
            },
            Self::In { name, values } => lookup(item, name)?
                .is_some_and(|value| values.iter().any(|candidate| value == candidate)),
            Self::BeginsWith { name, value } => {
                lookup(item, name)?.is_some_and(|candidate| begins_with(candidate, value))
            }
            Self::Contains { name, value } => {
                lookup(item, name)?.is_some_and(|candidate| contains(candidate, value))
            }
            Self::AttributeType { name, kind } => {
                lookup(item, name)?.is_some_and(|value| attribute_type(value) == kind)
            }
            Self::SizeComparison {
                name,
                operator,
                value,
            } => match lookup(item, name)?.and_then(attribute_size) {
                Some(size) => {
                    let size = crate::DynamoNumber::parse(&size.to_string())?;
                    compare_values(&AttributeValue::N(size), value, *operator)?
                }
                None => false,
            },
            Self::And(left, right) => {
                left.evaluate_at_depth(item, depth + 1)?
                    && right.evaluate_at_depth(item, depth + 1)?
            }
            Self::Or(left, right) => {
                left.evaluate_at_depth(item, depth + 1)?
                    || right.evaluate_at_depth(item, depth + 1)?
            }
            Self::Not(condition) => !condition.evaluate_at_depth(item, depth + 1)?,
        })
    }
}

fn lookup<'a>(item: Option<&'a Item>, path: &AttributePath) -> Result<Option<&'a AttributeValue>> {
    path.validate()?;
    match item {
        Some(item) => get_path(item, path),
        None => Ok(None),
    }
}

fn compare_values(
    left: &AttributeValue,
    right: &AttributeValue,
    operator: ComparisonOperator,
) -> Result<bool> {
    let right = crate::canonicalize_attribute_value(right)?;
    if matches!(
        operator,
        ComparisonOperator::Equal | ComparisonOperator::NotEqual
    ) {
        let equal = left == &right;
        return Ok(if operator == ComparisonOperator::Equal {
            equal
        } else {
            !equal
        });
    }
    let ordering = match (left, &right) {
        (AttributeValue::N(left), AttributeValue::N(right)) => left.numeric_cmp(right)?,
        (AttributeValue::S(left), AttributeValue::S(right)) => {
            left.as_bytes().cmp(right.as_bytes())
        }
        (AttributeValue::B(left), AttributeValue::B(right)) => left.cmp(right),
        _ => return Ok(false),
    };
    Ok(match operator {
        ComparisonOperator::LessThan => ordering == Ordering::Less,
        ComparisonOperator::LessThanOrEqual => ordering != Ordering::Greater,
        ComparisonOperator::GreaterThan => ordering == Ordering::Greater,
        ComparisonOperator::GreaterThanOrEqual => ordering != Ordering::Less,
        ComparisonOperator::Equal | ComparisonOperator::NotEqual => unreachable!(),
    })
}

fn begins_with(candidate: &AttributeValue, prefix: &AttributeValue) -> bool {
    match (candidate, prefix) {
        (AttributeValue::S(candidate), AttributeValue::S(prefix)) => candidate.starts_with(prefix),
        (AttributeValue::B(candidate), AttributeValue::B(prefix)) => candidate.starts_with(prefix),
        _ => false,
    }
}

fn contains(candidate: &AttributeValue, operand: &AttributeValue) -> bool {
    match (candidate, operand) {
        (AttributeValue::S(candidate), AttributeValue::S(operand)) => candidate.contains(operand),
        (AttributeValue::B(candidate), AttributeValue::B(operand)) => {
            operand.is_empty()
                || candidate
                    .windows(operand.len())
                    .any(|window| window == operand)
        }
        (AttributeValue::Ss(values), AttributeValue::S(value)) => values.contains(value),
        (AttributeValue::Ns(values), AttributeValue::N(value)) => values.contains(value),
        (AttributeValue::Bs(values), AttributeValue::B(value)) => values.contains(value),
        (AttributeValue::L(values), operand) => values.contains(operand),
        _ => false,
    }
}

fn attribute_type(value: &AttributeValue) -> &'static str {
    match value {
        AttributeValue::B(_) => "B",
        AttributeValue::Bool(_) => "BOOL",
        AttributeValue::Bs(_) => "BS",
        AttributeValue::L(_) => "L",
        AttributeValue::M(_) => "M",
        AttributeValue::N(_) => "N",
        AttributeValue::Ns(_) => "NS",
        AttributeValue::Null(_) => "NULL",
        AttributeValue::S(_) => "S",
        AttributeValue::Ss(_) => "SS",
    }
}

fn attribute_size(value: &AttributeValue) -> Option<usize> {
    match value {
        AttributeValue::B(value) => Some(value.len()),
        AttributeValue::Bs(values) => Some(values.len()),
        AttributeValue::L(values) => Some(values.len()),
        AttributeValue::M(values) => Some(values.len()),
        AttributeValue::Ns(values) => Some(values.len()),
        AttributeValue::S(value) => Some(value.len()),
        AttributeValue::Ss(values) => Some(values.len()),
        AttributeValue::Bool(_) | AttributeValue::N(_) | AttributeValue::Null(_) => None,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Token {
    Identifier(String),
    Name(String),
    Value(String),
    LeftParen,
    RightParen,
    Comma,
    Equal,
    Plus,
    Minus,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
    NotEqual,
    Dot,
    LeftBracket,
    RightBracket,
    Index(usize),
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum PathElement {
    Name(String),
    Index(usize),
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct AttributePath(Vec<PathElement>);

impl<'de> Deserialize<'de> for AttributePath {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let path = Self(Vec::<PathElement>::deserialize(deserializer)?);
        path.validate().map_err(serde::de::Error::custom)?;
        Ok(path)
    }
}

impl AttributePath {
    pub fn top_level(name: impl Into<String>) -> Self {
        Self(vec![PathElement::Name(name.into())])
    }

    pub fn elements(&self) -> &[PathElement] {
        &self.0
    }

    pub fn root_name(&self) -> &str {
        match &self.0[0] {
            PathElement::Name(name) => name,
            PathElement::Index(_) => unreachable!("validated paths begin with a name"),
        }
    }

    pub fn is_top_level(&self) -> bool {
        self.0.len() == 1
    }

    fn overlaps(&self, other: &Self) -> bool {
        self.0.starts_with(&other.0) || other.0.starts_with(&self.0)
    }

    fn validate(&self) -> Result<()> {
        if self.0.is_empty() || self.0.len() > 32 || !matches!(self.0[0], PathElement::Name(_)) {
            return Err(Error::Validation(
                "invalid document path depth or root".into(),
            ));
        }
        for element in &self.0 {
            if let PathElement::Name(name) = element {
                if name.is_empty() || name.len() > 64 * 1024 {
                    return Err(Error::Validation(
                        "path attribute name length must be 1..=65536 UTF-8 bytes".into(),
                    ));
                }
            }
        }
        Ok(())
    }
}

impl From<String> for AttributePath {
    fn from(name: String) -> Self {
        Self::top_level(name)
    }
}

impl From<&str> for AttributePath {
    fn from(name: &str) -> Self {
        Self::top_level(name)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SortKeyCondition {
    Equal(AttributeValue),
    LessThan(AttributeValue),
    LessThanOrEqual(AttributeValue),
    GreaterThan(AttributeValue),
    GreaterThanOrEqual(AttributeValue),
    Between(AttributeValue, AttributeValue),
    BeginsWith(AttributeValue),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyCondition {
    pub partition_name: String,
    pub partition_value: AttributeValue,
    pub sort: Option<(String, SortKeyCondition)>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SetOperand {
    Operand(UpdateOperand),
    Arithmetic {
        left: UpdateOperand,
        operator: ArithmeticOperator,
        right: UpdateOperand,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum UpdateOperand {
    Value(AttributeValue),
    Path(AttributePath),
    IfNotExists {
        source: AttributePath,
        default: AttributeValue,
    },
    ListAppend {
        left: Box<UpdateOperand>,
        right: Box<UpdateOperand>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArithmeticOperator {
    Add,
    Subtract,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum UpdateAction {
    Set {
        target: AttributePath,
        operand: SetOperand,
    },
    Remove {
        target: AttributePath,
    },
    Add {
        target: AttributePath,
        value: AttributeValue,
    },
    Delete {
        target: AttributePath,
        value: AttributeValue,
    },
}

impl UpdateAction {
    pub fn target(&self) -> &AttributePath {
        match self {
            Self::Set { target, .. }
            | Self::Remove { target }
            | Self::Add { target, .. }
            | Self::Delete { target, .. } => target,
        }
    }
}

/// A validated top-level update expression. All operands are evaluated from
/// one immutable pre-update item before any actions are applied.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct UpdatePlan {
    actions: Vec<UpdateAction>,
}

impl<'de> Deserialize<'de> for UpdatePlan {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Wire {
            actions: Vec<UpdateAction>,
        }

        let plan = Self {
            actions: Wire::deserialize(deserializer)?.actions,
        };
        plan.validate_structure()
            .map_err(serde::de::Error::custom)?;
        Ok(plan)
    }
}

impl UpdatePlan {
    pub fn actions(&self) -> &[UpdateAction] {
        &self.actions
    }

    pub fn apply<'a, I>(&self, old: &Item, key_names: I) -> Result<Item>
    where
        I: IntoIterator<Item = &'a str>,
    {
        self.validate_structure()?;
        let key_names = key_names.into_iter().collect::<BTreeSet<_>>();
        let mut targets: Vec<&AttributePath> = Vec::new();
        for action in &self.actions {
            let target = action.target();
            target.validate()?;
            if targets.iter().any(|existing| existing.overlaps(target)) {
                return Err(Error::Validation(format!(
                    "multiple update actions target attribute {target:?}"
                )));
            }
            targets.push(target);
            if key_names.contains(target.root_name()) {
                return Err(Error::Validation(format!(
                    "update expression cannot modify primary key attribute {target:?}"
                )));
            }
            if matches!(
                action,
                UpdateAction::Add { .. } | UpdateAction::Delete { .. }
            ) && !target.is_top_level()
            {
                return Err(Error::Validation(
                    "ADD and DELETE support only top-level attributes".into(),
                ));
            }
        }

        // DynamoDB evaluates operands from the old image. It orders multiple
        // SET list positions ascending and removes list positions descending,
        // preventing index shifts from changing which old elements are acted on.
        let mut sets = self
            .actions
            .iter()
            .filter_map(|action| match action {
                UpdateAction::Set { target, operand } => {
                    Some(evaluate_set_operand(operand, old).map(|value| (target, value)))
                }
                _ => None,
            })
            .collect::<Result<Vec<_>>>()?;
        sets.sort_by(|left, right| left.0.cmp(right.0));
        let mut removes = self
            .actions
            .iter()
            .filter_map(|action| match action {
                UpdateAction::Remove { target } => Some(target),
                _ => None,
            })
            .collect::<Vec<_>>();
        removes.sort_by(|left, right| compare_remove_paths(left, right));

        let mut result = old.clone();
        for (target, value) in sets {
            set_path(&mut result, target, value)?;
        }
        for target in removes {
            remove_path(&mut result, target)?;
        }
        for action in &self.actions {
            match action {
                UpdateAction::Add { target, value } => {
                    apply_add(&mut result, old, target, value)?;
                }
                UpdateAction::Delete { target, value } => {
                    apply_delete(&mut result, old, target, value)?;
                }
                UpdateAction::Set { .. } | UpdateAction::Remove { .. } => {}
            }
        }
        Ok(result)
    }

    fn validate_structure(&self) -> Result<()> {
        if self.actions.is_empty() {
            return Err(Error::Validation(
                "update plan must contain at least one action".into(),
            ));
        }
        let mut targets: Vec<&AttributePath> = Vec::new();
        for action in &self.actions {
            let target = action.target();
            target.validate()?;
            if targets.iter().any(|existing| existing.overlaps(target)) {
                return Err(Error::Validation(format!(
                    "multiple update actions target attribute {target:?}"
                )));
            }
            if matches!(
                action,
                UpdateAction::Add { .. } | UpdateAction::Delete { .. }
            ) && !target.is_top_level()
            {
                return Err(Error::Validation(
                    "ADD and DELETE support only top-level attributes".into(),
                ));
            }
            targets.push(target);
        }
        Ok(())
    }

    /// Select the action target paths from an old/new image for UPDATED_OLD or
    /// UPDATED_NEW output construction.
    pub fn project_targets(&self, item: &Item) -> Item {
        let paths = self
            .actions
            .iter()
            .map(UpdateAction::target)
            .collect::<Vec<_>>();
        project_attribute_paths(item, &paths)
    }
}

fn compare_remove_paths(left: &AttributePath, right: &AttributePath) -> Ordering {
    let left_parent = &left.0[..left.0.len().saturating_sub(1)];
    let right_parent = &right.0[..right.0.len().saturating_sub(1)];
    match (left.0.last(), right.0.last()) {
        (Some(PathElement::Index(left)), Some(PathElement::Index(right)))
            if left_parent == right_parent =>
        {
            right.cmp(left)
        }
        _ => left.cmp(right),
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParsedUpdate {
    pub plan: UpdatePlan,
    pub condition: Option<Condition>,
}

/// Validated top-level projection expression.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Projection {
    attributes: Vec<AttributePath>,
}

impl<'de> Deserialize<'de> for Projection {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Wire {
            attributes: Vec<AttributePath>,
        }

        let projection = Self {
            attributes: Wire::deserialize(deserializer)?.attributes,
        };
        projection
            .validate_structure()
            .map_err(serde::de::Error::custom)?;
        Ok(projection)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParsedReadExpressions {
    pub key_condition: Option<KeyCondition>,
    pub filter: Option<Condition>,
    pub projection: Option<Projection>,
}

impl Projection {
    pub fn attributes(&self) -> &[AttributePath] {
        &self.attributes
    }

    pub fn apply(&self, item: &Item) -> Item {
        let paths = self.attributes.iter().collect::<Vec<_>>();
        project_attribute_paths(item, &paths)
    }

    fn validate_structure(&self) -> Result<()> {
        if self.attributes.is_empty() {
            return Err(Error::Validation(
                "projection must contain at least one attribute path".into(),
            ));
        }
        let mut paths: Vec<&AttributePath> = Vec::new();
        for path in &self.attributes {
            path.validate()?;
            if paths.iter().any(|existing| existing.overlaps(path)) {
                return Err(Error::Validation(
                    "projection contains duplicate or overlapping paths".into(),
                ));
            }
            paths.push(path);
        }
        Ok(())
    }
}

fn project_attribute_paths(item: &Item, paths: &[&AttributePath]) -> Item {
    let mut roots: BTreeMap<&str, Vec<&[PathElement]>> = BTreeMap::new();
    for path in paths {
        roots
            .entry(path.root_name())
            .or_default()
            .push(&path.elements()[1..]);
    }
    roots
        .into_iter()
        .filter_map(|(name, suffixes)| {
            let value = item.get(name)?;
            project_value(value, &suffixes).map(|value| (name.to_owned(), value))
        })
        .collect()
}

fn project_value(value: &AttributeValue, suffixes: &[&[PathElement]]) -> Option<AttributeValue> {
    if suffixes.iter().any(|path| path.is_empty()) {
        return Some(value.clone());
    }
    match value {
        AttributeValue::M(map) => {
            let mut groups: BTreeMap<&str, Vec<&[PathElement]>> = BTreeMap::new();
            for suffix in suffixes {
                if let Some(PathElement::Name(name)) = suffix.first() {
                    groups.entry(name).or_default().push(&suffix[1..]);
                }
            }
            let projected = groups
                .into_iter()
                .filter_map(|(name, suffixes)| {
                    let value = map.get(name)?;
                    project_value(value, &suffixes).map(|value| (name.to_owned(), value))
                })
                .collect::<BTreeMap<_, _>>();
            (!projected.is_empty()).then_some(AttributeValue::M(projected))
        }
        AttributeValue::L(list) => {
            let mut groups: BTreeMap<usize, Vec<&[PathElement]>> = BTreeMap::new();
            for suffix in suffixes {
                if let Some(PathElement::Index(index)) = suffix.first() {
                    groups.entry(*index).or_default().push(&suffix[1..]);
                }
            }
            let projected = groups
                .into_iter()
                .filter_map(|(index, suffixes)| project_value(list.get(index)?, &suffixes))
                .collect::<Vec<_>>();
            (!projected.is_empty()).then_some(AttributeValue::L(projected))
        }
        _ => None,
    }
}

/// Parse an audited top-level projection. Aliases are mandatory so the core
/// never embeds an incomplete DynamoDB reserved-word list.
pub fn parse_projection(expression: &str, names: &BTreeMap<String, String>) -> Result<Projection> {
    validate_bindings(expression, names, &BTreeMap::new())?;
    let tokens = lex(expression)?;
    let (projection, used_names) = parse_projection_tokens(&tokens, names)?;
    require_exact_bindings(names, &BTreeMap::new(), &used_names, &BTreeSet::new())?;
    Ok(projection)
}

fn parse_projection_tokens(
    tokens: &[Token],
    names: &BTreeMap<String, String>,
) -> Result<(Projection, BTreeSet<String>)> {
    let mut attributes: Vec<AttributePath> = Vec::new();
    let mut used_names = BTreeSet::new();
    let mut offset = 0;
    while offset < tokens.len() {
        let path = parse_alias_path(tokens, &mut offset, names, &mut used_names)?;
        if attributes.iter().any(|existing| existing.overlaps(&path)) {
            return Err(Error::Validation(
                "projection contains duplicate or overlapping paths".into(),
            ));
        }
        attributes.push(path);
        if offset == tokens.len() {
            break;
        }
        if !matches!(tokens.get(offset), Some(Token::Comma)) {
            return Err(projection_syntax_error());
        }
        offset += 1;
    }
    if attributes.is_empty() || matches!(tokens.last(), Some(Token::Comma)) {
        return Err(projection_syntax_error());
    }
    Ok((Projection { attributes }, used_names))
}

fn parse_alias_path(
    tokens: &[Token],
    offset: &mut usize,
    names: &BTreeMap<String, String>,
    used_names: &mut BTreeSet<String>,
) -> Result<AttributePath> {
    let Some(Token::Name(placeholder)) = tokens.get(*offset) else {
        return Err(projection_syntax_error());
    };
    *offset += 1;
    used_names.insert(placeholder.clone());
    let mut elements = vec![PathElement::Name(resolve_name(placeholder, names)?)];
    loop {
        match tokens.get(*offset) {
            Some(Token::Dot) => {
                *offset += 1;
                let Some(Token::Name(placeholder)) = tokens.get(*offset) else {
                    return Err(projection_syntax_error());
                };
                *offset += 1;
                used_names.insert(placeholder.clone());
                elements.push(PathElement::Name(resolve_name(placeholder, names)?));
            }
            Some(Token::LeftBracket) => {
                *offset += 1;
                let Some(Token::Index(index)) = tokens.get(*offset) else {
                    return Err(projection_syntax_error());
                };
                let index = *index;
                *offset += 1;
                if !matches!(tokens.get(*offset), Some(Token::RightBracket)) {
                    return Err(projection_syntax_error());
                }
                *offset += 1;
                elements.push(PathElement::Index(index));
            }
            _ => break,
        }
        if elements.len() > 32 {
            return Err(Error::Validation(
                "document path exceeds 32 elements".into(),
            ));
        }
    }
    Ok(AttributePath(elements))
}

fn projection_syntax_error() -> Error {
    Error::Unsupported("projection supports comma-separated aliased document paths".into())
}

/// Parse the audited top-level condition subset into a typed core AST.
///
/// Attribute aliases are mandatory. This rejects unaliased names instead of
/// embedding an incomplete copy of DynamoDB's reserved-word list.
pub fn parse_condition(
    expression: &str,
    names: &BTreeMap<String, String>,
    values: &BTreeMap<String, AttributeValue>,
) -> Result<Condition> {
    validate_bindings(expression, names, values)?;
    let tokens = lex(expression)?;
    let (condition, used_names, used_values) = parse_condition_tokens(&tokens, names, values)?;

    require_exact_bindings(names, values, &used_names, &used_values)?;
    Ok(condition)
}

/// Parse the base-table partition equality form used by the initial Query
/// planner. The returned item is intentionally schema-validated later against
/// the table descriptor by `encode_partition_prefix`.
pub fn parse_key_equality(
    expression: &str,
    names: &BTreeMap<String, String>,
    values: &BTreeMap<String, AttributeValue>,
) -> Result<Item> {
    match parse_condition(expression, names, values)? {
        Condition::Equals { name, value } if name.is_top_level() => {
            Ok(Item::from([(name.root_name().to_owned(), value)]))
        }
        _ => Err(Error::Unsupported(
            "Query key condition supports only #partition_key = :value".into(),
        )),
    }
}

/// Parse the DynamoDB base-table key-condition subset into a typed range plan.
/// Attribute aliases are mandatory and all declared bindings must be used.
pub fn parse_key_condition(
    expression: &str,
    names: &BTreeMap<String, String>,
    values: &BTreeMap<String, AttributeValue>,
) -> Result<KeyCondition> {
    validate_bindings(expression, names, values)?;
    let tokens = lex(expression)?;
    let (condition, used_names, used_values) = parse_key_condition_tokens(&tokens, names, values)?;
    require_exact_bindings(names, values, &used_names, &used_values)?;
    Ok(condition)
}

fn parse_key_condition_tokens(
    tokens: &[Token],
    names: &BTreeMap<String, String>,
    values: &BTreeMap<String, AttributeValue>,
) -> Result<(KeyCondition, BTreeSet<String>, BTreeSet<String>)> {
    let [Token::Name(partition_name), Token::Equal, Token::Value(partition_value), rest @ ..] =
        tokens
    else {
        return Err(key_condition_syntax_error());
    };
    let mut used_names = BTreeSet::from([partition_name.clone()]);
    let mut used_values = BTreeSet::from([partition_value.clone()]);
    let partition_name = resolve_name(partition_name, names)?;
    let partition_value = resolve_expression_value(partition_value, values)?;
    let sort = if rest.is_empty() {
        None
    } else {
        let [Token::Identifier(and), sort_tokens @ ..] = rest else {
            return Err(key_condition_syntax_error());
        };
        if !and.eq_ignore_ascii_case("AND") {
            return Err(key_condition_syntax_error());
        }
        let (name, condition, value_placeholders) =
            parse_sort_condition(sort_tokens, names, values)?;
        used_names.insert(name.0);
        used_values.extend(value_placeholders);
        Some((name.1, condition))
    };
    Ok((
        KeyCondition {
            partition_name,
            partition_value,
            sort,
        },
        used_names,
        used_values,
    ))
}

fn parse_sort_condition(
    tokens: &[Token],
    names: &BTreeMap<String, String>,
    values: &BTreeMap<String, AttributeValue>,
) -> Result<((String, String), SortKeyCondition, BTreeSet<String>)> {
    let build = |name: &String,
                 placeholder: &String,
                 constructor: fn(AttributeValue) -> SortKeyCondition|
     -> Result<_> {
        Ok((
            (name.clone(), resolve_name(name, names)?),
            constructor(resolve_expression_value(placeholder, values)?),
            BTreeSet::from([placeholder.clone()]),
        ))
    };
    match tokens {
        [Token::Name(name), Token::Equal, Token::Value(value)] => {
            build(name, value, SortKeyCondition::Equal)
        }
        [Token::Name(name), Token::Less, Token::Value(value)] => {
            build(name, value, SortKeyCondition::LessThan)
        }
        [Token::Name(name), Token::LessEqual, Token::Value(value)] => {
            build(name, value, SortKeyCondition::LessThanOrEqual)
        }
        [Token::Name(name), Token::Greater, Token::Value(value)] => {
            build(name, value, SortKeyCondition::GreaterThan)
        }
        [Token::Name(name), Token::GreaterEqual, Token::Value(value)] => {
            build(name, value, SortKeyCondition::GreaterThanOrEqual)
        }
        [Token::Name(name), Token::Identifier(between), Token::Value(lower), Token::Identifier(and), Token::Value(upper)]
            if between.eq_ignore_ascii_case("BETWEEN") && and.eq_ignore_ascii_case("AND") =>
        {
            Ok((
                (name.clone(), resolve_name(name, names)?),
                SortKeyCondition::Between(
                    resolve_expression_value(lower, values)?,
                    resolve_expression_value(upper, values)?,
                ),
                BTreeSet::from([lower.clone(), upper.clone()]),
            ))
        }
        [Token::Identifier(function), Token::LeftParen, Token::Name(name), Token::Comma, Token::Value(value), Token::RightParen]
            if function == "begins_with" =>
        {
            build(name, value, SortKeyCondition::BeginsWith)
        }
        _ => Err(key_condition_syntax_error()),
    }
}

fn resolve_expression_value(
    placeholder: &str,
    values: &BTreeMap<String, AttributeValue>,
) -> Result<AttributeValue> {
    crate::canonicalize_attribute_value(values.get(placeholder).ok_or_else(|| {
        Error::Validation(format!("missing expression attribute value {placeholder}"))
    })?)
}

fn key_condition_syntax_error() -> Error {
    Error::Unsupported(
        "Query key condition requires #pk = :pk with optional sort-key =, <, <=, >, >=, BETWEEN, or begins_with"
            .into(),
    )
}

/// Parse Query/Scan key, filter, and projection expressions against DynamoDB's
/// one shared expression-name/value namespace.
pub fn parse_read_expressions(
    key_condition_expression: Option<&str>,
    filter_expression: Option<&str>,
    projection_expression: Option<&str>,
    names: &BTreeMap<String, String>,
    values: &BTreeMap<String, AttributeValue>,
) -> Result<ParsedReadExpressions> {
    let expressions = [
        ("key condition", key_condition_expression),
        ("filter", filter_expression),
        ("projection", projection_expression),
    ];
    let Some((_, first)) = expressions.iter().find(|(_, value)| value.is_some()) else {
        if names.is_empty() && values.is_empty() {
            return Ok(ParsedReadExpressions {
                key_condition: None,
                filter: None,
                projection: None,
            });
        }
        return Err(Error::Validation(
            "expression bindings were supplied without an expression".into(),
        ));
    };
    validate_bindings(first.expect("selected present expression"), names, values)?;
    for (kind, expression) in expressions {
        if let Some(expression) = expression {
            validate_expression_length(expression, kind)?;
        }
    }

    let mut used_names = BTreeSet::new();
    let mut used_values = BTreeSet::new();
    let key_condition = match key_condition_expression {
        Some(expression) => {
            let tokens = lex(expression)?;
            let (condition, names, values) = parse_key_condition_tokens(&tokens, names, values)?;
            used_names.extend(names);
            used_values.extend(values);
            Some(condition)
        }
        None => None,
    };
    let filter = match filter_expression {
        Some(expression) => {
            let tokens = lex(expression)?;
            let (condition, names, values) = parse_condition_tokens(&tokens, names, values)?;
            used_names.extend(names);
            used_values.extend(values);
            Some(condition)
        }
        None => None,
    };
    let projection = match projection_expression {
        Some(expression) => {
            let tokens = lex(expression)?;
            let (projection, names) = parse_projection_tokens(&tokens, names)?;
            used_names.extend(names);
            Some(projection)
        }
        None => None,
    };
    require_exact_bindings(names, values, &used_names, &used_values)?;
    Ok(ParsedReadExpressions {
        key_condition,
        filter,
        projection,
    })
}

fn parse_condition_tokens(
    tokens: &[Token],
    names: &BTreeMap<String, String>,
    values: &BTreeMap<String, AttributeValue>,
) -> Result<(Condition, BTreeSet<String>, BTreeSet<String>)> {
    let mut parser = ConditionParser {
        tokens,
        names,
        values,
        offset: 0,
        used_names: BTreeSet::new(),
        used_values: BTreeSet::new(),
        nesting: 0,
    };
    let condition = parser.parse_or()?;
    if parser.offset != tokens.len() {
        return Err(condition_syntax_error());
    }
    Ok((condition, parser.used_names, parser.used_values))
}

struct ConditionParser<'a> {
    tokens: &'a [Token],
    names: &'a BTreeMap<String, String>,
    values: &'a BTreeMap<String, AttributeValue>,
    offset: usize,
    used_names: BTreeSet<String>,
    used_values: BTreeSet<String>,
    nesting: usize,
}

impl<'a> ConditionParser<'a> {
    fn parse_or(&mut self) -> Result<Condition> {
        let mut condition = self.parse_and()?;
        while self.consume_keyword("OR") {
            condition = Condition::Or(Box::new(condition), Box::new(self.parse_and()?));
        }
        Ok(condition)
    }

    fn parse_and(&mut self) -> Result<Condition> {
        let mut condition = self.parse_not()?;
        while self.consume_keyword("AND") {
            condition = Condition::And(Box::new(condition), Box::new(self.parse_not()?));
        }
        Ok(condition)
    }

    fn parse_not(&mut self) -> Result<Condition> {
        if self.consume_keyword("NOT") {
            return self.parse_nested(|parser| Ok(Condition::Not(Box::new(parser.parse_not()?))));
        }
        self.parse_primary()
    }

    fn parse_primary(&mut self) -> Result<Condition> {
        if matches!(self.peek(), Some(Token::LeftParen)) {
            self.offset += 1;
            let condition = self.parse_nested(Self::parse_or)?;
            if !matches!(self.next(), Some(Token::RightParen)) {
                return Err(condition_syntax_error());
            }
            return Ok(condition);
        }
        match self.peek() {
            Some(Token::Identifier(function)) if function == "attribute_exists" => {
                self.offset += 1;
                Ok(Condition::AttributeExists(self.parse_function_name()?))
            }
            Some(Token::Identifier(function)) if function == "attribute_not_exists" => {
                self.offset += 1;
                Ok(Condition::AttributeNotExists(self.parse_function_name()?))
            }
            Some(Token::Identifier(function)) if function == "begins_with" => {
                self.offset += 1;
                let (name, value) = self.parse_name_value_function()?;
                Ok(Condition::BeginsWith { name, value })
            }
            Some(Token::Identifier(function)) if function == "contains" => {
                self.offset += 1;
                let (name, value) = self.parse_name_value_function()?;
                Ok(Condition::Contains { name, value })
            }
            Some(Token::Identifier(function)) if function == "attribute_type" => {
                self.offset += 1;
                let (name, value) = self.parse_name_value_function()?;
                let AttributeValue::S(kind) = value else {
                    return Err(Error::Validation(
                        "attribute_type operand must be a string type code".into(),
                    ));
                };
                if !["S", "SS", "N", "NS", "B", "BS", "BOOL", "NULL", "L", "M"]
                    .contains(&kind.as_str())
                {
                    return Err(Error::Validation(format!(
                        "invalid attribute_type code {kind:?}"
                    )));
                }
                Ok(Condition::AttributeType { name, kind })
            }
            Some(Token::Identifier(function)) if function == "size" => {
                self.offset += 1;
                let name = self.parse_function_name()?;
                let operator = self.parse_comparison_operator()?;
                let value = self.parse_value()?;
                Ok(Condition::SizeComparison {
                    name,
                    operator,
                    value,
                })
            }
            Some(Token::Name(_)) => self.parse_name_condition(),
            _ => Err(condition_syntax_error()),
        }
    }

    fn parse_name_condition(&mut self) -> Result<Condition> {
        let name = self.parse_name()?;
        if self.consume_keyword("BETWEEN") {
            let lower = self.parse_value()?;
            if !self.consume_keyword("AND") {
                return Err(condition_syntax_error());
            }
            let upper = self.parse_value()?;
            return Ok(Condition::Between { name, lower, upper });
        }
        if self.consume_keyword("IN") {
            if !matches!(self.next(), Some(Token::LeftParen)) {
                return Err(condition_syntax_error());
            }
            let mut values = Vec::new();
            loop {
                values.push(self.parse_value()?);
                match self.next() {
                    Some(Token::Comma) if values.len() < 100 => continue,
                    Some(Token::RightParen) => break,
                    _ => return Err(condition_syntax_error()),
                }
            }
            return Ok(Condition::In { name, values });
        }
        let operator = self.parse_comparison_operator()?;
        let value = self.parse_value()?;
        if operator == ComparisonOperator::Equal {
            Ok(Condition::Equals { name, value })
        } else {
            Ok(Condition::Comparison {
                name,
                operator,
                value,
            })
        }
    }

    fn parse_function_name(&mut self) -> Result<AttributePath> {
        if !matches!(self.next(), Some(Token::LeftParen)) {
            return Err(condition_syntax_error());
        }
        let name = self.parse_name()?;
        if !matches!(self.next(), Some(Token::RightParen)) {
            return Err(condition_syntax_error());
        }
        Ok(name)
    }

    fn parse_name_value_function(&mut self) -> Result<(AttributePath, AttributeValue)> {
        if !matches!(self.next(), Some(Token::LeftParen)) {
            return Err(condition_syntax_error());
        }
        let name = self.parse_name()?;
        if !matches!(self.next(), Some(Token::Comma)) {
            return Err(condition_syntax_error());
        }
        let value = self.parse_value()?;
        if !matches!(self.next(), Some(Token::RightParen)) {
            return Err(condition_syntax_error());
        }
        Ok((name, value))
    }

    fn parse_name(&mut self) -> Result<AttributePath> {
        let Some(Token::Name(placeholder)) = self.next() else {
            return Err(condition_syntax_error());
        };
        self.used_names.insert(placeholder.clone());
        let mut elements = vec![PathElement::Name(resolve_name(placeholder, self.names)?)];
        loop {
            match self.peek() {
                Some(Token::Dot) => {
                    self.offset += 1;
                    let Some(Token::Name(placeholder)) = self.next() else {
                        return Err(condition_syntax_error());
                    };
                    self.used_names.insert(placeholder.clone());
                    elements.push(PathElement::Name(resolve_name(placeholder, self.names)?));
                }
                Some(Token::LeftBracket) => {
                    self.offset += 1;
                    let Some(Token::Index(index)) = self.next() else {
                        return Err(condition_syntax_error());
                    };
                    let index = *index;
                    if !matches!(self.next(), Some(Token::RightBracket)) {
                        return Err(condition_syntax_error());
                    }
                    elements.push(PathElement::Index(index));
                }
                _ => break,
            }
            if elements.len() > 32 {
                return Err(Error::Validation(
                    "document path exceeds 32 elements".into(),
                ));
            }
        }
        Ok(AttributePath(elements))
    }

    fn parse_value(&mut self) -> Result<AttributeValue> {
        let Some(Token::Value(placeholder)) = self.next() else {
            return Err(condition_syntax_error());
        };
        self.used_values.insert(placeholder.clone());
        resolve_expression_value(placeholder, self.values)
    }

    fn parse_comparison_operator(&mut self) -> Result<ComparisonOperator> {
        Ok(match self.next() {
            Some(Token::Equal) => ComparisonOperator::Equal,
            Some(Token::NotEqual) => ComparisonOperator::NotEqual,
            Some(Token::Less) => ComparisonOperator::LessThan,
            Some(Token::LessEqual) => ComparisonOperator::LessThanOrEqual,
            Some(Token::Greater) => ComparisonOperator::GreaterThan,
            Some(Token::GreaterEqual) => ComparisonOperator::GreaterThanOrEqual,
            _ => return Err(condition_syntax_error()),
        })
    }

    fn consume_keyword(&mut self, expected: &str) -> bool {
        match self.peek() {
            Some(Token::Identifier(value)) if value.eq_ignore_ascii_case(expected) => {
                self.offset += 1;
                true
            }
            _ => false,
        }
    }

    fn parse_nested<T>(&mut self, parse: impl FnOnce(&mut Self) -> Result<T>) -> Result<T> {
        if self.nesting >= MAX_EXPRESSION_NESTING {
            return Err(Error::Validation(format!(
                "condition nesting exceeds {MAX_EXPRESSION_NESTING} levels"
            )));
        }
        self.nesting += 1;
        let result = parse(self);
        self.nesting -= 1;
        result
    }

    fn peek(&self) -> Option<&'a Token> {
        self.tokens.get(self.offset)
    }

    fn next(&mut self) -> Option<&'a Token> {
        let token = self.tokens.get(self.offset);
        self.offset += usize::from(token.is_some());
        token
    }
}

fn condition_syntax_error() -> Error {
    Error::Unsupported(
        "unsupported condition expression; use aliases with documented comparisons, BETWEEN, IN, boolean operators, or supported functions"
            .into(),
    )
}

fn require_exact_bindings(
    names: &BTreeMap<String, String>,
    values: &BTreeMap<String, AttributeValue>,
    used_names: &BTreeSet<String>,
    used_values: &BTreeSet<String>,
) -> Result<()> {
    let declared_names = names.keys().cloned().collect::<BTreeSet<_>>();
    let declared_values = values.keys().cloned().collect::<BTreeSet<_>>();
    if declared_names != *used_names || declared_values != *used_values {
        return Err(Error::Validation(
            "expression contains missing or unused expression bindings".into(),
        ));
    }
    Ok(())
}

/// Parse one update expression and an optional condition against one shared
/// binding namespace. The deliberately small grammar is rejected closed.
pub fn parse_update(
    update_expression: &str,
    condition_expression: Option<&str>,
    names: &BTreeMap<String, String>,
    values: &BTreeMap<String, AttributeValue>,
) -> Result<ParsedUpdate> {
    validate_bindings(update_expression, names, values)?;
    if let Some(condition) = condition_expression {
        validate_expression_length(condition, "condition")?;
    }

    let tokens = lex(update_expression)?;
    let mut parser = UpdateParser::new(&tokens, names, values);
    let plan = parser.parse()?;
    let mut used_names = parser.used_names;
    let mut used_values = parser.used_values;
    let condition = if let Some(expression) = condition_expression {
        let tokens = lex(expression)?;
        let (condition, condition_names, condition_values) =
            parse_condition_tokens(&tokens, names, values)?;
        used_names.extend(condition_names);
        used_values.extend(condition_values);
        Some(condition)
    } else {
        None
    };
    require_exact_bindings(names, values, &used_names, &used_values)?;
    Ok(ParsedUpdate { plan, condition })
}

struct UpdateParser<'a> {
    tokens: &'a [Token],
    names: &'a BTreeMap<String, String>,
    values: &'a BTreeMap<String, AttributeValue>,
    offset: usize,
    used_names: BTreeSet<String>,
    used_values: BTreeSet<String>,
    nesting: usize,
}

impl<'a> UpdateParser<'a> {
    fn new(
        tokens: &'a [Token],
        names: &'a BTreeMap<String, String>,
        values: &'a BTreeMap<String, AttributeValue>,
    ) -> Self {
        Self {
            tokens,
            names,
            values,
            offset: 0,
            used_names: BTreeSet::new(),
            used_values: BTreeSet::new(),
            nesting: 0,
        }
    }

    fn parse(&mut self) -> Result<UpdatePlan> {
        let mut actions = Vec::new();
        let mut clauses = BTreeSet::new();
        while self.offset < self.tokens.len() {
            let clause = match self.next() {
                Some(Token::Identifier(value)) if is_update_clause(value) => value.to_uppercase(),
                _ => return Err(update_syntax_error()),
            };
            if !clauses.insert(clause.clone()) {
                return Err(Error::Validation(format!(
                    "update expression repeats {clause} clause"
                )));
            }
            let before = actions.len();
            loop {
                actions.push(match clause.as_str() {
                    "SET" => self.parse_set()?,
                    "REMOVE" => UpdateAction::Remove {
                        target: self.parse_name()?,
                    },
                    "ADD" => UpdateAction::Add {
                        target: self.parse_name()?,
                        value: self.parse_value()?,
                    },
                    "DELETE" => UpdateAction::Delete {
                        target: self.parse_name()?,
                        value: self.parse_value()?,
                    },
                    _ => unreachable!(),
                });
                if matches!(self.peek(), Some(Token::Comma)) {
                    self.offset += 1;
                    continue;
                }
                if self.peek().is_none()
                    || matches!(self.peek(), Some(Token::Identifier(value)) if is_update_clause(value))
                {
                    break;
                }
                return Err(update_syntax_error());
            }
            debug_assert!(actions.len() > before);
        }
        if actions.is_empty() {
            return Err(update_syntax_error());
        }
        let plan = UpdatePlan { actions };
        let mut targets: Vec<&AttributePath> = Vec::new();
        for action in plan.actions() {
            if targets
                .iter()
                .any(|existing| existing.overlaps(action.target()))
            {
                return Err(Error::Validation(format!(
                    "multiple update actions target attribute {:?}",
                    action.target()
                )));
            }
            targets.push(action.target());
        }
        Ok(plan)
    }

    fn parse_set(&mut self) -> Result<UpdateAction> {
        let target = self.parse_name()?;
        if !matches!(self.next(), Some(Token::Equal)) {
            return Err(update_syntax_error());
        }
        let left = self.parse_update_operand()?;
        let operand = match self.peek() {
            Some(Token::Plus) => {
                self.offset += 1;
                SetOperand::Arithmetic {
                    left,
                    operator: ArithmeticOperator::Add,
                    right: self.parse_update_operand()?,
                }
            }
            Some(Token::Minus) => {
                self.offset += 1;
                SetOperand::Arithmetic {
                    left,
                    operator: ArithmeticOperator::Subtract,
                    right: self.parse_update_operand()?,
                }
            }
            _ => SetOperand::Operand(left),
        };
        Ok(UpdateAction::Set { target, operand })
    }

    fn parse_update_operand(&mut self) -> Result<UpdateOperand> {
        match self.peek() {
            Some(Token::Value(value)) => {
                let value = value.clone();
                self.offset += 1;
                Ok(UpdateOperand::Value(self.resolve_value(&value)?))
            }
            Some(Token::Name(_)) => Ok(UpdateOperand::Path(self.parse_name()?)),
            Some(Token::Identifier(function)) if function == "if_not_exists" => {
                self.offset += 1;
                if !matches!(self.next(), Some(Token::LeftParen)) {
                    return Err(update_syntax_error());
                }
                let source = self.parse_name()?;
                if !matches!(self.next(), Some(Token::Comma)) {
                    return Err(update_syntax_error());
                }
                let default = self.parse_value()?;
                if !matches!(self.next(), Some(Token::RightParen)) {
                    return Err(update_syntax_error());
                }
                Ok(UpdateOperand::IfNotExists { source, default })
            }
            Some(Token::Identifier(function)) if function == "list_append" => {
                self.offset += 1;
                self.parse_nested_operand(|parser| {
                    if !matches!(parser.next(), Some(Token::LeftParen)) {
                        return Err(update_syntax_error());
                    }
                    let left = parser.parse_update_operand()?;
                    if !matches!(parser.next(), Some(Token::Comma)) {
                        return Err(update_syntax_error());
                    }
                    let right = parser.parse_update_operand()?;
                    if !matches!(parser.next(), Some(Token::RightParen)) {
                        return Err(update_syntax_error());
                    }
                    Ok(UpdateOperand::ListAppend {
                        left: Box::new(left),
                        right: Box::new(right),
                    })
                })
            }
            _ => Err(update_syntax_error()),
        }
    }

    fn parse_name(&mut self) -> Result<AttributePath> {
        let Some(Token::Name(name)) = self.next() else {
            return Err(update_syntax_error());
        };
        let mut elements = vec![PathElement::Name(self.resolve_path_name(name)?)];
        loop {
            match self.peek() {
                Some(Token::Dot) => {
                    self.offset += 1;
                    let Some(Token::Name(name)) = self.next() else {
                        return Err(update_syntax_error());
                    };
                    elements.push(PathElement::Name(self.resolve_path_name(name)?));
                }
                Some(Token::LeftBracket) => {
                    self.offset += 1;
                    let Some(Token::Index(index)) = self.next() else {
                        return Err(update_syntax_error());
                    };
                    let index = *index;
                    if !matches!(self.next(), Some(Token::RightBracket)) {
                        return Err(update_syntax_error());
                    }
                    elements.push(PathElement::Index(index));
                }
                _ => break,
            }
            if elements.len() > 32 {
                return Err(Error::Validation(
                    "document path exceeds 32 elements".into(),
                ));
            }
        }
        Ok(AttributePath(elements))
    }

    fn parse_nested_operand<T>(&mut self, parse: impl FnOnce(&mut Self) -> Result<T>) -> Result<T> {
        if self.nesting >= MAX_EXPRESSION_NESTING {
            return Err(Error::Validation(format!(
                "update operand nesting exceeds {MAX_EXPRESSION_NESTING} levels"
            )));
        }
        self.nesting += 1;
        let result = parse(self);
        self.nesting -= 1;
        result
    }

    fn resolve_path_name(&mut self, placeholder: &str) -> Result<String> {
        self.used_names.insert(placeholder.to_owned());
        resolve_name(placeholder, self.names)
    }

    fn parse_value(&mut self) -> Result<AttributeValue> {
        let Some(Token::Value(value)) = self.next() else {
            return Err(update_syntax_error());
        };
        self.resolve_value(value)
    }

    fn resolve_value(&mut self, placeholder: &str) -> Result<AttributeValue> {
        self.used_values.insert(placeholder.to_owned());
        crate::canonicalize_attribute_value(self.values.get(placeholder).ok_or_else(|| {
            Error::Validation(format!("missing expression attribute value {placeholder}"))
        })?)
    }

    fn peek(&self) -> Option<&'a Token> {
        self.tokens.get(self.offset)
    }

    fn next(&mut self) -> Option<&'a Token> {
        let token = self.tokens.get(self.offset);
        self.offset += usize::from(token.is_some());
        token
    }
}

fn is_update_clause(value: &str) -> bool {
    ["SET", "REMOVE", "ADD", "DELETE"]
        .iter()
        .any(|keyword| value.eq_ignore_ascii_case(keyword))
}

fn update_syntax_error() -> Error {
    Error::Unsupported(
        "unsupported update expression; SET supports path/value operands, exact arithmetic, if_not_exists, and list_append; REMOVE supports paths; ADD/DELETE support top-level numbers or sets"
            .into(),
    )
}

fn get_path<'a>(item: &'a Item, path: &AttributePath) -> Result<Option<&'a AttributeValue>> {
    path.validate()?;
    let mut value = match &path.0[0] {
        PathElement::Name(name) => item.get(name),
        PathElement::Index(_) => unreachable!("validated path root"),
    };
    for element in &path.0[1..] {
        value = match (value, element) {
            (Some(AttributeValue::M(map)), PathElement::Name(name)) => map.get(name),
            (Some(AttributeValue::L(list)), PathElement::Index(index)) => list.get(*index),
            (None, _) => return Ok(None),
            _ => return Ok(None),
        };
    }
    Ok(value)
}

fn set_path(item: &mut Item, path: &AttributePath, value: AttributeValue) -> Result<()> {
    if path.is_top_level() {
        item.insert(path.root_name().to_owned(), value);
        return Ok(());
    }
    let mut current = item
        .get_mut(path.root_name())
        .ok_or_else(|| Error::Validation("document path parent does not exist for SET".into()))?;
    for element in &path.0[1..path.0.len() - 1] {
        current = match (current, element) {
            (AttributeValue::M(map), PathElement::Name(name)) => map.get_mut(name),
            (AttributeValue::L(list), PathElement::Index(index)) => list.get_mut(*index),
            _ => None,
        }
        .ok_or_else(|| Error::Validation("document path is invalid for SET".into()))?;
    }
    match (current, path.0.last().expect("non-empty path")) {
        (AttributeValue::M(map), PathElement::Name(name)) => {
            map.insert(name.clone(), value);
        }
        (AttributeValue::L(list), PathElement::Index(index)) if *index < list.len() => {
            list[*index] = value;
        }
        (AttributeValue::L(list), PathElement::Index(_)) => list.push(value),
        _ => return Err(Error::Validation("document path is invalid for SET".into())),
    }
    Ok(())
}

fn remove_path(item: &mut Item, path: &AttributePath) -> Result<()> {
    if path.is_top_level() {
        item.remove(path.root_name());
        return Ok(());
    }
    let mut current = item.get_mut(path.root_name()).ok_or_else(|| {
        Error::Validation("document path parent does not exist for REMOVE".into())
    })?;
    for element in &path.0[1..path.0.len() - 1] {
        current = match (current, element) {
            (AttributeValue::M(map), PathElement::Name(name)) => map.get_mut(name),
            (AttributeValue::L(list), PathElement::Index(index)) => list.get_mut(*index),
            _ => None,
        }
        .ok_or_else(|| Error::Validation("document path is invalid for REMOVE".into()))?;
    }
    match (current, path.0.last().expect("non-empty path")) {
        (AttributeValue::M(map), PathElement::Name(name)) => {
            map.remove(name);
        }
        (AttributeValue::L(list), PathElement::Index(index)) if *index < list.len() => {
            list.remove(*index);
        }
        (AttributeValue::L(_), PathElement::Index(_)) => {}
        _ => {
            return Err(Error::Validation(
                "document path is invalid for REMOVE".into(),
            ))
        }
    }
    Ok(())
}

fn evaluate_set_operand(operand: &SetOperand, old: &Item) -> Result<AttributeValue> {
    match operand {
        SetOperand::Operand(operand) => evaluate_update_operand(operand, old),
        SetOperand::Arithmetic {
            left,
            operator,
            right,
        } => {
            let AttributeValue::N(left) = evaluate_update_operand(left, old)? else {
                return Err(Error::Validation(
                    "left arithmetic operand must resolve to a number".into(),
                ));
            };
            let AttributeValue::N(right) = evaluate_update_operand(right, old)? else {
                return Err(Error::Validation(
                    "right arithmetic operand must resolve to a number".into(),
                ));
            };
            let result = match operator {
                ArithmeticOperator::Add => left.checked_add(&right)?,
                ArithmeticOperator::Subtract => left.checked_sub(&right)?,
            };
            Ok(AttributeValue::N(result))
        }
    }
}

fn evaluate_update_operand(operand: &UpdateOperand, old: &Item) -> Result<AttributeValue> {
    evaluate_update_operand_at_depth(operand, old, 0)
}

fn evaluate_update_operand_at_depth(
    operand: &UpdateOperand,
    old: &Item,
    depth: usize,
) -> Result<AttributeValue> {
    if depth > MAX_CONDITION_AST_DEPTH {
        return Err(Error::Validation(format!(
            "update operand AST depth exceeds {MAX_CONDITION_AST_DEPTH} levels"
        )));
    }
    match operand {
        UpdateOperand::Value(value) => Ok(value.clone()),
        UpdateOperand::Path(path) => get_path(old, path)?.cloned().ok_or_else(|| {
            Error::Validation(format!("update operand path {path:?} does not exist"))
        }),
        UpdateOperand::IfNotExists { source, default } => Ok(get_path(old, source)?
            .cloned()
            .unwrap_or_else(|| default.clone())),
        UpdateOperand::ListAppend { left, right } => {
            let AttributeValue::L(mut left) =
                evaluate_update_operand_at_depth(left, old, depth + 1)?
            else {
                return Err(Error::Validation(
                    "left list_append operand must resolve to a list".into(),
                ));
            };
            let AttributeValue::L(right) = evaluate_update_operand_at_depth(right, old, depth + 1)?
            else {
                return Err(Error::Validation(
                    "right list_append operand must resolve to a list".into(),
                ));
            };
            left.extend(right);
            Ok(AttributeValue::L(left))
        }
    }
}

fn apply_add(
    result: &mut Item,
    old: &Item,
    target: &AttributePath,
    value: &AttributeValue,
) -> Result<()> {
    let name = target.root_name();
    let updated = match (old.get(name), value) {
        (None, AttributeValue::N(value)) => AttributeValue::N(value.clone()),
        (Some(AttributeValue::N(left)), AttributeValue::N(right)) => {
            AttributeValue::N(left.checked_add(right)?)
        }
        (None, AttributeValue::Ss(values)) => AttributeValue::Ss(values.clone()),
        (Some(AttributeValue::Ss(left)), AttributeValue::Ss(right)) => AttributeValue::Ss(
            left.iter()
                .chain(right)
                .cloned()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
        ),
        (None, AttributeValue::Ns(values)) => AttributeValue::Ns(values.clone()),
        (Some(AttributeValue::Ns(left)), AttributeValue::Ns(right)) => AttributeValue::Ns(
            left.iter()
                .chain(right)
                .cloned()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
        ),
        (None, AttributeValue::Bs(values)) => AttributeValue::Bs(values.clone()),
        (Some(AttributeValue::Bs(left)), AttributeValue::Bs(right)) => AttributeValue::Bs(
            left.iter()
                .chain(right)
                .cloned()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
        ),
        _ => {
            return Err(Error::Validation(
                "ADD requires matching number or homogeneous set operands".into(),
            ))
        }
    };
    result.insert(name.to_owned(), updated);
    Ok(())
}

fn apply_delete(
    result: &mut Item,
    old: &Item,
    target: &AttributePath,
    value: &AttributeValue,
) -> Result<()> {
    let name = target.root_name();
    let Some(existing) = old.get(name) else {
        return match value {
            AttributeValue::Ss(_) | AttributeValue::Ns(_) | AttributeValue::Bs(_) => Ok(()),
            _ => Err(Error::Validation("DELETE requires a set operand".into())),
        };
    };
    let updated = match (existing, value) {
        (AttributeValue::Ss(left), AttributeValue::Ss(right)) => {
            subtract_set(left, right).map(AttributeValue::Ss)
        }
        (AttributeValue::Ns(left), AttributeValue::Ns(right)) => {
            subtract_set(left, right).map(AttributeValue::Ns)
        }
        (AttributeValue::Bs(left), AttributeValue::Bs(right)) => {
            subtract_set(left, right).map(AttributeValue::Bs)
        }
        _ => {
            return Err(Error::Validation(
                "DELETE requires matching homogeneous set operands".into(),
            ))
        }
    };
    if let Some(updated) = updated {
        result.insert(name.to_owned(), updated);
    } else {
        result.remove(name);
    }
    Ok(())
}

fn subtract_set<T: Ord + Clone>(left: &[T], right: &[T]) -> Option<Vec<T>> {
    let right = right.iter().collect::<BTreeSet<_>>();
    let result = left
        .iter()
        .filter(|value| !right.contains(value))
        .cloned()
        .collect::<Vec<_>>();
    (!result.is_empty()).then_some(result)
}

fn validate_bindings(
    expression: &str,
    names: &BTreeMap<String, String>,
    values: &BTreeMap<String, AttributeValue>,
) -> Result<()> {
    validate_expression_length(expression, "update or condition")?;
    let mut binding_bytes = 0_usize;
    for (placeholder, name) in names {
        validate_placeholder(placeholder, b'#')?;
        if name.is_empty() || name.len() > MAX_PLACEHOLDER_BYTES {
            return Err(Error::Validation(format!(
                "expression attribute name length must be 1..={MAX_PLACEHOLDER_BYTES} UTF-8 bytes"
            )));
        }
        binding_bytes = binding_bytes
            .checked_add(placeholder.len())
            .and_then(|size| size.checked_add(name.len()))
            .ok_or_else(|| Error::Validation("expression binding size overflow".into()))?;
    }
    for (placeholder, value) in values {
        validate_placeholder(placeholder, b':')?;
        let canonical = crate::canonicalize_attribute_value(value)?;
        let encoded = serde_cbor::to_vec(&canonical)
            .map_err(|error| Error::Serialization(error.to_string()))?;
        binding_bytes = binding_bytes
            .checked_add(placeholder.len())
            .and_then(|size| size.checked_add(encoded.len()))
            .ok_or_else(|| Error::Validation("expression binding size overflow".into()))?;
    }
    if binding_bytes > MAX_BINDING_BYTES {
        return Err(Error::Validation(format!(
            "expression bindings exceed {MAX_BINDING_BYTES} bytes"
        )));
    }
    Ok(())
}

fn validate_expression_length(expression: &str, kind: &str) -> Result<()> {
    if expression.is_empty() || expression.len() > MAX_EXPRESSION_BYTES {
        return Err(Error::Validation(format!(
            "{kind} expression length must be 1..={MAX_EXPRESSION_BYTES} UTF-8 bytes"
        )));
    }
    Ok(())
}

fn validate_placeholder(value: &str, prefix: u8) -> Result<()> {
    let bytes = value.as_bytes();
    if !(2..=MAX_PLACEHOLDER_BYTES).contains(&bytes.len())
        || bytes[0] != prefix
        || !bytes[1..]
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
    {
        return Err(Error::Validation(format!(
            "invalid expression placeholder {value:?}"
        )));
    }
    Ok(())
}

fn resolve_name(value: &str, names: &BTreeMap<String, String>) -> Result<String> {
    names
        .get(value)
        .cloned()
        .ok_or_else(|| Error::Validation(format!("missing expression attribute name {value}")))
}

fn lex(expression: &str) -> Result<Vec<Token>> {
    let bytes = expression.as_bytes();
    let mut tokens = Vec::new();
    let mut offset = 0;
    while offset < bytes.len() {
        match bytes[offset] {
            byte if byte.is_ascii_whitespace() => offset += 1,
            b'(' => {
                tokens.push(Token::LeftParen);
                offset += 1;
            }
            b')' => {
                tokens.push(Token::RightParen);
                offset += 1;
            }
            b',' => {
                tokens.push(Token::Comma);
                offset += 1;
            }
            b'.' => {
                tokens.push(Token::Dot);
                offset += 1;
            }
            b'[' => {
                tokens.push(Token::LeftBracket);
                offset += 1;
            }
            b']' => {
                tokens.push(Token::RightBracket);
                offset += 1;
            }
            b'=' => {
                tokens.push(Token::Equal);
                offset += 1;
            }
            b'+' => {
                tokens.push(Token::Plus);
                offset += 1;
            }
            b'-' => {
                tokens.push(Token::Minus);
                offset += 1;
            }
            b'<' => {
                if bytes.get(offset + 1) == Some(&b'>') {
                    tokens.push(Token::NotEqual);
                    offset += 2;
                } else if bytes.get(offset + 1) == Some(&b'=') {
                    tokens.push(Token::LessEqual);
                    offset += 2;
                } else {
                    tokens.push(Token::Less);
                    offset += 1;
                }
            }
            b'>' => {
                if bytes.get(offset + 1) == Some(&b'=') {
                    tokens.push(Token::GreaterEqual);
                    offset += 2;
                } else {
                    tokens.push(Token::Greater);
                    offset += 1;
                }
            }
            prefix @ (b'#' | b':') => {
                let start = offset;
                offset += 1;
                while offset < bytes.len()
                    && (bytes[offset].is_ascii_alphanumeric() || bytes[offset] == b'_')
                {
                    offset += 1;
                }
                let value = &expression[start..offset];
                validate_placeholder(value, prefix)?;
                tokens.push(if prefix == b'#' {
                    Token::Name(value.into())
                } else {
                    Token::Value(value.into())
                });
            }
            byte if byte.is_ascii_digit() => {
                let start = offset;
                offset += 1;
                while offset < bytes.len() && bytes[offset].is_ascii_digit() {
                    offset += 1;
                }
                let index = expression[start..offset]
                    .parse::<usize>()
                    .map_err(|_| Error::Validation("document path index overflow".into()))?;
                tokens.push(Token::Index(index));
            }
            byte if byte.is_ascii_alphabetic() || byte == b'_' => {
                let start = offset;
                offset += 1;
                while offset < bytes.len()
                    && (bytes[offset].is_ascii_alphanumeric() || bytes[offset] == b'_')
                {
                    offset += 1;
                }
                tokens.push(Token::Identifier(expression[start..offset].into()));
            }
            _ => {
                return Err(Error::Validation(format!(
                    "invalid expression token at byte {offset}"
                )))
            }
        }
    }
    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DynamoNumber;

    #[test]
    fn attribute_path_deserialization_and_update_application_fail_closed() {
        let valid = AttributePath(vec![
            PathElement::Name("profile".into()),
            PathElement::Index(0),
        ]);
        let encoded = serde_json::to_vec(&valid).unwrap();
        assert_eq!(
            serde_json::from_slice::<AttributePath>(&encoded).unwrap(),
            valid
        );

        for invalid in [
            Vec::<PathElement>::new(),
            vec![PathElement::Index(0)],
            vec![PathElement::Name(String::new())],
            (0..33)
                .map(|index| PathElement::Name(format!("level-{index}")))
                .collect(),
        ] {
            let encoded = serde_json::to_vec(&invalid).unwrap();
            assert!(serde_json::from_slice::<AttributePath>(&encoded).is_err());
        }

        assert!(serde_json::from_str::<UpdatePlan>(r#"{"actions":[]}"#).is_err());
        assert!(serde_json::from_str::<Projection>(r#"{"attributes":[]}"#).is_err());

        let projection = Projection {
            attributes: vec![AttributePath::top_level("state")],
        };
        let mut encoded = serde_json::to_value(&projection).unwrap();
        let attributes = encoded["attributes"].as_array_mut().unwrap();
        attributes.push(attributes[0].clone());
        assert!(serde_json::from_value::<Projection>(encoded).is_err());

        let invalid = AttributePath(Vec::new());
        let plan = UpdatePlan {
            actions: vec![UpdateAction::Remove {
                target: invalid.clone(),
            }],
        };
        assert!(matches!(
            plan.apply(&Item::new(), std::iter::empty()),
            Err(Error::Validation(_))
        ));
        assert!(matches!(
            Condition::AttributeExists(invalid).evaluate(Some(&Item::new())),
            Err(Error::Validation(_))
        ));
    }

    #[test]
    fn condition_parser_and_evaluator_bound_recursive_nesting() {
        let names = BTreeMap::from([("#state".into(), "state".into())]);
        let expression = format!(
            "{}attribute_exists(#state)",
            "NOT ".repeat(MAX_EXPRESSION_NESTING + 1)
        );
        assert!(matches!(
            parse_condition(&expression, &names, &BTreeMap::new()),
            Err(Error::Validation(message)) if message.contains("nesting exceeds")
        ));

        let mut condition = Condition::AttributeExists(AttributePath::top_level("state"));
        for _ in 0..=MAX_CONDITION_AST_DEPTH {
            condition = Condition::Not(Box::new(condition));
        }
        assert!(matches!(
            condition.evaluate(Some(&Item::new())),
            Err(Error::Validation(message)) if message.contains("AST depth exceeds")
        ));

        let item = Item::from([("state".into(), AttributeValue::S("OPEN".into()))]);
        let short_circuited = Condition::Or(
            Box::new(Condition::AttributeExists(AttributePath::top_level(
                "state",
            ))),
            Box::new(condition),
        );
        assert!(matches!(
            short_circuited.evaluate(Some(&item)),
            Err(Error::Validation(message)) if message.contains("AST depth exceeds")
        ));
    }

    #[test]
    fn update_parser_and_evaluator_bound_recursive_operands() {
        let names = BTreeMap::from([("#list".into(), "list".into())]);
        let values = BTreeMap::from([(":empty".into(), AttributeValue::L(Vec::new()))]);
        let nested = "list_append(".repeat(MAX_EXPRESSION_NESTING + 1);
        let suffix = ",:empty)".repeat(MAX_EXPRESSION_NESTING + 1);
        let expression = format!("SET #list={nested}:empty{suffix}");
        assert!(matches!(
            parse_update(&expression, None, &names, &values),
            Err(Error::Validation(message)) if message.contains("operand nesting exceeds")
        ));

        let mut operand = UpdateOperand::Value(AttributeValue::L(Vec::new()));
        for _ in 0..=MAX_CONDITION_AST_DEPTH {
            operand = UpdateOperand::ListAppend {
                left: Box::new(operand),
                right: Box::new(UpdateOperand::Value(AttributeValue::L(Vec::new()))),
            };
        }
        assert!(matches!(
            evaluate_update_operand(&operand, &Item::new()),
            Err(Error::Validation(message)) if message.contains("AST depth exceeds")
        ));
    }

    #[test]
    fn parser_is_whitespace_independent_and_rejects_unused_bindings() {
        let names = BTreeMap::from([("#status".into(), "status".into())]);
        let values = BTreeMap::from([(":open".into(), AttributeValue::S("OPEN".into()))]);
        assert_eq!(
            parse_condition("#status=:open", &names, &values).unwrap(),
            Condition::Equals {
                name: "status".into(),
                value: AttributeValue::S("OPEN".into()),
            }
        );
        assert!(parse_condition("attribute_exists(#status)", &names, &BTreeMap::new()).is_ok());
        assert!(parse_condition("#status = :open", &names, &BTreeMap::new()).is_err());
        assert!(parse_condition("status = :open", &BTreeMap::new(), &values).is_err());

        let set = BTreeMap::from([(
            ":set".into(),
            AttributeValue::Ss(vec!["z".into(), "a".into()]),
        )]);
        let parsed = parse_condition("#status = :set", &names, &set).unwrap();
        assert_eq!(
            parsed,
            Condition::Equals {
                name: "status".into(),
                value: AttributeValue::Ss(vec!["a".into(), "z".into()]),
            }
        );
    }

    fn number(value: &str) -> AttributeValue {
        AttributeValue::N(DynamoNumber::parse(value).unwrap())
    }

    #[test]
    fn update_operands_use_one_immutable_old_item() {
        let names = BTreeMap::from([
            ("#a".into(), "a".into()),
            ("#b".into(), "b".into()),
            ("#missing".into(), "missing".into()),
        ]);
        let values = BTreeMap::from([
            (":one".into(), number("1")),
            (":fallback".into(), AttributeValue::S("fallback".into())),
        ]);
        let parsed = parse_update(
            "SET #a = #a + :one, #b = if_not_exists(#missing, :fallback)",
            None,
            &names,
            &values,
        )
        .unwrap();
        let old = Item::from([
            ("a".into(), number("2")),
            ("b".into(), AttributeValue::S("old".into())),
        ]);
        assert_eq!(
            parsed.plan.apply(&old, std::iter::empty()).unwrap(),
            Item::from([
                ("a".into(), number("3")),
                ("b".into(), AttributeValue::S("fallback".into())),
            ])
        );
    }

    #[test]
    fn add_delete_and_remove_have_dynamodb_set_semantics() {
        let names = BTreeMap::from([
            ("#count".into(), "count".into()),
            ("#tags".into(), "tags".into()),
            ("#gone".into(), "gone".into()),
        ]);
        let values = BTreeMap::from([
            (":two".into(), number("2")),
            (
                ":add".into(),
                AttributeValue::Ss(vec!["b".into(), "c".into()]),
            ),
            (
                ":delete".into(),
                AttributeValue::Ss(vec!["a".into(), "c".into()]),
            ),
        ]);
        let parsed = parse_update(
            "ADD #count :two, #tags :add DELETE #tags :delete REMOVE #gone",
            None,
            &names,
            &values,
        );
        assert!(parsed.is_err(), "one target cannot appear in two actions");

        let parsed = parse_update(
            "ADD #count :two, #tags :add REMOVE #gone",
            None,
            &names,
            &BTreeMap::from([
                (":two".into(), number("2")),
                (
                    ":add".into(),
                    AttributeValue::Ss(vec!["b".into(), "c".into()]),
                ),
            ]),
        )
        .unwrap();
        let old = Item::from([
            ("count".into(), number("3")),
            (
                "tags".into(),
                AttributeValue::Ss(vec!["a".into(), "b".into()]),
            ),
            ("gone".into(), AttributeValue::Bool(true)),
        ]);
        let updated = parsed.plan.apply(&old, std::iter::empty()).unwrap();
        assert_eq!(updated.get("count"), Some(&number("5")));
        assert_eq!(
            updated.get("tags"),
            Some(&AttributeValue::Ss(vec![
                "a".into(),
                "b".into(),
                "c".into()
            ]))
        );
        assert!(!updated.contains_key("gone"));
    }

    #[test]
    fn update_rejects_key_mutation_and_audits_shared_bindings() {
        let names = BTreeMap::from([
            ("#pk".into(), "pk".into()),
            ("#state".into(), "state".into()),
        ]);
        let values = BTreeMap::from([
            (":next".into(), AttributeValue::S("next".into())),
            (":old".into(), AttributeValue::S("old".into())),
        ]);
        let parsed =
            parse_update("SET #pk = :next", Some("#state = :old"), &names, &values).unwrap();
        let old = Item::from([("pk".into(), AttributeValue::S("id".into()))]);
        assert!(parsed.plan.apply(&old, ["pk"]).is_err());

        let mut unused = names.clone();
        unused.insert("#unused".into(), "unused".into());
        assert!(parse_update("SET #pk = :next", Some("#state = :old"), &unused, &values,).is_err());
        assert!(parse_update("SET #pk = :next trailing", None, &names, &values).is_err());
    }

    #[test]
    fn projection_is_deterministic_and_rejects_duplicates_or_unused_aliases() {
        let names = BTreeMap::from([
            ("#state".into(), "state".into()),
            ("#count".into(), "count".into()),
        ]);
        let projection = parse_projection("#state, #count", &names).unwrap();
        let item = Item::from([
            ("pk".into(), AttributeValue::S("id".into())),
            ("state".into(), AttributeValue::S("OPEN".into())),
            ("count".into(), number("3")),
        ]);
        assert_eq!(
            projection.apply(&item),
            Item::from([
                ("count".into(), number("3")),
                ("state".into(), AttributeValue::S("OPEN".into())),
            ])
        );
        assert!(parse_projection(
            "#state, #state",
            &BTreeMap::from([("#state".into(), "state".into())])
        )
        .is_err());
        assert!(parse_projection("#state", &names).is_err());
        assert!(parse_projection("state", &BTreeMap::new()).is_err());
    }

    #[test]
    fn key_condition_parser_covers_bounded_sort_forms() {
        let names = BTreeMap::from([
            ("#pk".into(), "account".into()),
            ("#sk".into(), "sequence".into()),
        ]);
        let values = BTreeMap::from([
            (":pk".into(), AttributeValue::S("acct-1".into())),
            (":lower".into(), number("2")),
            (":upper".into(), number("10")),
        ]);
        assert_eq!(
            parse_key_condition(
                "#pk = :pk AND #sk BETWEEN :lower AND :upper",
                &names,
                &values,
            )
            .unwrap(),
            KeyCondition {
                partition_name: "account".into(),
                partition_value: AttributeValue::S("acct-1".into()),
                sort: Some((
                    "sequence".into(),
                    SortKeyCondition::Between(number("2"), number("10")),
                )),
            }
        );
        let begins = parse_key_condition(
            "#pk=:pk AND begins_with(#sk,:prefix)",
            &names,
            &BTreeMap::from([
                (":pk".into(), AttributeValue::S("acct-1".into())),
                (":prefix".into(), AttributeValue::S("2026-".into())),
            ]),
        )
        .unwrap();
        assert!(matches!(
            begins.sort,
            Some((_, SortKeyCondition::BeginsWith(_)))
        ));
        assert!(parse_key_condition("#pk=:pk OR #sk=:lower", &names, &values).is_err());
    }

    #[test]
    fn read_expressions_audit_one_shared_binding_namespace() {
        let names = BTreeMap::from([
            ("#pk".into(), "account".into()),
            ("#state".into(), "state".into()),
            ("#count".into(), "count".into()),
        ]);
        let values = BTreeMap::from([
            (":pk".into(), AttributeValue::S("acct-1".into())),
            (":open".into(), AttributeValue::S("OPEN".into())),
        ]);
        let parsed = parse_read_expressions(
            Some("#pk=:pk"),
            Some("#state=:open"),
            Some("#state,#count"),
            &names,
            &values,
        )
        .unwrap();
        assert!(parsed.key_condition.is_some());
        assert!(parsed.filter.is_some());
        assert_eq!(
            parsed.projection.unwrap().attributes(),
            &[
                AttributePath::top_level("state"),
                AttributePath::top_level("count"),
            ]
        );
        let mut unused = names;
        unused.insert("#unused".into(), "unused".into());
        assert!(parse_read_expressions(
            Some("#pk=:pk"),
            Some("#state=:open"),
            Some("#state,#count"),
            &unused,
            &values,
        )
        .is_err());
    }

    #[test]
    fn condition_parser_and_evaluator_use_exact_types_and_precedence() {
        let names = BTreeMap::from([
            ("#n".into(), "number".into()),
            ("#s".into(), "text".into()),
            ("#tags".into(), "tags".into()),
        ]);
        let values = BTreeMap::from([
            (":low".into(), number("2")),
            (":high".into(), number("10")),
            (":prefix".into(), AttributeValue::S("ab".into())),
            (":tag".into(), AttributeValue::S("legal".into())),
            (":type".into(), AttributeValue::S("N".into())),
            (":len".into(), number("3")),
        ]);
        let condition = parse_condition(
            "#n > :low AND #n <= :high AND begins_with(#s,:prefix) AND contains(#tags,:tag) AND attribute_type(#n,:type) AND size(#s)=:len",
            &names,
            &values,
        )
        .unwrap();
        let item = Item::from([
            ("number".into(), number("10")),
            ("text".into(), AttributeValue::S("abc".into())),
            (
                "tags".into(),
                AttributeValue::Ss(vec!["finance".into(), "legal".into()]),
            ),
        ]);
        assert!(condition.evaluate(Some(&item)).unwrap());

        let precedence = parse_condition(
            "NOT #n = :high OR #s = :prefix AND #n = :low",
            &BTreeMap::from([("#n".into(), "number".into()), ("#s".into(), "text".into())]),
            &BTreeMap::from([
                (":high".into(), number("10")),
                (":prefix".into(), AttributeValue::S("ab".into())),
                (":low".into(), number("2")),
            ]),
        )
        .unwrap();
        assert!(!precedence.evaluate(Some(&item)).unwrap());
    }

    #[test]
    fn between_in_and_type_mismatch_follow_typed_comparison_rules() {
        let item = Item::from([("n".into(), number("10"))]);
        let between = parse_condition(
            "#n BETWEEN :low AND :high",
            &BTreeMap::from([("#n".into(), "n".into())]),
            &BTreeMap::from([(":low".into(), number("2")), (":high".into(), number("10"))]),
        )
        .unwrap();
        assert!(between.evaluate(Some(&item)).unwrap());
        let in_condition = parse_condition(
            "#n IN (:one,:ten,:twenty)",
            &BTreeMap::from([("#n".into(), "n".into())]),
            &BTreeMap::from([
                (":one".into(), number("1")),
                (":ten".into(), number("10")),
                (":twenty".into(), number("20")),
            ]),
        )
        .unwrap();
        assert!(in_condition.evaluate(Some(&item)).unwrap());
        let mismatch = Condition::Comparison {
            name: "n".into(),
            operator: ComparisonOperator::GreaterThan,
            value: AttributeValue::S("2".into()),
        };
        assert!(!mismatch.evaluate(Some(&item)).unwrap());
    }

    #[test]
    fn nested_update_paths_mutate_maps_and_lists_without_stale_operands() {
        let names = BTreeMap::from([
            ("#profile".into(), "profile".into()),
            ("#balance".into(), "balance".into()),
            ("#obsolete".into(), "obsolete".into()),
            ("#lines".into(), "lines".into()),
            ("#state".into(), "state".into()),
        ]);
        let values = BTreeMap::from([
            (":delta".into(), number("0.01")),
            (":closed".into(), AttributeValue::S("CLOSED".into())),
        ]);
        let plan = parse_update(
            "SET #profile.#balance=#profile.#balance+:delta, #lines[1].#state=:closed REMOVE #profile.#obsolete",
            None,
            &names,
            &values,
        )
        .unwrap()
        .plan;
        let old = Item::from([
            (
                "profile".into(),
                AttributeValue::M(Item::from([
                    ("balance".into(), number("99999999999999999999.99")),
                    ("obsolete".into(), AttributeValue::Bool(true)),
                ])),
            ),
            (
                "lines".into(),
                AttributeValue::L(vec![
                    AttributeValue::M(Item::from([(
                        "state".into(),
                        AttributeValue::S("OPEN".into()),
                    )])),
                    AttributeValue::M(Item::from([(
                        "state".into(),
                        AttributeValue::S("OPEN".into()),
                    )])),
                ]),
            ),
        ]);
        let updated = plan.apply(&old, std::iter::empty()).unwrap();
        let AttributeValue::M(profile) = &updated["profile"] else {
            panic!("profile must remain a map")
        };
        assert_eq!(profile["balance"], number("100000000000000000000"));
        assert!(!profile.contains_key("obsolete"));
        let AttributeValue::L(lines) = &updated["lines"] else {
            panic!("lines must remain a list")
        };
        let AttributeValue::M(second) = &lines[1] else {
            panic!("line must remain a map")
        };
        assert_eq!(second["state"], AttributeValue::S("CLOSED".into()));

        let projected = plan.project_targets(&updated);
        let AttributeValue::M(projected_profile) = &projected["profile"] else {
            panic!("projected profile must be a map")
        };
        assert_eq!(projected_profile.len(), 1);
        assert!(projected_profile.contains_key("balance"));
        assert!(matches!(&projected["lines"], AttributeValue::L(values) if values.len() == 1));
    }

    #[test]
    fn nested_update_rejects_overlap_and_invalid_parents() {
        let names = BTreeMap::from([
            ("#profile".into(), "profile".into()),
            ("#balance".into(), "balance".into()),
        ]);
        let values = BTreeMap::from([(":value".into(), AttributeValue::S("value".into()))]);
        assert!(parse_update(
            "SET #profile=:value, #profile.#balance=:value",
            None,
            &names,
            &values,
        )
        .is_err());
        let plan = parse_update("SET #profile.#balance=:value", None, &names, &values)
            .unwrap()
            .plan;
        assert!(plan.apply(&Item::new(), std::iter::empty()).is_err());
    }

    #[test]
    fn nested_conditions_and_projections_resolve_document_paths() {
        let item = Item::from([
            (
                "profile".into(),
                AttributeValue::M(Item::from([
                    ("name".into(), AttributeValue::S("Ada".into())),
                    ("balance".into(), number("10")),
                ])),
            ),
            (
                "lines".into(),
                AttributeValue::L(vec![
                    AttributeValue::M(Item::from([(
                        "state".into(),
                        AttributeValue::S("OLD".into()),
                    )])),
                    AttributeValue::M(Item::from([(
                        "state".into(),
                        AttributeValue::S("OPEN".into()),
                    )])),
                ]),
            ),
        ]);
        let condition = parse_condition(
            "#profile.#balance >= :minimum AND attribute_exists(#lines[1].#state)",
            &BTreeMap::from([
                ("#profile".into(), "profile".into()),
                ("#balance".into(), "balance".into()),
                ("#lines".into(), "lines".into()),
                ("#state".into(), "state".into()),
            ]),
            &BTreeMap::from([(":minimum".into(), number("9.999999999999999999999"))]),
        )
        .unwrap();
        assert!(condition.evaluate(Some(&item)).unwrap());

        let projection = parse_projection(
            "#profile.#name,#lines[1].#state",
            &BTreeMap::from([
                ("#profile".into(), "profile".into()),
                ("#name".into(), "name".into()),
                ("#lines".into(), "lines".into()),
                ("#state".into(), "state".into()),
            ]),
        )
        .unwrap();
        let projected = projection.apply(&item);
        assert_eq!(
            projected["profile"],
            AttributeValue::M(Item::from([(
                "name".into(),
                AttributeValue::S("Ada".into()),
            )]))
        );
        assert!(matches!(
            &projected["lines"],
            AttributeValue::L(lines)
                if lines == &vec![AttributeValue::M(Item::from([(
                    "state".into(),
                    AttributeValue::S("OPEN".into()),
                )]))]
        ));
    }

    #[test]
    fn list_set_and_remove_actions_use_dynamodb_index_ordering() {
        let names = BTreeMap::from([("#list".into(), "list".into())]);
        let remove = parse_update("REMOVE #list[1], #list[2]", None, &names, &BTreeMap::new())
            .unwrap()
            .plan;
        let old = Item::from([(
            "list".into(),
            AttributeValue::L(
                ["a", "b", "c", "d"]
                    .into_iter()
                    .map(|value| AttributeValue::S(value.into()))
                    .collect(),
            ),
        )]);
        assert_eq!(
            remove.apply(&old, std::iter::empty()).unwrap()["list"],
            AttributeValue::L(vec![
                AttributeValue::S("a".into()),
                AttributeValue::S("d".into()),
            ])
        );

        let set = parse_update(
            "SET #list[3]=:d, #list[1]=:b, #list[2]=:c",
            None,
            &names,
            &BTreeMap::from([
                (":b".into(), AttributeValue::S("b".into())),
                (":c".into(), AttributeValue::S("c".into())),
                (":d".into(), AttributeValue::S("d".into())),
            ]),
        )
        .unwrap()
        .plan;
        let one = Item::from([(
            "list".into(),
            AttributeValue::L(vec![AttributeValue::S("a".into())]),
        )]);
        assert_eq!(
            set.apply(&one, std::iter::empty()).unwrap()["list"],
            old["list"]
        );
    }

    #[test]
    fn set_supports_path_copy_list_append_and_both_arithmetic_operand_orders() {
        let plan = parse_update(
            "SET #copy=#profile.#name, #list=list_append(:prefix,#list), #total=:one + #count, #remaining=#count - :one",
            None,
            &BTreeMap::from([
                ("#copy".into(), "copy".into()),
                ("#profile".into(), "profile".into()),
                ("#name".into(), "name".into()),
                ("#list".into(), "list".into()),
                ("#total".into(), "total".into()),
                ("#remaining".into(), "remaining".into()),
                ("#count".into(), "count".into()),
            ]),
            &BTreeMap::from([
                (
                    ":prefix".into(),
                    AttributeValue::L(vec![AttributeValue::S("z".into())]),
                ),
                (":one".into(), number("1")),
            ]),
        )
        .unwrap()
        .plan;
        let old = Item::from([
            (
                "profile".into(),
                AttributeValue::M(Item::from([(
                    "name".into(),
                    AttributeValue::S("Ada".into()),
                )])),
            ),
            (
                "list".into(),
                AttributeValue::L(vec![AttributeValue::S("a".into())]),
            ),
            ("count".into(), number("2")),
        ]);
        let updated = plan.apply(&old, std::iter::empty()).unwrap();
        assert_eq!(updated["copy"], AttributeValue::S("Ada".into()));
        assert_eq!(
            updated["list"],
            AttributeValue::L(vec![
                AttributeValue::S("z".into()),
                AttributeValue::S("a".into()),
            ])
        );
        assert_eq!(updated["total"], number("3"));
        assert_eq!(updated["remaining"], number("1"));
    }
}
