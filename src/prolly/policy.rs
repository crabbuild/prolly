//! Merge policy registry helpers.
//!
//! The standard merge API accepts a single resolver closure. This module lets
//! applications compose domain-specific resolver policies by key prefix, exact
//! key, or custom key matcher, then pass the registry back to `merge`.

use std::fmt;
use std::sync::Arc;

use super::error::{Conflict, Resolution, Resolver};

/// Shared merge policy function used by [`MergePolicyRegistry`].
pub type MergePolicyFn = Arc<dyn Fn(&Conflict) -> Resolution + Send + Sync + 'static>;

type KeyMatcherFn = Arc<dyn Fn(&[u8]) -> bool + Send + Sync + 'static>;

/// Domain-level conflict resolver selected by key.
#[derive(Clone)]
pub struct MergePolicyRegistry {
    rules: Vec<MergePolicyRule>,
    default: Option<MergePolicyFn>,
}

impl MergePolicyRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            rules: Vec::new(),
            default: None,
        }
    }

    /// Create a registry with a default policy for unmatched keys.
    pub fn with_default<F>(policy: F) -> Self
    where
        F: Fn(&Conflict) -> Resolution + Send + Sync + 'static,
    {
        Self::new().default_policy(policy)
    }

    /// Set the default policy for unmatched keys.
    pub fn set_default<F>(&mut self, policy: F) -> &mut Self
    where
        F: Fn(&Conflict) -> Resolution + Send + Sync + 'static,
    {
        self.default = Some(Arc::new(policy));
        self
    }

    /// Builder-style default policy registration.
    pub fn default_policy<F>(mut self, policy: F) -> Self
    where
        F: Fn(&Conflict) -> Resolution + Send + Sync + 'static,
    {
        self.set_default(policy);
        self
    }

    /// Add a policy for keys starting with `prefix`.
    ///
    /// Rules are evaluated from newest to oldest, so later rules override
    /// earlier broader rules.
    pub fn push_prefix<F>(&mut self, prefix: impl Into<Vec<u8>>, policy: F) -> &mut Self
    where
        F: Fn(&Conflict) -> Resolution + Send + Sync + 'static,
    {
        self.rules
            .push(MergePolicyRule::prefix(prefix.into(), Arc::new(policy)));
        self
    }

    /// Builder-style prefix policy registration.
    pub fn add_prefix<F>(mut self, prefix: impl Into<Vec<u8>>, policy: F) -> Self
    where
        F: Fn(&Conflict) -> Resolution + Send + Sync + 'static,
    {
        self.push_prefix(prefix, policy);
        self
    }

    /// Add a policy for one exact key.
    ///
    /// Rules are evaluated from newest to oldest, so later rules override
    /// earlier broader rules.
    pub fn push_exact<F>(&mut self, key: impl Into<Vec<u8>>, policy: F) -> &mut Self
    where
        F: Fn(&Conflict) -> Resolution + Send + Sync + 'static,
    {
        self.rules
            .push(MergePolicyRule::exact(key.into(), Arc::new(policy)));
        self
    }

    /// Builder-style exact-key policy registration.
    pub fn add_exact<F>(mut self, key: impl Into<Vec<u8>>, policy: F) -> Self
    where
        F: Fn(&Conflict) -> Resolution + Send + Sync + 'static,
    {
        self.push_exact(key, policy);
        self
    }

    /// Add a policy selected by a custom key matcher.
    ///
    /// `name` is used only for diagnostics and debugging.
    pub fn push_pattern<M, F>(
        &mut self,
        name: impl Into<String>,
        matcher: M,
        policy: F,
    ) -> &mut Self
    where
        M: Fn(&[u8]) -> bool + Send + Sync + 'static,
        F: Fn(&Conflict) -> Resolution + Send + Sync + 'static,
    {
        self.rules.push(MergePolicyRule::pattern(
            name.into(),
            Arc::new(matcher),
            Arc::new(policy),
        ));
        self
    }

    /// Builder-style custom key matcher policy registration.
    pub fn add_pattern<M, F>(mut self, name: impl Into<String>, matcher: M, policy: F) -> Self
    where
        M: Fn(&[u8]) -> bool + Send + Sync + 'static,
        F: Fn(&Conflict) -> Resolution + Send + Sync + 'static,
    {
        self.push_pattern(name, matcher, policy);
        self
    }

    /// Number of registered key-specific rules.
    pub fn len(&self) -> usize {
        self.rules.len()
    }

    /// Whether no key-specific rules are registered.
    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    /// Whether a default policy is configured for unmatched keys.
    pub fn has_default(&self) -> bool {
        self.default.is_some()
    }

    /// Borrow registered rules in insertion order.
    pub fn rules(&self) -> &[MergePolicyRule] {
        &self.rules
    }

    /// Return the newest matching key-specific rule, if any.
    pub fn matching_rule(&self, key: &[u8]) -> Option<&MergePolicyRule> {
        self.rules.iter().rev().find(|rule| rule.matches(key))
    }

    /// Resolve one conflict using the newest matching rule or the default.
    ///
    /// If no rule matches and no default policy exists, the conflict is left
    /// unresolved.
    pub fn resolve(&self, conflict: &Conflict) -> Resolution {
        if let Some(rule) = self.matching_rule(&conflict.key) {
            return (rule.policy.as_ref())(conflict);
        }

        self.default
            .as_ref()
            .map(|policy| (policy.as_ref())(conflict))
            .unwrap_or_else(Resolution::unresolved)
    }

    /// Convert this registry into a standard merge resolver.
    pub fn into_resolver(self) -> Resolver {
        Box::new(move |conflict| self.resolve(conflict))
    }

    /// Clone this registry into a standard merge resolver.
    pub fn as_resolver(&self) -> Resolver {
        self.clone().into_resolver()
    }
}

impl Default for MergePolicyRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for MergePolicyRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MergePolicyRegistry")
            .field("rules", &self.rules)
            .field("has_default", &self.has_default())
            .finish()
    }
}

/// One registered merge policy rule.
#[derive(Clone)]
pub struct MergePolicyRule {
    matcher: MergePolicyMatcher,
    policy: MergePolicyFn,
}

impl MergePolicyRule {
    fn prefix(prefix: Vec<u8>, policy: MergePolicyFn) -> Self {
        Self {
            matcher: MergePolicyMatcher::Prefix(prefix),
            policy,
        }
    }

    fn exact(key: Vec<u8>, policy: MergePolicyFn) -> Self {
        Self {
            matcher: MergePolicyMatcher::Exact(key),
            policy,
        }
    }

    fn pattern(name: String, matcher: KeyMatcherFn, policy: MergePolicyFn) -> Self {
        Self {
            matcher: MergePolicyMatcher::Pattern { name, matcher },
            policy,
        }
    }

    /// Return true when this rule applies to `key`.
    pub fn matches(&self, key: &[u8]) -> bool {
        match &self.matcher {
            MergePolicyMatcher::Prefix(prefix) => key.starts_with(prefix),
            MergePolicyMatcher::Exact(exact) => key == exact,
            MergePolicyMatcher::Pattern { matcher, .. } => (matcher.as_ref())(key),
        }
    }

    /// Return a human-oriented rule label for debugging.
    pub fn label(&self) -> MergePolicyRuleLabel<'_> {
        match &self.matcher {
            MergePolicyMatcher::Prefix(prefix) => MergePolicyRuleLabel::Prefix(prefix),
            MergePolicyMatcher::Exact(key) => MergePolicyRuleLabel::Exact(key),
            MergePolicyMatcher::Pattern { name, .. } => MergePolicyRuleLabel::Pattern(name),
        }
    }
}

impl fmt::Debug for MergePolicyRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MergePolicyRule")
            .field("label", &self.label())
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
enum MergePolicyMatcher {
    Prefix(Vec<u8>),
    Exact(Vec<u8>),
    Pattern { name: String, matcher: KeyMatcherFn },
}

/// Human-readable label for a merge policy rule.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MergePolicyRuleLabel<'a> {
    /// Prefix-based rule.
    Prefix(&'a [u8]),
    /// Exact-key rule.
    Exact(&'a [u8]),
    /// Custom matcher rule.
    Pattern(&'a str),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prolly::error::resolver;

    fn conflict(key: &[u8]) -> Conflict {
        Conflict {
            key: key.to_vec(),
            base: Some(b"base".to_vec()),
            left: Some(b"left".to_vec()),
            right: Some(b"right".to_vec()),
        }
    }

    #[test]
    fn newest_matching_rule_wins() {
        let registry = MergePolicyRegistry::new()
            .add_prefix(b"settings/".to_vec(), resolver::prefer_left)
            .add_exact(b"settings/theme".to_vec(), resolver::prefer_right);

        let result = registry.resolve(&conflict(b"settings/theme"));
        assert_eq!(result, Resolution::value(b"right".to_vec()));

        let rule = registry.matching_rule(b"settings/theme").unwrap();
        assert_eq!(rule.label(), MergePolicyRuleLabel::Exact(b"settings/theme"));
    }

    #[test]
    fn pattern_and_default_policies_work() {
        let registry = MergePolicyRegistry::with_default(|_| Resolution::unresolved()).add_pattern(
            "summary",
            |key| key.ends_with(b"/summary"),
            |conflict| {
                let mut value = conflict.left.clone().unwrap_or_default();
                value.push(b'\n');
                value.extend(conflict.right.clone().unwrap_or_default());
                Resolution::value(value)
            },
        );

        assert_eq!(
            registry.resolve(&conflict(b"documents/42/summary")),
            Resolution::value(b"left\nright".to_vec())
        );
        assert_eq!(
            registry.resolve(&conflict(b"documents/42/title")),
            Resolution::unresolved()
        );
    }
}
