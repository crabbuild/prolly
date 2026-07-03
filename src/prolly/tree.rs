//! Tree structure for Prolly Trees

use serde::{Deserialize, Serialize};

use super::cid::Cid;
use super::config::Config;

/// A Prolly Tree handle
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Tree {
    /// Root node CID (None if empty)
    pub root: Option<Cid>,
    /// Tree configuration
    pub config: Config,
}

impl Tree {
    /// Create a new empty tree with the given configuration
    pub fn new(config: Config) -> Self {
        Self { root: None, config }
    }

    /// Check if the tree is empty
    pub fn is_empty(&self) -> bool {
        self.root.is_none()
    }
}

impl Default for Tree {
    fn default() -> Self {
        Self::new(Config::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tree_new() {
        let tree = Tree::new(Config::default());
        assert!(tree.is_empty());
        assert!(tree.root.is_none());
    }

    #[test]
    fn test_tree_default() {
        let tree = Tree::default();
        assert!(tree.is_empty());
    }

    #[test]
    fn test_tree_with_root() {
        let cid = Cid::from_bytes(b"test");
        let tree = Tree {
            root: Some(cid.clone()),
            config: Config::default(),
        };
        assert!(!tree.is_empty());
        assert_eq!(tree.root, Some(cid));
    }
}
