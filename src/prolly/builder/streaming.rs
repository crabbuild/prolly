use super::{new_builder_node, EncodedNodeSizer, NodeSummary};
use crate::prolly::boundary::BoundaryDetector;
use crate::prolly::cid::Cid;
use crate::prolly::config::Config;
use crate::prolly::error::Error;
use crate::prolly::node::Node;

#[derive(Debug)]
pub(crate) struct EmittedNode {
    pub(crate) summary: NodeSummary,
    pub(crate) node: Node,
    pub(crate) bytes: Vec<u8>,
}

pub(crate) struct LevelEmitter {
    config: Config,
    leaf: bool,
    level: u8,
    current: Node,
    detector: BoundaryDetector,
    sizer: EncodedNodeSizer,
}

impl LevelEmitter {
    pub(crate) fn new(config: Config, leaf: bool, level: u8) -> Result<Self, Error> {
        if leaf != (level == 0) {
            return Err(Error::InvalidNode);
        }
        Ok(Self {
            current: new_builder_node(&config, leaf, level),
            detector: BoundaryDetector::new(config.format.chunking.clone(), u16::from(level))?,
            sizer: EncodedNodeSizer::new(config.format.clone(), leaf, level)?,
            config,
            leaf,
            level,
        })
    }

    pub(crate) fn push_leaf(
        &mut self,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<Vec<EmittedNode>, Error> {
        if !self.leaf {
            return Err(Error::InvalidNode);
        }
        self.push_entry(key, value, None)
    }

    pub(crate) fn push_child(&mut self, child: NodeSummary) -> Result<Vec<EmittedNode>, Error> {
        if self.leaf {
            return Err(Error::InvalidNode);
        }
        self.push_entry(
            child.first_key,
            child.cid.as_bytes().to_vec(),
            Some(child.count),
        )
    }

    fn push_entry(
        &mut self,
        key: Vec<u8>,
        value: Vec<u8>,
        child_count: Option<u64>,
    ) -> Result<Vec<EmittedNode>, Error> {
        let hard_max = self.config.format.chunking.hard_max_node_bytes;
        let mut emitted = Vec::new();
        let mut encoded_size = self.sizer.size_after(&key, &value, child_count)?;
        if !self.current.is_empty() && encoded_size > hard_max {
            emitted.push(self.flush_current().expect("nonempty node flushes"));
            self.detector.reset();
            encoded_size = self.sizer.size_after(&key, &value, child_count)?;
        }
        if encoded_size > hard_max {
            return Err(Error::EntryTooLarge {
                encoded_bytes: encoded_size,
                limit: hard_max,
            });
        }

        let encoded_entry_bytes = encoded_size.saturating_sub(self.sizer.size());
        self.sizer
            .push_sized(&key, &value, child_count, encoded_size)?;
        let boundary = self
            .detector
            .observe(&key, &value, encoded_entry_bytes as usize)?;
        self.current.keys.push(key);
        self.current.vals.push(value);
        if let Some(count) = child_count {
            self.current.child_counts.push(count);
        }
        if boundary {
            emitted.push(
                self.flush_current()
                    .expect("observed entry makes node nonempty"),
            );
        }
        Ok(emitted)
    }

    pub(crate) fn finish(&mut self) -> Option<EmittedNode> {
        self.flush_current()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.current.is_empty()
    }

    fn flush_current(&mut self) -> Option<EmittedNode> {
        if self.current.is_empty() {
            return None;
        }
        let node = std::mem::replace(
            &mut self.current,
            new_builder_node(&self.config, self.leaf, self.level),
        );
        let bytes = node.to_bytes();
        debug_assert!(bytes.len() as u64 <= self.config.format.chunking.hard_max_node_bytes);
        let summary = NodeSummary {
            cid: Cid::from_bytes(&bytes),
            first_key: node.keys[0].clone(),
            count: if self.leaf {
                node.keys.len() as u64
            } else {
                node.child_counts.iter().sum()
            },
        };
        self.sizer.reset();
        Some(EmittedNode {
            summary,
            node,
            bytes,
        })
    }

    #[cfg(test)]
    fn buffered_entries(&self) -> usize {
        self.current.len()
    }
}

struct ParentLevel {
    pending_first: Option<NodeSummary>,
    emitter: Option<LevelEmitter>,
}

impl ParentLevel {
    fn new() -> Self {
        Self {
            pending_first: None,
            emitter: None,
        }
    }
}

pub(crate) struct HierarchicalEmitter {
    config: Config,
    leaf: LevelEmitter,
    parents: Vec<ParentLevel>,
}

impl HierarchicalEmitter {
    pub(crate) fn new(config: Config) -> Result<Self, Error> {
        Ok(Self {
            leaf: LevelEmitter::new(config.clone(), true, 0)?,
            config,
            parents: Vec::new(),
        })
    }

    pub(crate) fn push_leaf(
        &mut self,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<Vec<EmittedNode>, Error> {
        let leaf_nodes = self.leaf.push_leaf(key, value)?;
        let mut emitted = Vec::new();
        for node in leaf_nodes {
            let summary = node.summary.clone();
            emitted.push(node);
            self.cascade(0, summary, &mut emitted)?;
        }
        Ok(emitted)
    }

    pub(crate) fn finish(&mut self) -> Result<(Option<NodeSummary>, Vec<EmittedNode>), Error> {
        let mut emitted = Vec::new();
        if let Some(node) = self.leaf.finish() {
            let summary = node.summary.clone();
            emitted.push(node);
            self.cascade(0, summary, &mut emitted)?;
        }
        if self.parents.is_empty() {
            return Ok((None, emitted));
        }

        let mut index = 0;
        loop {
            if index >= self.parents.len() {
                return Err(Error::InvalidNode);
            }
            if let Some(mut emitter) = self.parents[index].emitter.take() {
                if let Some(node) = emitter.finish() {
                    let summary = node.summary.clone();
                    emitted.push(node);
                    self.cascade((index + 1) as u8, summary, &mut emitted)?;
                }
                index += 1;
                continue;
            }
            if let Some(root) = self.parents[index].pending_first.take() {
                return Ok((Some(root), emitted));
            }
            index += 1;
        }
    }

    fn cascade(
        &mut self,
        child_level: u8,
        child: NodeSummary,
        emitted: &mut Vec<EmittedNode>,
    ) -> Result<(), Error> {
        let mut queue = vec![(child_level, child)];
        while let Some((level, summary)) = queue.pop() {
            let parent_level = level.checked_add(1).ok_or(Error::InvalidNode)?;
            let index = usize::from(parent_level - 1);
            while self.parents.len() <= index {
                self.parents.push(ParentLevel::new());
            }
            let state = &mut self.parents[index];
            if state.emitter.is_none() {
                if state.pending_first.is_none() {
                    state.pending_first = Some(summary);
                    continue;
                }
                let first = state.pending_first.take().expect("pending child exists");
                let mut emitter = LevelEmitter::new(self.config.clone(), false, parent_level)?;
                let first_emissions = emitter.push_child(first)?;
                if !first_emissions.is_empty() {
                    return Err(Error::InvalidNode);
                }
                state.emitter = Some(emitter);
            }

            let nodes = state
                .emitter
                .as_mut()
                .expect("second child creates parent emitter")
                .push_child(summary)?;
            for node in nodes {
                let next = node.summary.clone();
                emitted.push(node);
                queue.push((parent_level, next));
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn active_levels(&self) -> usize {
        1 + self.parents.len()
    }

    #[cfg(test)]
    pub(crate) fn buffered_entries(&self) -> usize {
        self.leaf.buffered_entries()
            + self
                .parents
                .iter()
                .map(|level| {
                    usize::from(level.pending_first.is_some())
                        + level
                            .emitter
                            .as_ref()
                            .map(LevelEmitter::buffered_entries)
                            .unwrap_or(0)
                })
                .sum::<usize>()
    }
}
