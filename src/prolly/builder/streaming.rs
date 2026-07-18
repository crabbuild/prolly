use super::{new_builder_node, EncodedNodeSizer, NodeSummary};
use crate::prolly::boundary::BoundaryDetector;
use crate::prolly::cid::Cid;
use crate::prolly::config::Config;
use crate::prolly::encoding::Encoding;
use crate::prolly::error::Error;
use crate::prolly::format::{ChunkMeasure, NodeLayoutSpec, TreeFormat};
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
    sizer: Option<EncodedNodeSizer>,
    encoded_measure: bool,
    upper_size: u64,
}

impl LevelEmitter {
    pub(crate) fn new(config: Config, leaf: bool, level: u8) -> Result<Self, Error> {
        if leaf != (level == 0) {
            return Err(Error::InvalidNode);
        }
        config.format.validate()?;
        if let NodeLayoutSpec::Custom { id, .. } = &config.format.node_layout {
            return Err(Error::InvalidFormat(format!(
                "node layout '{id}' has no registered codec"
            )));
        }
        let encoded_measure = config.format.chunking.measure == ChunkMeasure::EncodedBytes;
        let sizer = encoded_measure
            .then(|| EncodedNodeSizer::new(config.format.clone(), leaf, level))
            .transpose()?;
        let upper_size = match &sizer {
            Some(sizer) => sizer.size(),
            None => conservative_empty_upper(&config.format)?,
        };
        Ok(Self {
            current: new_builder_node(&config, leaf, level),
            detector: BoundaryDetector::new(config.format.chunking.clone(), u16::from(level))?,
            encoded_measure,
            sizer,
            upper_size,
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
        let mut emitted = Vec::new();
        self.push_leaf_with(key, value, |node| emitted.push(node))?;
        Ok(emitted)
    }

    pub(crate) fn push_leaf_with(
        &mut self,
        key: Vec<u8>,
        value: Vec<u8>,
        emit: impl FnMut(EmittedNode),
    ) -> Result<(), Error> {
        if !self.leaf {
            return Err(Error::InvalidNode);
        }
        self.push_entry_with(key, value, None, emit)
    }

    pub(crate) fn push_child(&mut self, child: NodeSummary) -> Result<Vec<EmittedNode>, Error> {
        let mut emitted = Vec::new();
        self.push_child_with(child, |node| emitted.push(node))?;
        Ok(emitted)
    }

    pub(crate) fn push_child_with(
        &mut self,
        child: NodeSummary,
        emit: impl FnMut(EmittedNode),
    ) -> Result<(), Error> {
        if self.leaf {
            return Err(Error::InvalidNode);
        }
        self.push_entry_with(
            child.first_key,
            child.cid.as_bytes().to_vec(),
            Some(child.count),
            emit,
        )
    }

    fn push_entry_with(
        &mut self,
        key: Vec<u8>,
        value: Vec<u8>,
        child_count: Option<u64>,
        mut emit: impl FnMut(EmittedNode),
    ) -> Result<(), Error> {
        let hard_max = self.config.format.chunking.hard_max_node_bytes;
        let encoded_entry_bytes = if self.encoded_measure {
            let previous_key = self.current.keys.last().map(Vec::as_slice);
            let mut encoded_size = self
                .sizer
                .as_ref()
                .expect("encoded measure has a sizer")
                .size_after(previous_key, &key, &value, child_count)?;
            if !self.current.is_empty() && encoded_size > hard_max {
                emit(self.flush_current()?.expect("nonempty node flushes"));
                self.detector.reset();
                encoded_size = self
                    .sizer
                    .as_ref()
                    .expect("encoded measure has a sizer")
                    .size_after(None, &key, &value, child_count)?;
            }
            if encoded_size > hard_max {
                return Err(Error::EntryTooLarge {
                    encoded_bytes: encoded_size,
                    limit: hard_max,
                });
            }

            let encoded_entry_bytes = encoded_size.saturating_sub(
                self.sizer
                    .as_ref()
                    .expect("encoded measure has a sizer")
                    .size(),
            );
            self.sizer
                .as_mut()
                .expect("encoded measure has a sizer")
                .push_sized(&key, &value, child_count, encoded_size)?;
            self.upper_size = encoded_size;
            encoded_entry_bytes
        } else {
            let entry_upper = conservative_entry_upper(&key, &value, child_count)?;
            let mut encoded_size = self
                .upper_size
                .checked_add(entry_upper)
                .ok_or(Error::InvalidNode)?;
            if encoded_size > hard_max {
                encoded_size = self.exact_size_after(&key, &value, child_count)?;
                if !self.current.is_empty() && encoded_size > hard_max {
                    emit(self.flush_current()?.expect("nonempty node flushes"));
                    self.detector.reset();
                    encoded_size = self.exact_size_after(&key, &value, child_count)?;
                }
                if encoded_size > hard_max {
                    return Err(Error::EntryTooLarge {
                        encoded_bytes: encoded_size,
                        limit: hard_max,
                    });
                }
            }
            self.upper_size = encoded_size;
            0
        };
        let boundary = self
            .detector
            .observe(&key, &value, encoded_entry_bytes as usize)?;
        self.current.keys.push(key);
        self.current.vals.push(value);
        if let Some(count) = child_count {
            self.current.child_counts.push(count);
        }
        if boundary {
            emit(
                self.flush_current()?
                    .expect("observed entry makes node nonempty"),
            );
        }
        Ok(())
    }

    fn exact_size_after(
        &self,
        key: &[u8],
        value: &[u8],
        child_count: Option<u64>,
    ) -> Result<u64, Error> {
        let mut sizer = EncodedNodeSizer::new(self.config.format.clone(), self.leaf, self.level)?;
        for index in 0..self.current.len() {
            let previous_key = index
                .checked_sub(1)
                .map(|previous| self.current.keys[previous].as_slice());
            let buffered_child_count = (!self.leaf).then(|| self.current.child_counts[index]);
            let size = sizer.size_after(
                previous_key,
                &self.current.keys[index],
                &self.current.vals[index],
                buffered_child_count,
            )?;
            sizer.push_sized(
                &self.current.keys[index],
                &self.current.vals[index],
                buffered_child_count,
                size,
            )?;
        }
        sizer.size_after(
            self.current.keys.last().map(Vec::as_slice),
            key,
            value,
            child_count,
        )
    }

    pub(crate) fn finish(&mut self) -> Result<Option<EmittedNode>, Error> {
        self.flush_current()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.current.is_empty()
    }

    fn flush_current(&mut self) -> Result<Option<EmittedNode>, Error> {
        if self.current.is_empty() {
            return Ok(None);
        }
        let node = std::mem::replace(
            &mut self.current,
            new_builder_node(&self.config, self.leaf, self.level),
        );
        let bytes = node.to_bytes();
        let hard_max = self.config.format.chunking.hard_max_node_bytes;
        if bytes.len() as u64 > hard_max {
            return Err(Error::EntryTooLarge {
                encoded_bytes: bytes.len() as u64,
                limit: hard_max,
            });
        }
        let summary = NodeSummary {
            cid: Cid::from_bytes(&bytes),
            first_key: node.keys[0].clone(),
            count: if self.leaf {
                node.keys.len() as u64
            } else {
                node.child_counts.iter().sum()
            },
        };
        if let Some(sizer) = &mut self.sizer {
            sizer.reset();
            self.upper_size = sizer.size();
        } else {
            self.upper_size = conservative_empty_upper(&self.config.format)?;
        }
        Ok(Some(EmittedNode {
            summary,
            node,
            bytes,
        }))
    }

    #[cfg(test)]
    fn buffered_entries(&self) -> usize {
        self.current.len()
    }
}

fn conservative_entry_upper(
    key: &[u8],
    value: &[u8],
    child_count: Option<u64>,
) -> Result<u64, Error> {
    let key_len = u64::try_from(key.len()).map_err(|_| Error::InvalidNode)?;
    let value_len = u64::try_from(value.len()).map_err(|_| Error::InvalidNode)?;
    key_len
        .checked_add(value_len)
        .and_then(|size| size.checked_add(if child_count.is_some() { 74 } else { 64 }))
        .ok_or(Error::InvalidNode)
}

fn conservative_empty_upper(format: &TreeFormat) -> Result<u64, Error> {
    let custom_encoding_len = match &format.value_encoding {
        Encoding::Custom(name) => u64::try_from(name.len()).map_err(|_| Error::InvalidNode)?,
        Encoding::Raw | Encoding::Cbor | Encoding::Json => 0,
    };
    128_u64
        .checked_add(custom_encoding_len)
        .ok_or(Error::InvalidNode)
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
        if let Some(node) = self.leaf.finish()? {
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
                if let Some(node) = emitter.finish()? {
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
