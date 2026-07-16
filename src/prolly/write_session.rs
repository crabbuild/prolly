//! Bounded read-through mutation sessions.

use std::collections::BTreeMap;
use std::ops::{Bound, ControlFlow};

use super::error::{Error, Mutation};
use super::read::{EntryRef, ScanOutcome};
use super::store::Store;
use super::{KeyValue, Prolly, Tree};

/// Pending value in a write-session overlay.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PendingValue {
    Value(Vec<u8>),
    Deleted,
}

/// An overlay checkpoint valid until the next successful flush.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Savepoint {
    generation: u64,
    journal_len: usize,
}

/// Bounded mutable view over an immutable base tree.
pub struct WriteSession<'a, S: Store> {
    manager: &'a Prolly<S>,
    base: Tree,
    overlay: BTreeMap<Vec<u8>, PendingValue>,
    max_bytes: usize,
    current_bytes: usize,
    generation: u64,
    journal: Vec<(Vec<u8>, Option<PendingValue>)>,
}

impl<'a, S: Store> WriteSession<'a, S> {
    pub fn new(manager: &'a Prolly<S>, base: Tree, max_bytes: usize) -> Self {
        Self {
            manager,
            base,
            overlay: BTreeMap::new(),
            max_bytes,
            current_bytes: 0,
            generation: 0,
            journal: Vec::new(),
        }
    }

    pub fn base(&self) -> &Tree {
        &self.base
    }

    pub fn pending_bytes(&self) -> usize {
        self.current_bytes
    }

    pub fn is_empty(&self) -> bool {
        self.overlay.is_empty()
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), Error> {
        self.stage(key, PendingValue::Value(value))
    }

    pub fn delete(&mut self, key: Vec<u8>) -> Result<(), Error> {
        self.stage(key, PendingValue::Deleted)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        self.get_with(key, <[u8]>::to_vec)
    }

    /// Read through the overlay without cloning a staged or base value.
    pub fn get_with<R>(
        &self,
        key: &[u8],
        read: impl FnOnce(&[u8]) -> R,
    ) -> Result<Option<R>, Error> {
        match self.overlay.get(key) {
            Some(PendingValue::Value(value)) => Ok(Some(read(value))),
            Some(PendingValue::Deleted) => Ok(None),
            None => self.manager.get_with(&self.base, key, read),
        }
    }

    /// Materialize one bounded merged range from the base cursor and overlay.
    pub fn range(&self, start: &[u8], end: Option<&[u8]>) -> Result<Vec<KeyValue>, Error> {
        let mut entries = Vec::new();
        self.scan_range(start, end, |entry| entries.push(entry.to_owned()))?;
        Ok(entries)
    }

    /// Visit the ordered overlay/base merge without materializing a map.
    pub fn scan_range(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        mut visit: impl for<'entry> FnMut(EntryRef<'entry>),
    ) -> Result<u64, Error> {
        Ok(self
            .scan_range_until(start, end, |entry| {
                visit(entry);
                ControlFlow::<()>::Continue(())
            })?
            .visited)
    }

    /// Visit the ordered overlay/base merge with early termination.
    pub fn scan_range_until<B>(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        mut visit: impl for<'entry> FnMut(EntryRef<'entry>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        let mut overlay = self
            .overlay
            .range::<[u8], _>((Bound::Included(start), Bound::Unbounded))
            .take_while(|(key, _)| end.map_or(true, |end| key.as_slice() < end))
            .peekable();
        let mut visited = 0u64;
        let mut stopped = None;

        let base_outcome = self
            .manager
            .scan_range_until(&self.base, start, end, |base_entry| {
                while overlay
                    .peek()
                    .is_some_and(|(key, _)| key.as_slice() < base_entry.key())
                {
                    let (key, pending) = overlay.next().expect("peeked overlay entry");
                    if let PendingValue::Value(value) = pending {
                        visited = visited.saturating_add(1);
                        if let ControlFlow::Break(value) =
                            visit(EntryRef::new(key.as_slice(), value.as_slice()))
                        {
                            stopped = Some(value);
                            return ControlFlow::Break(());
                        }
                    }
                }

                if overlay
                    .peek()
                    .is_some_and(|(key, _)| key.as_slice() == base_entry.key())
                {
                    let (key, pending) = overlay.next().expect("peeked overlay entry");
                    if let PendingValue::Value(value) = pending {
                        visited = visited.saturating_add(1);
                        if let ControlFlow::Break(value) =
                            visit(EntryRef::new(key.as_slice(), value.as_slice()))
                        {
                            stopped = Some(value);
                            return ControlFlow::Break(());
                        }
                    }
                    return ControlFlow::Continue(());
                }

                visited = visited.saturating_add(1);
                if let ControlFlow::Break(value) = visit(base_entry) {
                    stopped = Some(value);
                    ControlFlow::Break(())
                } else {
                    ControlFlow::Continue(())
                }
            })?;

        if base_outcome.break_value.is_some() {
            return Ok(ScanOutcome {
                visited,
                break_value: stopped,
            });
        }
        for (key, pending) in overlay {
            if let PendingValue::Value(value) = pending {
                visited = visited.saturating_add(1);
                if let ControlFlow::Break(value) =
                    visit(EntryRef::new(key.as_slice(), value.as_slice()))
                {
                    return Ok(ScanOutcome::stopped(visited, value));
                }
            }
        }
        Ok(ScanOutcome::complete(visited))
    }

    pub fn savepoint(&self) -> Savepoint {
        Savepoint {
            generation: self.generation,
            journal_len: self.journal.len(),
        }
    }

    pub fn revert(&mut self, savepoint: Savepoint) -> Result<(), Error> {
        if savepoint.generation != self.generation || savepoint.journal_len > self.journal.len() {
            return Err(Error::InvalidSavepoint);
        }
        while self.journal.len() > savepoint.journal_len {
            let (key, previous) = self.journal.pop().ok_or(Error::InvalidSavepoint)?;
            match previous {
                Some(value) => {
                    self.overlay.insert(key, value);
                }
                None => {
                    self.overlay.remove(&key);
                }
            }
        }
        self.current_bytes = overlay_bytes(&self.overlay);
        Ok(())
    }

    /// Flush through the canonical writer. State changes only after success.
    pub fn flush(&mut self) -> Result<Tree, Error> {
        let mutations = self
            .overlay
            .iter()
            .map(|(key, pending)| match pending {
                PendingValue::Value(value) => Mutation::Upsert {
                    key: key.clone(),
                    val: value.clone(),
                },
                PendingValue::Deleted => Mutation::Delete { key: key.clone() },
            })
            .collect();
        let tree = self.manager.batch(&self.base, mutations)?;
        self.base = tree.clone();
        self.overlay.clear();
        self.journal.clear();
        self.current_bytes = 0;
        self.generation = self.generation.wrapping_add(1);
        Ok(tree)
    }

    fn stage(&mut self, key: Vec<u8>, pending: PendingValue) -> Result<(), Error> {
        let previous = self.overlay.get(&key).cloned();
        let prior_bytes = previous
            .as_ref()
            .map(|value| entry_bytes(&key, value))
            .unwrap_or(0);
        let next_bytes = entry_bytes(&key, &pending);
        let total = self
            .current_bytes
            .saturating_sub(prior_bytes)
            .saturating_add(next_bytes);
        if total > self.max_bytes {
            return Err(Error::BufferFull);
        }
        self.journal.push((key.clone(), previous));
        self.overlay.insert(key, pending);
        self.current_bytes = total;
        Ok(())
    }
}

fn entry_bytes(key: &[u8], value: &PendingValue) -> usize {
    key.len()
        + match value {
            PendingValue::Value(value) => value.len(),
            PendingValue::Deleted => 0,
        }
}

fn overlay_bytes(overlay: &BTreeMap<Vec<u8>, PendingValue>) -> usize {
    overlay
        .iter()
        .map(|(key, value)| entry_bytes(key, value))
        .sum()
}
