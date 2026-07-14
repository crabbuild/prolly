use super::super::cid::Cid;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

pub(crate) const DEFAULT_PROXIMITY_CACHE_NODES: usize = 8_192;

struct Entry<T> {
    value: Arc<T>,
    bytes: usize,
    generation: u64,
}

/// Bounded, generation-LRU content cache. Separate instantiations keep object
/// types isolated even when their CIDs happen to be equal.
pub(crate) struct ContentCache<T> {
    max_nodes: usize,
    entries: HashMap<Cid, Entry<T>>,
    access_log: VecDeque<(Cid, u64)>,
    generation: u64,
}

impl<T> ContentCache<T> {
    pub(crate) fn new(max_nodes: usize) -> Self {
        Self {
            max_nodes,
            entries: HashMap::new(),
            access_log: VecDeque::new(),
            generation: 0,
        }
    }

    pub(crate) fn get(&mut self, cid: &Cid) -> Option<(Arc<T>, usize)> {
        let entry = self.entries.get_mut(cid)?;
        self.generation = self.generation.wrapping_add(1);
        entry.generation = self.generation;
        self.access_log.push_back((cid.clone(), self.generation));
        Some((entry.value.clone(), entry.bytes))
    }

    pub(crate) fn insert(&mut self, cid: Cid, value: Arc<T>, bytes: usize) {
        if self.max_nodes == 0 {
            return;
        }
        self.generation = self.generation.wrapping_add(1);
        self.entries.insert(
            cid.clone(),
            Entry {
                value,
                bytes,
                generation: self.generation,
            },
        );
        self.access_log.push_back((cid, self.generation));
        while self.entries.len() > self.max_nodes {
            let Some((candidate, generation)) = self.access_log.pop_front() else {
                break;
            };
            if self
                .entries
                .get(&candidate)
                .is_some_and(|entry| entry.generation == generation)
            {
                self.entries.remove(&candidate);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_content_cache_is_bounded_and_refreshes_recent_entries() {
        let mut cache = ContentCache::new(2);
        let one = Cid::from_bytes(b"one");
        let two = Cid::from_bytes(b"two");
        let three = Cid::from_bytes(b"three");
        cache.insert(one.clone(), Arc::new(1u8), 1);
        cache.insert(two.clone(), Arc::new(2u8), 1);
        assert_eq!(*cache.get(&one).unwrap().0, 1);
        cache.insert(three.clone(), Arc::new(3u8), 1);
        assert!(cache.get(&two).is_none());
        assert!(cache.get(&one).is_some());
        assert!(cache.get(&three).is_some());
    }
}
