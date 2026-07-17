use std::any::Any;
use std::sync::{Arc, Mutex};

use super::error::{BindingError, ErrorCode};

const SLOT_BITS: u32 = 32;
const GENERATION_BITS: u32 = 24;
const GENERATION_SHIFT: u32 = SLOT_BITS;
const KIND_SHIFT: u32 = SLOT_BITS + GENERATION_BITS;
const MAX_GENERATION: u32 = (1 << GENERATION_BITS) - 1;

type Resource = Arc<dyn Any + Send + Sync>;

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HandleKind {
    Engine = 1,
    Store = 2,
    ReadSession = 3,
    WriteSession = 4,
    Cursor = 5,
    Page = 6,
    Value = 7,
    IndexedSnapshot = 8,
    ProximitySession = 9,
    Accelerator = 10,
}

impl TryFrom<u8> for HandleKind {
    type Error = BindingError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Engine),
            2 => Ok(Self::Store),
            3 => Ok(Self::ReadSession),
            4 => Ok(Self::WriteSession),
            5 => Ok(Self::Cursor),
            6 => Ok(Self::Page),
            7 => Ok(Self::Value),
            8 => Ok(Self::IndexedSnapshot),
            9 => Ok(Self::ProximitySession),
            10 => Ok(Self::Accelerator),
            _ => Err(BindingError::invalid_handle(
                "handle has an unknown type tag",
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ResourceHandle(u64);

impl ResourceHandle {
    fn new(kind: HandleKind, generation: u32, slot: u32) -> Self {
        debug_assert!((1..=MAX_GENERATION).contains(&generation));
        let encoded_slot = u64::from(slot) + 1;
        Self(
            (u64::from(kind as u8) << KIND_SHIFT)
                | (u64::from(generation) << GENERATION_SHIFT)
                | encoded_slot,
        )
    }

    pub(crate) fn from_raw(raw: u64) -> Result<Self, BindingError> {
        let handle = Self(raw);
        handle.parts()?;
        Ok(handle)
    }

    pub(crate) fn raw(self) -> u64 {
        self.0
    }

    #[allow(dead_code)]
    pub(crate) fn slot(self) -> u32 {
        self.parts().expect("validated resource handle").2
    }

    #[allow(dead_code)]
    pub(crate) fn generation(self) -> u32 {
        self.parts().expect("validated resource handle").1
    }

    pub(crate) fn kind(self) -> HandleKind {
        self.parts().expect("validated resource handle").0
    }

    fn parts(self) -> Result<(HandleKind, u32, u32), BindingError> {
        let encoded_slot = (self.0 & u64::from(u32::MAX)) as u32;
        let generation = ((self.0 >> GENERATION_SHIFT) & u64::from(MAX_GENERATION)) as u32;
        let kind = HandleKind::try_from((self.0 >> KIND_SHIFT) as u8)?;
        if encoded_slot == 0 || generation == 0 {
            return Err(BindingError::invalid_handle(
                "handle has a zero slot or generation",
            ));
        }
        Ok((kind, generation, encoded_slot - 1))
    }
}

#[derive(Default)]
struct Slot {
    generation: u32,
    kind: Option<HandleKind>,
    resource: Option<Resource>,
}

#[derive(Default)]
struct RegistryState {
    slots: Vec<Slot>,
    free: Vec<u32>,
    active: usize,
}

#[derive(Default)]
pub(crate) struct HandleRegistry {
    state: Mutex<RegistryState>,
}

impl HandleRegistry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn insert<T>(&self, kind: HandleKind, resource: T) -> ResourceHandle
    where
        T: Any + Send + Sync + 'static,
    {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let resource: Resource = Arc::new(resource);
        let slot_index = if let Some(slot) = state.free.pop() {
            slot
        } else {
            state.slots.push(Slot::default());
            u32::try_from(state.slots.len() - 1).expect("handle registry exceeds u32 slots")
        };
        let slot = &mut state.slots[slot_index as usize];
        slot.generation = next_generation(slot.generation);
        slot.kind = Some(kind);
        slot.resource = Some(resource);
        let handle = ResourceHandle::new(kind, slot.generation, slot_index);
        state.active += 1;
        handle
    }

    #[allow(dead_code)]
    pub(crate) fn get<T>(&self, handle: ResourceHandle) -> Result<Arc<T>, BindingError>
    where
        T: Any + Send + Sync + 'static,
    {
        let (kind, generation, slot_index) = handle.parts()?;
        let resource = {
            let state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let slot = state
                .slots
                .get(slot_index as usize)
                .ok_or_else(|| BindingError::invalid_handle("handle slot is out of range"))?;
            validate_slot(slot, kind, generation)?;
            slot.resource
                .as_ref()
                .cloned()
                .ok_or_else(|| BindingError::new(ErrorCode::Closed, "handle is already closed"))?
        };
        Arc::downcast::<T>(resource).map_err(|_| {
            BindingError::invalid_handle("handle resource type does not match requested type")
        })
    }

    pub(crate) fn close(&self, handle: ResourceHandle) -> Result<(), BindingError> {
        let (kind, generation, slot_index) = handle.parts()?;
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let slot = state
            .slots
            .get_mut(slot_index as usize)
            .ok_or_else(|| BindingError::invalid_handle("handle slot is out of range"))?;
        validate_slot(slot, kind, generation)?;
        if slot.resource.take().is_some() {
            state.active -= 1;
            state.free.push(slot_index);
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn active_count(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .active
    }
}

fn validate_slot(slot: &Slot, kind: HandleKind, generation: u32) -> Result<(), BindingError> {
    if slot.generation != generation || slot.kind != Some(kind) {
        return Err(BindingError::invalid_handle(
            "handle generation or type tag is stale",
        ));
    }
    Ok(())
}

fn next_generation(current: u32) -> u32 {
    if current == MAX_GENERATION {
        1
    } else {
        current + 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stale_generation_is_rejected_after_slot_reuse() {
        let registry = HandleRegistry::new();
        let first = registry.insert(HandleKind::ReadSession, String::from("one"));
        registry.close(first).unwrap();
        let second = registry.insert(HandleKind::ReadSession, String::from("two"));

        assert_eq!(first.slot(), second.slot());
        assert_ne!(first.generation(), second.generation());
        assert_eq!(
            registry.get::<String>(first).unwrap_err().code,
            ErrorCode::InvalidHandle,
        );
        assert_eq!(registry.get::<String>(second).unwrap().as_str(), "two");
        let decoded = ResourceHandle::from_raw(second.raw()).unwrap();
        assert_eq!(decoded, second);
        assert_eq!(decoded.kind(), HandleKind::ReadSession);
    }

    #[test]
    fn close_is_idempotent_until_the_slot_is_reused() {
        let registry = HandleRegistry::new();
        let handle = registry.insert(HandleKind::Page, vec![1_u8, 2, 3]);

        registry.close(handle).unwrap();
        registry.close(handle).unwrap();
        assert_eq!(registry.active_count(), 0);
    }
}
