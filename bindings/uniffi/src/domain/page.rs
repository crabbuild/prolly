use super::error::{BindingError, ErrorCode};

pub(crate) const PAGE_MAGIC: [u8; 4] = *b"PRPG";
pub(crate) const PAGE_VERSION_V1: u16 = 1;
pub(crate) const PAGE_VERSION: u16 = 2;
pub(crate) const PAGE_HEADER_BYTES: usize = 28;
pub(crate) const PAGE_FLAG_TERMINAL: u32 = 1;

const RECORD_FLAG_VALUE: u32 = 1;
const RECORD_FLAG_PROOF: u32 = 2;
const RECORD_FLAG_PROJECTION: u32 = 1;
const RECORD_FLAG_CURSOR: u32 = 2;

#[repr(u16)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PackedPageKind {
    Entry = 1,
    GetMany = 2,
    Diff = 3,
    Conflict = 4,
    IndexMatch = 5,
    JoinedIndexRecord = 6,
    ProximityNeighbor = 7,
}

impl PackedPageKind {
    fn record_width(self) -> usize {
        match self {
            Self::Entry => 16,
            Self::GetMany => 12,
            Self::Diff => 28,
            Self::Conflict => 36,
            Self::IndexMatch => 36,
            Self::JoinedIndexRecord => 44,
            Self::ProximityNeighbor => 40,
        }
    }

    fn offset_pairs(self) -> &'static [(usize, usize)] {
        match self {
            Self::Entry => &[(0, 4), (8, 12)],
            Self::GetMany => &[(4, 8)],
            Self::Diff => &[(4, 8), (12, 16), (20, 24)],
            Self::Conflict => &[(4, 8), (12, 16), (20, 24), (28, 32)],
            Self::IndexMatch => &[(4, 8), (12, 16), (20, 24), (28, 32)],
            Self::JoinedIndexRecord => &[(4, 8), (12, 16), (20, 24), (28, 32), (36, 40)],
            Self::ProximityNeighbor => &[(4, 8), (24, 28), (32, 36)],
        }
    }
}

impl TryFrom<u16> for PackedPageKind {
    type Error = BindingError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Entry),
            2 => Ok(Self::GetMany),
            3 => Ok(Self::Diff),
            4 => Ok(Self::Conflict),
            5 => Ok(Self::IndexMatch),
            6 => Ok(Self::JoinedIndexRecord),
            7 => Ok(Self::ProximityNeighbor),
            _ => Err(BindingError::malformed_transport(format!(
                "unknown packed page kind {value}"
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PageLimits {
    pub(crate) max_records: u32,
    pub(crate) max_arena_bytes: u64,
}

impl Default for PageLimits {
    fn default() -> Self {
        Self {
            max_records: 65_536,
            max_arena_bytes: 64 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(dead_code)]
pub(crate) struct EntryRecordRef<'a> {
    pub(crate) key: &'a [u8],
    pub(crate) value: &'a [u8],
}

#[derive(Clone, Copy, Debug, PartialEq)]
#[allow(dead_code)]
pub(crate) struct NeighborRecordRef<'a> {
    pub(crate) key: &'a [u8],
    pub(crate) distance: f64,
    pub(crate) rank: u32,
    pub(crate) value: Option<&'a [u8]>,
    pub(crate) proof: Option<&'a [u8]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(dead_code)]
pub(crate) struct IndexMatchRecordRef<'a> {
    pub(crate) term: &'a [u8],
    pub(crate) primary_key: &'a [u8],
    pub(crate) projection: Option<&'a [u8]>,
    pub(crate) cursor: Option<&'a [u8]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(dead_code)]
pub(crate) struct JoinedIndexRecordRef<'a> {
    pub(crate) term: &'a [u8],
    pub(crate) primary_key: &'a [u8],
    pub(crate) projection: Option<&'a [u8]>,
    pub(crate) source_value: &'a [u8],
    pub(crate) cursor: Option<&'a [u8]>,
}

#[derive(Debug)]
pub(crate) struct PackedPage<'a> {
    version: u16,
    kind: PackedPageKind,
    flags: u32,
    record_count: u32,
    table: &'a [u8],
    arena: &'a [u8],
}

impl<'a> PackedPage<'a> {
    pub(crate) fn parse(bytes: &'a [u8], limits: PageLimits) -> Result<Self, BindingError> {
        if bytes.len() < PAGE_HEADER_BYTES {
            return Err(BindingError::malformed_transport(
                "packed page is shorter than its header",
            ));
        }
        if bytes[..4] != PAGE_MAGIC {
            return Err(BindingError::malformed_transport(
                "packed page magic is invalid",
            ));
        }

        let version = read_u16(bytes, 4)?;
        if !matches!(version, PAGE_VERSION_V1 | PAGE_VERSION) {
            return Err(BindingError::malformed_transport(format!(
                "unsupported packed page version {version}"
            )));
        }
        let kind = PackedPageKind::try_from(read_u16(bytes, 6)?)?;
        if version == PAGE_VERSION_V1
            && !matches!(kind, PackedPageKind::Entry | PackedPageKind::GetMany)
        {
            return Err(BindingError::malformed_transport(
                "version 1 supports only entry and get-many pages",
            ));
        }

        let flags = read_u32(bytes, 8)?;
        if flags & !PAGE_FLAG_TERMINAL != 0 {
            return Err(BindingError::malformed_transport(
                "packed page has unknown header flags",
            ));
        }
        let record_count = read_u32(bytes, 12)?;
        if record_count > limits.max_records {
            return Err(BindingError::malformed_transport(
                "packed page exceeds the record limit",
            ));
        }
        let table_bytes = read_u32(bytes, 16)? as usize;
        let arena_bytes_u64 = read_u64(bytes, 20)?;
        if arena_bytes_u64 > limits.max_arena_bytes {
            return Err(BindingError::malformed_transport(
                "packed page exceeds the arena byte limit",
            ));
        }
        let arena_bytes = usize::try_from(arena_bytes_u64).map_err(|_| {
            BindingError::malformed_transport("packed page arena exceeds the address space")
        })?;

        let required_table = (record_count as usize)
            .checked_mul(kind.record_width())
            .ok_or_else(|| {
                BindingError::malformed_transport("packed page table length overflows")
            })?;
        let table_is_valid = if version == PAGE_VERSION {
            table_bytes == required_table
        } else {
            table_bytes >= required_table && table_bytes % kind.record_width() == 0
        };
        if !table_is_valid {
            return Err(BindingError::malformed_transport(
                "packed page table length does not match its records",
            ));
        }

        let table_end = PAGE_HEADER_BYTES
            .checked_add(table_bytes)
            .ok_or_else(|| BindingError::malformed_transport("packed page table overflows"))?;
        let total = table_end.checked_add(arena_bytes).ok_or_else(|| {
            BindingError::malformed_transport("packed page total length overflows")
        })?;
        if total != bytes.len() {
            return Err(BindingError::malformed_transport(
                "packed page length does not match its header",
            ));
        }

        let table = &bytes[PAGE_HEADER_BYTES..table_end];
        let arena = &bytes[table_end..];
        let page = Self {
            version,
            kind,
            flags,
            record_count,
            table,
            arena,
        };
        page.validate_records()?;
        Ok(page)
    }

    pub(crate) fn version(&self) -> u16 {
        self.version
    }

    pub(crate) fn kind(&self) -> PackedPageKind {
        self.kind
    }

    pub(crate) fn record_count(&self) -> u32 {
        self.record_count
    }

    pub(crate) fn terminal(&self) -> bool {
        self.flags & PAGE_FLAG_TERMINAL != 0
    }

    #[allow(dead_code)]
    pub(crate) fn entry(&self, index: u32) -> Result<EntryRecordRef<'a>, BindingError> {
        if self.kind != PackedPageKind::Entry {
            return Err(BindingError::malformed_transport(
                "packed page is not an entry page",
            ));
        }
        let record = self.record(index)?;
        Ok(EntryRecordRef {
            key: self.arena_slice(read_u32(record, 0)?, read_u32(record, 4)?)?,
            value: self.arena_slice(read_u32(record, 8)?, read_u32(record, 12)?)?,
        })
    }

    #[allow(dead_code)]
    pub(crate) fn neighbor(&self, index: u32) -> Result<NeighborRecordRef<'a>, BindingError> {
        if self.kind != PackedPageKind::ProximityNeighbor {
            return Err(BindingError::malformed_transport(
                "packed page is not a proximity neighbor page",
            ));
        }
        let record = self.record(index)?;
        let flags = read_u32(record, 0)?;
        Ok(NeighborRecordRef {
            key: self.arena_slice(read_u32(record, 4)?, read_u32(record, 8)?)?,
            distance: f64::from_bits(read_u64(record, 12)?),
            rank: read_u32(record, 20)?,
            value: optional_slice(
                self,
                flags & RECORD_FLAG_VALUE != 0,
                read_u32(record, 24)?,
                read_u32(record, 28)?,
            )?,
            proof: optional_slice(
                self,
                flags & RECORD_FLAG_PROOF != 0,
                read_u32(record, 32)?,
                read_u32(record, 36)?,
            )?,
        })
    }

    #[allow(dead_code)]
    pub(crate) fn index_match(&self, index: u32) -> Result<IndexMatchRecordRef<'a>, BindingError> {
        if self.kind != PackedPageKind::IndexMatch {
            return Err(BindingError::malformed_transport(
                "packed page is not an index-match page",
            ));
        }
        let record = self.record(index)?;
        let flags = read_u32(record, 0)?;
        Ok(IndexMatchRecordRef {
            term: self.arena_slice(read_u32(record, 4)?, read_u32(record, 8)?)?,
            primary_key: self.arena_slice(read_u32(record, 12)?, read_u32(record, 16)?)?,
            projection: optional_slice(
                self,
                flags & RECORD_FLAG_PROJECTION != 0,
                read_u32(record, 20)?,
                read_u32(record, 24)?,
            )?,
            cursor: optional_slice(
                self,
                flags & RECORD_FLAG_CURSOR != 0,
                read_u32(record, 28)?,
                read_u32(record, 32)?,
            )?,
        })
    }

    #[allow(dead_code)]
    pub(crate) fn joined_index_record(
        &self,
        index: u32,
    ) -> Result<JoinedIndexRecordRef<'a>, BindingError> {
        if self.kind != PackedPageKind::JoinedIndexRecord {
            return Err(BindingError::malformed_transport(
                "packed page is not a joined-index-record page",
            ));
        }
        let record = self.record(index)?;
        let flags = read_u32(record, 0)?;
        Ok(JoinedIndexRecordRef {
            term: self.arena_slice(read_u32(record, 4)?, read_u32(record, 8)?)?,
            primary_key: self.arena_slice(read_u32(record, 12)?, read_u32(record, 16)?)?,
            projection: optional_slice(
                self,
                flags & RECORD_FLAG_PROJECTION != 0,
                read_u32(record, 20)?,
                read_u32(record, 24)?,
            )?,
            source_value: self.arena_slice(read_u32(record, 28)?, read_u32(record, 32)?)?,
            cursor: optional_slice(
                self,
                flags & RECORD_FLAG_CURSOR != 0,
                read_u32(record, 36)?,
                read_u32(record, 40)?,
            )?,
        })
    }

    fn record(&self, index: u32) -> Result<&'a [u8], BindingError> {
        if index >= self.record_count {
            return Err(BindingError::malformed_transport(
                "packed page record index is out of range",
            ));
        }
        let width = self.kind.record_width();
        let start = index as usize * width;
        Ok(&self.table[start..start + width])
    }

    fn arena_slice(&self, offset: u32, length: u32) -> Result<&'a [u8], BindingError> {
        let start = offset as usize;
        let end = start.checked_add(length as usize).ok_or_else(|| {
            BindingError::malformed_transport("packed page field range overflows")
        })?;
        self.arena.get(start..end).ok_or_else(|| {
            BindingError::malformed_transport("packed page field is outside the arena")
        })
    }

    fn validate_records(&self) -> Result<(), BindingError> {
        for index in 0..self.record_count {
            let record = self.record(index)?;
            if self.kind == PackedPageKind::GetMany && read_u32(record, 0)? > 1 {
                return Err(BindingError::malformed_transport(
                    "get-many presence flag is invalid",
                ));
            }
            if self.kind == PackedPageKind::ProximityNeighbor {
                let flags = read_u32(record, 0)?;
                if flags & !(RECORD_FLAG_VALUE | RECORD_FLAG_PROOF) != 0 {
                    return Err(BindingError::malformed_transport(
                        "neighbor record has unknown flags",
                    ));
                }
                let distance = f64::from_bits(read_u64(record, 12)?);
                if !distance.is_finite() {
                    return Err(BindingError::malformed_transport(
                        "neighbor distance is not finite",
                    ));
                }
                optional_slice(
                    self,
                    flags & RECORD_FLAG_VALUE != 0,
                    read_u32(record, 24)?,
                    read_u32(record, 28)?,
                )?;
                optional_slice(
                    self,
                    flags & RECORD_FLAG_PROOF != 0,
                    read_u32(record, 32)?,
                    read_u32(record, 36)?,
                )?;
            }
            if matches!(
                self.kind,
                PackedPageKind::IndexMatch | PackedPageKind::JoinedIndexRecord
            ) {
                let flags = read_u32(record, 0)?;
                if flags & !(RECORD_FLAG_PROJECTION | RECORD_FLAG_CURSOR) != 0 {
                    return Err(BindingError::malformed_transport(
                        "index record has unknown flags",
                    ));
                }
                optional_slice(
                    self,
                    flags & RECORD_FLAG_PROJECTION != 0,
                    read_u32(record, 20)?,
                    read_u32(record, 24)?,
                )?;
                let (cursor_offset, cursor_length) = match self.kind {
                    PackedPageKind::IndexMatch => (28, 32),
                    PackedPageKind::JoinedIndexRecord => (36, 40),
                    _ => unreachable!(),
                };
                optional_slice(
                    self,
                    flags & RECORD_FLAG_CURSOR != 0,
                    read_u32(record, cursor_offset)?,
                    read_u32(record, cursor_length)?,
                )?;
            }
            for &(offset_at, length_at) in self.kind.offset_pairs() {
                self.arena_slice(read_u32(record, offset_at)?, read_u32(record, length_at)?)?;
            }
        }
        Ok(())
    }
}

fn optional_slice<'a>(
    page: &PackedPage<'a>,
    present: bool,
    offset: u32,
    length: u32,
) -> Result<Option<&'a [u8]>, BindingError> {
    if present {
        Ok(Some(page.arena_slice(offset, length)?))
    } else if offset == 0 && length == 0 {
        Ok(None)
    } else {
        Err(BindingError::malformed_transport(
            "absent optional field has a non-empty range",
        ))
    }
}

#[derive(Debug)]
// Builders are activated by the indexed and proximity transport tasks. The
// parser is already used to validate every page before it crosses the ABI.
#[allow(dead_code)]
pub(crate) struct PackedPageBuilder {
    kind: PackedPageKind,
    records: Vec<Vec<u8>>,
    arena: Vec<u8>,
    limits: PageLimits,
}

#[allow(dead_code)]
impl PackedPageBuilder {
    pub(crate) fn new(kind: PackedPageKind) -> Self {
        Self {
            kind,
            records: Vec::new(),
            arena: Vec::new(),
            limits: PageLimits::default(),
        }
    }

    pub(crate) fn with_limits(kind: PackedPageKind, limits: PageLimits) -> Self {
        Self {
            kind,
            records: Vec::new(),
            arena: Vec::new(),
            limits,
        }
    }

    pub(crate) fn push_entry(mut self, key: &[u8], value: &[u8]) -> Result<Self, BindingError> {
        if self.kind != PackedPageKind::Entry {
            return Err(BindingError::malformed_transport(
                "entry records require an entry page",
            ));
        }
        let (key_offset, key_len) = self.push_arena(key)?;
        let (value_offset, value_len) = self.push_arena(value)?;
        let mut record = Vec::with_capacity(self.kind.record_width());
        record.extend_from_slice(&key_offset.to_le_bytes());
        record.extend_from_slice(&key_len.to_le_bytes());
        record.extend_from_slice(&value_offset.to_le_bytes());
        record.extend_from_slice(&value_len.to_le_bytes());
        self.push_record(record)?;
        Ok(self)
    }

    pub(crate) fn push_neighbor(
        mut self,
        key: &[u8],
        distance: f64,
        rank: u32,
        value: Option<&[u8]>,
        proof: Option<&[u8]>,
    ) -> Result<Self, BindingError> {
        if self.kind != PackedPageKind::ProximityNeighbor {
            return Err(BindingError::malformed_transport(
                "neighbor records require a proximity-neighbor page",
            ));
        }
        if !distance.is_finite() {
            return Err(BindingError::new(
                ErrorCode::InvalidProximity,
                "neighbor distance must be finite",
            ));
        }
        let (key_offset, key_len) = self.push_arena(key)?;
        let (value_offset, value_len) = if let Some(value) = value {
            self.push_arena(value)?
        } else {
            (0, 0)
        };
        let (proof_offset, proof_len) = if let Some(proof) = proof {
            self.push_arena(proof)?
        } else {
            (0, 0)
        };
        let flags = (if value.is_some() {
            RECORD_FLAG_VALUE
        } else {
            0
        }) | (if proof.is_some() {
            RECORD_FLAG_PROOF
        } else {
            0
        });
        let mut record = Vec::with_capacity(self.kind.record_width());
        record.extend_from_slice(&flags.to_le_bytes());
        record.extend_from_slice(&key_offset.to_le_bytes());
        record.extend_from_slice(&key_len.to_le_bytes());
        record.extend_from_slice(&distance.to_bits().to_le_bytes());
        record.extend_from_slice(&rank.to_le_bytes());
        record.extend_from_slice(&value_offset.to_le_bytes());
        record.extend_from_slice(&value_len.to_le_bytes());
        record.extend_from_slice(&proof_offset.to_le_bytes());
        record.extend_from_slice(&proof_len.to_le_bytes());
        self.push_record(record)?;
        Ok(self)
    }

    pub(crate) fn push_index_match(
        mut self,
        term: &[u8],
        primary_key: &[u8],
        projection: Option<&[u8]>,
        cursor: Option<&[u8]>,
    ) -> Result<Self, BindingError> {
        if self.kind != PackedPageKind::IndexMatch {
            return Err(BindingError::malformed_transport(
                "index-match records require an index-match page",
            ));
        }
        let (term_offset, term_len) = self.push_arena(term)?;
        let (key_offset, key_len) = self.push_arena(primary_key)?;
        let (projection_offset, projection_len) = match projection {
            Some(value) => self.push_arena(value)?,
            None => (0, 0),
        };
        let (cursor_offset, cursor_len) = match cursor {
            Some(value) => self.push_arena(value)?,
            None => (0, 0),
        };
        let flags = optional_flags(projection, cursor);
        let mut record = Vec::with_capacity(self.kind.record_width());
        for value in [
            flags,
            term_offset,
            term_len,
            key_offset,
            key_len,
            projection_offset,
            projection_len,
            cursor_offset,
            cursor_len,
        ] {
            record.extend_from_slice(&value.to_le_bytes());
        }
        self.push_record(record)?;
        Ok(self)
    }

    pub(crate) fn push_joined_index_record(
        mut self,
        term: &[u8],
        primary_key: &[u8],
        projection: Option<&[u8]>,
        source_value: &[u8],
        cursor: Option<&[u8]>,
    ) -> Result<Self, BindingError> {
        if self.kind != PackedPageKind::JoinedIndexRecord {
            return Err(BindingError::malformed_transport(
                "joined records require a joined-index-record page",
            ));
        }
        let (term_offset, term_len) = self.push_arena(term)?;
        let (key_offset, key_len) = self.push_arena(primary_key)?;
        let (projection_offset, projection_len) = match projection {
            Some(value) => self.push_arena(value)?,
            None => (0, 0),
        };
        let (source_offset, source_len) = self.push_arena(source_value)?;
        let (cursor_offset, cursor_len) = match cursor {
            Some(value) => self.push_arena(value)?,
            None => (0, 0),
        };
        let flags = optional_flags(projection, cursor);
        let mut record = Vec::with_capacity(self.kind.record_width());
        for value in [
            flags,
            term_offset,
            term_len,
            key_offset,
            key_len,
            projection_offset,
            projection_len,
            source_offset,
            source_len,
            cursor_offset,
            cursor_len,
        ] {
            record.extend_from_slice(&value.to_le_bytes());
        }
        self.push_record(record)?;
        Ok(self)
    }

    pub(crate) fn finish(self, terminal: bool) -> Result<Box<[u8]>, BindingError> {
        let table_bytes = self
            .records
            .len()
            .checked_mul(self.kind.record_width())
            .ok_or_else(|| BindingError::malformed_transport("page table length overflows"))?;
        let table_bytes = u32::try_from(table_bytes)
            .map_err(|_| BindingError::malformed_transport("page table length exceeds u32"))?;
        let record_count = u32::try_from(self.records.len())
            .map_err(|_| BindingError::malformed_transport("page record count exceeds u32"))?;
        let arena_bytes = u64::try_from(self.arena.len())
            .map_err(|_| BindingError::malformed_transport("page arena length exceeds u64"))?;
        let mut bytes =
            Vec::with_capacity(PAGE_HEADER_BYTES + table_bytes as usize + self.arena.len());
        bytes.extend_from_slice(&PAGE_MAGIC);
        bytes.extend_from_slice(&PAGE_VERSION.to_le_bytes());
        bytes.extend_from_slice(&(self.kind as u16).to_le_bytes());
        bytes.extend_from_slice(&(if terminal { PAGE_FLAG_TERMINAL } else { 0 }).to_le_bytes());
        bytes.extend_from_slice(&record_count.to_le_bytes());
        bytes.extend_from_slice(&table_bytes.to_le_bytes());
        bytes.extend_from_slice(&arena_bytes.to_le_bytes());
        for record in self.records {
            bytes.extend_from_slice(&record);
        }
        bytes.extend_from_slice(&self.arena);
        PackedPage::parse(&bytes, self.limits)?;
        Ok(bytes.into_boxed_slice())
    }

    fn push_record(&mut self, record: Vec<u8>) -> Result<(), BindingError> {
        if record.len() != self.kind.record_width() {
            return Err(BindingError::malformed_transport(
                "record width does not match the page kind",
            ));
        }
        if self.records.len() >= self.limits.max_records as usize {
            return Err(BindingError::malformed_transport(
                "page builder exceeds the record limit",
            ));
        }
        self.records.push(record);
        Ok(())
    }

    fn push_arena(&mut self, value: &[u8]) -> Result<(u32, u32), BindingError> {
        let next_len = self
            .arena
            .len()
            .checked_add(value.len())
            .ok_or_else(|| BindingError::malformed_transport("page arena length overflows"))?;
        if next_len as u64 > self.limits.max_arena_bytes {
            return Err(BindingError::malformed_transport(
                "page builder exceeds the arena byte limit",
            ));
        }
        let offset = u32::try_from(self.arena.len())
            .map_err(|_| BindingError::malformed_transport("page arena offset exceeds u32"))?;
        let length = u32::try_from(value.len())
            .map_err(|_| BindingError::malformed_transport("page field length exceeds u32"))?;
        self.arena.extend_from_slice(value);
        Ok((offset, length))
    }
}

fn optional_flags(projection: Option<&[u8]>, cursor: Option<&[u8]>) -> u32 {
    (if projection.is_some() {
        RECORD_FLAG_PROJECTION
    } else {
        0
    }) | (if cursor.is_some() {
        RECORD_FLAG_CURSOR
    } else {
        0
    })
}

fn read_u16(bytes: &[u8], offset: usize) -> Result<u16, BindingError> {
    let value = bytes
        .get(offset..offset + 2)
        .ok_or_else(|| BindingError::malformed_transport("packed page u16 field is truncated"))?;
    Ok(u16::from_le_bytes([value[0], value[1]]))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, BindingError> {
    let value = bytes
        .get(offset..offset + 4)
        .ok_or_else(|| BindingError::malformed_transport("packed page u32 field is truncated"))?;
    Ok(u32::from_le_bytes([value[0], value[1], value[2], value[3]]))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, BindingError> {
    let value = bytes
        .get(offset..offset + 8)
        .ok_or_else(|| BindingError::malformed_transport("packed page u64 field is truncated"))?;
    Ok(u64::from_le_bytes([
        value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7],
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proximity_neighbor_page_preserves_f64_distance() {
        let distance: f64 = 1.000_000_000_000_000_2;
        let bytes = PackedPageBuilder::new(PackedPageKind::ProximityNeighbor)
            .push_neighbor(b"k", distance, 7, Some(b"value"), None)
            .unwrap()
            .finish(true)
            .unwrap();
        let page = PackedPage::parse(&bytes, PageLimits::default()).unwrap();
        let record = page.neighbor(0).unwrap();
        assert_eq!(record.distance.to_bits(), distance.to_bits());
        assert_eq!(record.rank, 7);
    }

    #[test]
    fn joined_index_page_round_trips_binary_fields() {
        let bytes = PackedPageBuilder::new(PackedPageKind::JoinedIndexRecord)
            .push_joined_index_record(
                b"red",
                b"u1",
                Some(b"Ada"),
                br#"{"team":"red"}"#,
                Some(b"cursor"),
            )
            .unwrap()
            .finish(true)
            .unwrap();
        let page = PackedPage::parse(&bytes, PageLimits::default()).unwrap();
        let record = page.joined_index_record(0).unwrap();

        assert_eq!(record.term, b"red");
        assert_eq!(record.primary_key, b"u1");
        assert_eq!(record.projection, Some(b"Ada".as_slice()));
        assert_eq!(record.source_value, br#"{"team":"red"}"#);
        assert_eq!(record.cursor, Some(b"cursor".as_slice()));
    }

    #[test]
    fn index_match_page_round_trips_optional_fields() {
        let bytes = PackedPageBuilder::new(PackedPageKind::IndexMatch)
            .push_index_match(b"red", b"u1", None, Some(b"cursor"))
            .unwrap()
            .finish(false)
            .unwrap();
        let page = PackedPage::parse(&bytes, PageLimits::default()).unwrap();
        let record = page.index_match(0).unwrap();

        assert_eq!(record.term, b"red");
        assert_eq!(record.primary_key, b"u1");
        assert_eq!(record.projection, None);
        assert_eq!(record.cursor, Some(b"cursor".as_slice()));
        assert!(!page.terminal());
    }

    #[test]
    fn entry_page_round_trips_binary_fields() {
        let bytes = PackedPageBuilder::new(PackedPageKind::Entry)
            .push_entry(b"\0key", b"\xffvalue")
            .unwrap()
            .finish(true)
            .unwrap();
        let page = PackedPage::parse(&bytes, PageLimits::default()).unwrap();
        let entry = page.entry(0).unwrap();

        assert_eq!(page.kind(), PackedPageKind::Entry);
        assert_eq!(page.version(), PAGE_VERSION);
        assert_eq!(page.record_count(), 1);
        assert!(page.terminal());
        assert_eq!(entry.key, b"\0key");
        assert_eq!(entry.value, b"\xffvalue");
    }

    #[test]
    fn page_validator_rejects_out_of_arena_neighbor_payload() {
        let valid = PackedPageBuilder::new(PackedPageKind::ProximityNeighbor)
            .push_neighbor(b"key", 0.25, 0, Some(b"value"), None)
            .unwrap()
            .finish(true)
            .unwrap();
        let parsed = PackedPage::parse(&valid, PageLimits::default()).unwrap();
        let neighbor = parsed.neighbor(0).unwrap();
        assert_eq!(neighbor.key, b"key");
        assert_eq!(neighbor.distance, 0.25);
        assert_eq!(neighbor.rank, 0);
        assert_eq!(neighbor.value, Some(b"value".as_slice()));
        assert_eq!(neighbor.proof, None);

        let mut bytes = valid.into_vec();
        bytes[52..56].copy_from_slice(&u32::MAX.to_le_bytes());

        let error = PackedPage::parse(&bytes, PageLimits::default()).unwrap_err();
        assert_eq!(error.code, ErrorCode::MalformedTransport);
    }

    #[test]
    fn page_validator_rejects_an_absent_optional_field_with_a_range() {
        let mut bytes = PackedPageBuilder::new(PackedPageKind::ProximityNeighbor)
            .push_neighbor(b"key", 0.25, 0, Some(b"value"), None)
            .unwrap()
            .finish(true)
            .unwrap()
            .into_vec();
        bytes[28..32].copy_from_slice(&0_u32.to_le_bytes());

        let error = PackedPage::parse(&bytes, PageLimits::default()).unwrap_err();
        assert_eq!(error.code, ErrorCode::MalformedTransport);
    }

    #[test]
    fn builder_enforces_configured_record_limit() {
        let builder = PackedPageBuilder::with_limits(
            PackedPageKind::Entry,
            PageLimits {
                max_records: 0,
                max_arena_bytes: 16,
            },
        );
        assert_eq!(
            builder.push_entry(b"k", b"v").unwrap_err().code,
            ErrorCode::MalformedTransport,
        );
    }

    #[test]
    fn parser_accepts_existing_v1_entry_pages() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"PRPG");
        bytes.extend_from_slice(&1_u16.to_le_bytes());
        bytes.extend_from_slice(&1_u16.to_le_bytes());
        bytes.extend_from_slice(&1_u32.to_le_bytes());
        bytes.extend_from_slice(&1_u32.to_le_bytes());
        bytes.extend_from_slice(&16_u32.to_le_bytes());
        bytes.extend_from_slice(&2_u64.to_le_bytes());
        bytes.extend_from_slice(&0_u32.to_le_bytes());
        bytes.extend_from_slice(&1_u32.to_le_bytes());
        bytes.extend_from_slice(&1_u32.to_le_bytes());
        bytes.extend_from_slice(&1_u32.to_le_bytes());
        bytes.extend_from_slice(b"kv");

        let page = PackedPage::parse(&bytes, PageLimits::default()).unwrap();
        let entry = page.entry(0).unwrap();
        assert_eq!(entry.key, b"k");
        assert_eq!(entry.value, b"v");
    }
}
