use prolly::{IndexDescriptor, IndexedSnapshotBundle, SecondaryIndexCursor};

fn next_u64(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

#[test]
fn bounded_parser_fuzz_corpus_never_panics_or_allocates_from_untrusted_lengths() {
    const CASES: usize = 10_000;
    const MAX_INPUT_BYTES: usize = 4 * 1024;

    let mut state = 0x4d59_5df4_d0f3_3173;
    let mut input = Vec::with_capacity(MAX_INPUT_BYTES);
    for case in 0..CASES {
        let length = (next_u64(&mut state) as usize) % (MAX_INPUT_BYTES + 1);
        input.clear();
        input.extend((0..length).map(|_| next_u64(&mut state) as u8));

        let _ = IndexDescriptor::from_bytes(&input);
        let _ = SecondaryIndexCursor::from_bytes(&input);
        let _ = IndexedSnapshotBundle::from_bytes(&input);

        if case % 97 == 0 {
            input.fill(0xff);
            let _ = IndexDescriptor::from_bytes(&input);
            let _ = SecondaryIndexCursor::from_bytes(&input);
            let _ = IndexedSnapshotBundle::from_bytes(&input);
        }
    }
}
