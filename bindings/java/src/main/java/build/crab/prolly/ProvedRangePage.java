package build.crab.prolly;

public record ProvedRangePage(RangePageRecord page, RangePageProof proof) {
    static ProvedRangePage fromRecord(ProvedRangePageRecord record) throws ProllyBindingException {
        return new ProvedRangePage(
                record.getPage(),
                RangePageProof.fromRecord(record.getProof()));
    }
}
