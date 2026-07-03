package build.crab.prolly;

public record ProvedDiffPage(DiffPageRecord page, DiffPageProof proof) {
    static ProvedDiffPage fromRecord(ProvedDiffPageRecord record) throws ProllyBindingException {
        return new ProvedDiffPage(
                record.getPage(),
                DiffPageProof.fromRecord(record.getProof()));
    }
}
