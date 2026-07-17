package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaIndexPage;
import java.util.List;
import java.util.Optional;

public record OwnedIndexPage(List<IndexMatch> matches, Optional<byte[]> nextCursor) {
    static OwnedIndexPage fromNative(JavaIndexPage value) {
        return new OwnedIndexPage(
                value.getMatches().stream().map(row -> new IndexMatch(
                        row.getTerm().clone(), row.getPrimaryKey().clone(),
                        row.getProjection() == null ? null : row.getProjection().clone())).toList(),
                Optional.ofNullable(value.getNextCursor()).map(byte[]::clone));
    }
}
