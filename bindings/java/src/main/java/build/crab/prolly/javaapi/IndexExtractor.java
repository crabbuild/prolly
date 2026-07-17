package build.crab.prolly.javaapi;

import java.util.List;

@FunctionalInterface
public interface IndexExtractor {
    List<IndexEntry> extract(byte[] primaryKey, byte[] sourceValue);
}
