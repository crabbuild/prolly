package build.crab.prolly.javaapi;

public record ParallelConfig(long maxThreads, long parallelismThreshold) {
    public ParallelConfig {
        if (maxThreads < 0 || parallelismThreshold < 0) {
            throw new IllegalArgumentException("parallel config must be non-negative");
        }
    }
}
