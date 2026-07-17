package build.crab.prolly.javaapi;

public record GcSweep(GcPlan plan, long deletedNodes, long deletedBytes) {
    static GcSweep fromBridge(build.crab.prolly.api.JavaGcSweep value) {
        return new GcSweep(
                GcPlan.fromBridge(value.getPlan()), value.getDeletedNodes(), value.getDeletedBytes());
    }
}
