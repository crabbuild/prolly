package build.crab.prolly.examples;

import build.crab.prolly.Prolly;

public final class DiffMerge {
    private DiffMerge() {
    }

    public static void main(String[] args) throws Exception {
        Prolly.useLocalDebugLibrary();
        CookbookScenarios.diffMerge();
    }
}
