package build.crab.prolly.examples;

import build.crab.prolly.Prolly;

public final class SecondaryIndex {
    private SecondaryIndex() {
    }

    public static void main(String[] args) throws Exception {
        Prolly.useLocalDebugLibrary();
        CookbookScenarios.secondaryIndex();
    }
}
