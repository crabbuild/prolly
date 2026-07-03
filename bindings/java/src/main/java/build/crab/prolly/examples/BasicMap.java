package build.crab.prolly.examples;

import build.crab.prolly.Prolly;

public final class BasicMap {
    private BasicMap() {
    }

    public static void main(String[] args) throws Exception {
        Prolly.useLocalDebugLibrary();
        CookbookScenarios.basicMap();
    }
}
