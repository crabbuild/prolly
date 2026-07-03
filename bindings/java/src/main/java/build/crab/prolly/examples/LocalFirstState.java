package build.crab.prolly.examples;

import build.crab.prolly.Prolly;

public final class LocalFirstState {
    public static void main(String[] args) throws Exception {
        Prolly.useLocalDebugLibrary();
        CookbookApps.localFirstState();
    }
}
