package build.crab.prolly.examples;

import build.crab.prolly.Prolly;

public final class Resolver {
    public static void main(String[] args) throws Exception {
        Prolly.useLocalDebugLibrary();
        CookbookApps.resolver();
    }
}
