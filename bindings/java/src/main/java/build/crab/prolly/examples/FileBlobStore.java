package build.crab.prolly.examples;

import build.crab.prolly.Prolly;

public final class FileBlobStore {
    private FileBlobStore() {
    }

    public static void main(String[] args) throws Exception {
        Prolly.useLocalDebugLibrary();
        CookbookScenarios.fileBlobStore();
    }
}
