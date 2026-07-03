package build.crab.prolly.examples;

import build.crab.prolly.Prolly;

public final class AgentEventLog {
    public static void main(String[] args) throws Exception {
        Prolly.useLocalDebugLibrary();
        CookbookApps.agentEventLog();
    }
}
