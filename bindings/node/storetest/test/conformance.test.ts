import test from "node:test";

import { runStoreConformance } from "../src/index.ts";
import { FakeRemoteStore } from "../../test/support/fake-remote-store.ts";

test("shared Node store conformance covers the protocol-v1 memory store", async () => {
  await runStoreConformance(() => new FakeRemoteStore());
});
