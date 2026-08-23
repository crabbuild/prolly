import { spawnSync } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const packageDir = fileURLToPath(new URL("..", import.meta.url));
const scratch = await mkdtemp(join(tmpdir(), "prolly-node-package-"));
let tarball;

function run(command, args, cwd) {
  const result = spawnSync(command, args, { cwd, encoding: "utf8", stdio: "pipe" });
  if (result.status !== 0) {
    process.stderr.write(result.stdout ?? "");
    process.stderr.write(result.stderr ?? "");
    throw new Error(`${command} ${args.join(" ")} failed with status ${result.status}`);
  }
  return result.stdout.trim();
}

try {
  run("npm", ["run", "build"], packageDir);
  const pack = JSON.parse(run("npm", ["pack", "--json", "--ignore-scripts"], packageDir))[0];
  tarball = join(packageDir, pack.filename);
  run("npm", ["init", "--yes"], scratch);
  run("npm", ["install", "--ignore-scripts", tarball], scratch);
  run(
    process.execPath,
    ["--input-type=module", "--eval", "import { Engine } from 'prollydb'; const engine = await Engine.memory(); engine.close();"],
    scratch
  );
  process.stdout.write("packed Node binding imports and opens a native engine\n");
} finally {
  if (tarball) await rm(tarball, { force: true });
  await rm(scratch, { recursive: true, force: true });
}
