# Third-party projects

This directory contains projects incorporated into the `prolly` repository and
adapted for integration with its packages.

## Prolly Tree Visualizer

[`prolly-tree-visualizer`](prolly-tree-visualizer/README.md) is based on the
original
[`timsehn/prolly-tree-visualizer`](https://github.com/timsehn/prolly-tree-visualizer)
repository. Credit goes to Tim Sehn and the original contributors for the
visualizer's design and implementation.

The version vendored here replaces the original DoltLite backend with this
repository's `@trail/prolly-wasm` binding so it can render the actual trees,
content IDs, lookups, diffs, and storage behavior produced by `prolly-map`.
