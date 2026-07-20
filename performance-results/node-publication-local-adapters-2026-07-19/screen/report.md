# Node publication revision gate

Generated UTC: 2026-07-19T22:32:31.857430+00:00
Input: `performance-results/node-publication-local-adapters-2026-07-19/stores/raw-results.csv`
Revisions: 81357948363fdb51dbc201ae5ba1a7377bb297e0, a2f4e7a3066d2259783c8d815b45c0461c77b06b

All measurements are local-only; Turso Cloud synchronization is disabled.
Repeated samples within a revision pair are collapsed by median before paired changes are evaluated.
The broad local-adapter screen applies 5 us median and 10 us p95 absolute noise floors in addition to percentage limits; focused and foundation suites do not.

Evaluated groups: 108
Gate failures: 18

## Environment limitations

- pglite-sync: @electric-sql/pglite package is unavailable

## Failures

- median_latency_regression:('local-adapters', 'file-sync', 10000, 'batch', 'append')
- median_throughput_regression:('local-adapters', 'file-sync', 10000, 'batch', 'append')
- p95_latency_regression:('local-adapters', 'file-sync', 10000, 'batch', 'append')
- median_latency_regression:('local-adapters', 'file-sync', 10000, 'reopen', 'clustered')
- median_throughput_regression:('local-adapters', 'file-sync', 10000, 'reopen', 'clustered')
- median_latency_regression:('local-adapters', 'file-sync', 10000, 'reopen', 'random')
- median_throughput_regression:('local-adapters', 'file-sync', 10000, 'reopen', 'random')
- median_latency_regression:('local-adapters', 'memory-sync', 10000, 'merge', 'append')
- median_throughput_regression:('local-adapters', 'memory-sync', 10000, 'merge', 'append')
- median_latency_regression:('local-adapters', 'slatedb-sync', 10000, 'diff', 'clustered')
- median_throughput_regression:('local-adapters', 'slatedb-sync', 10000, 'diff', 'clustered')
- median_latency_regression:('local-adapters', 'slatedb-sync', 10000, 'diff', 'random')
- median_throughput_regression:('local-adapters', 'slatedb-sync', 10000, 'diff', 'random')
- p95_latency_regression:('local-adapters', 'slatedb-sync', 10000, 'diff', 'random')
- median_latency_regression:('local-adapters', 'slatedb-sync', 10000, 'reopen', 'random')
- median_throughput_regression:('local-adapters', 'slatedb-sync', 10000, 'reopen', 'random')
- median_latency_regression:('local-adapters', 'turso-async', 10000, 'reopen', 'random')
- median_throughput_regression:('local-adapters', 'turso-async', 10000, 'reopen', 'random')
