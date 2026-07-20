# Redis prolly scale baseline

All values below are medians of independent repetitions.

## Workload contract

- Sizes: 1000000 records; repetitions: 3.
- Mutations: 30% of each base; read/scan samples: 10000.
- Merge changes are the total across two equal, disjoint branches.

## Workloads

| Records | Operation | Pattern | Cache | Median ns/op | Median ops/s |
|---:|---|---|---|---:|---:|
| 1000000 | put | append | n/a | 3333875.0 | 300.0 |
| 1000000 | put | random | n/a | 5670458.0 | 176.4 |
| 1000000 | put | clustered | n/a | 5201167.0 | 192.3 |
| 1000000 | batch | append | n/a | 1823.0 | 548536.4 |
| 1000000 | batch | random | n/a | 8695.4 | 115002.9 |
| 1000000 | batch | clustered | n/a | 3048.6 | 328015.5 |
| 1000000 | get_cold | append | cold-manager | 1389764.2 | 719.5 |
| 1000000 | get_cold | random | cold-manager | 1375394.6 | 727.1 |
| 1000000 | get_cold | clustered | cold-manager | 1421346.1 | 703.6 |
| 1000000 | get_warm | append | warm-manager | 261.4 | 3825554.7 |
| 1000000 | get_warm | random | warm-manager | 636.2 | 1571925.4 |
| 1000000 | get_warm | clustered | warm-manager | 238.2 | 4197932.5 |
| 1000000 | query | append | n/a | 1286.8 | 777136.6 |
| 1000000 | query | random | n/a | 63105.6 | 15846.5 |
| 1000000 | query | clustered | n/a | 1390.3 | 719288.6 |
| 1000000 | scan | append | n/a | 3101.3 | 322448.0 |
| 1000000 | scan | random | n/a | 2128.1 | 469897.2 |
| 1000000 | scan | clustered | n/a | 2459.9 | 406519.9 |
| 1000000 | full_scan | append | n/a | 2905.8 | 344139.5 |
| 1000000 | diff | append | n/a | 2897.7 | 345098.6 |
| 1000000 | diff | random | n/a | 4017.8 | 248891.2 |
| 1000000 | diff | clustered | n/a | 1257.7 | 795112.1 |
| 1000000 | merge | append | n/a | 2294.2 | 435876.7 |
| 1000000 | merge | random | n/a | 111587.7 | 8961.6 |
| 1000000 | merge | clustered | n/a | 19.5 | 51372062.2 |

## Fixture context

| Records | Repetitions | Median build ms | Median records/s | Median Redis dataset MiB | Median AOF MiB | Median namespace keys |
|---:|---:|---:|---:|---:|---:|---:|
| 1000000 | 3 | 1768.299 | 565515.1 | 110.54 | 100.60 | 7720 |

## Measurement boundaries

- Fixture cloning, diff/merge branch setup, validation, stats, publication, and reopen checks are outside timed intervals.
- After each cell namespace is deleted, the harness completes and validates a manual AOF rewrite outside timing to bound disk usage; automatic rewrites remain disabled.
- Scans include full iterator consumption; cold point gets clear the manager cache before every lookup.
- Each workload cell uses a server-side `COPY` clone in an isolated Redis key namespace; cloning and cleanup are outside timing.

## Interpretation limits

- End-to-end asynchronous `AsyncProlly<RedisStore>` over local Docker TCP. Redis uses AOF with `appendfsync always`; RDB snapshots and automatic AOF rewrites are disabled.
- Tokio uses 4 worker threads; scheduler and async store overhead are included.
- Manager cache state is controlled. Redis cache state, Docker Desktop's Linux VM, TCP, host scheduling, and storage caches are not.
- Keys are 24 bytes and values are 100 bytes. `appendfsync always` measures Redis acknowledgement after its configured AOF fsync path, but Docker Desktop and host storage can still have volatile layers. Results do not predict Redis Cluster, remote Redis, concurrent writers, or raw Redis commands.
