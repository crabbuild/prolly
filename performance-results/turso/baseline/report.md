# Turso prolly scale baseline

All values below are medians of independent repetitions.

## Workload contract

- Sizes: 1000000 records; repetitions: 3.
- Mutations: 30% of each base; read/scan samples: 10000.
- Merge changes are the total across two equal, disjoint branches.

## Workloads

| Records | Operation | Pattern | Cache | Median ns/op | Median ops/s |
|---:|---|---|---|---:|---:|
| 1000000 | put | append | n/a | 4412833.0 | 226.6 |
| 1000000 | put | random | n/a | 6212458.0 | 161.0 |
| 1000000 | put | clustered | n/a | 7331000.0 | 136.4 |
| 1000000 | batch | append | n/a | 1286.0 | 777595.7 |
| 1000000 | batch | random | n/a | 7648.9 | 130738.2 |
| 1000000 | batch | clustered | n/a | 2682.8 | 372738.9 |
| 1000000 | get_cold | append | cold-manager | 166624.8 | 6001.5 |
| 1000000 | get_cold | random | cold-manager | 209291.1 | 4778.0 |
| 1000000 | get_cold | clustered | cold-manager | 176628.3 | 5661.6 |
| 1000000 | get_warm | append | warm-manager | 245.9 | 4067175.1 |
| 1000000 | get_warm | random | warm-manager | 691.9 | 1445226.0 |
| 1000000 | get_warm | clustered | warm-manager | 243.1 | 4113393.1 |
| 1000000 | query | append | n/a | 2242.8 | 445879.5 |
| 1000000 | query | random | n/a | 69907.8 | 14304.5 |
| 1000000 | query | clustered | n/a | 2510.9 | 398261.6 |
| 1000000 | scan | append | n/a | 4083.2 | 244906.2 |
| 1000000 | scan | random | n/a | 2984.7 | 335039.7 |
| 1000000 | scan | clustered | n/a | 3110.8 | 321463.7 |
| 1000000 | full_scan | append | n/a | 3282.2 | 304675.6 |
| 1000000 | diff | append | n/a | 447.1 | 2236547.2 |
| 1000000 | diff | random | n/a | 3739.7 | 267399.9 |
| 1000000 | diff | clustered | n/a | 1106.5 | 903739.1 |
| 1000000 | merge | append | n/a | 741.4 | 1348825.3 |
| 1000000 | merge | random | n/a | 113340.7 | 8823.0 |
| 1000000 | merge | clustered | n/a | 18.6 | 53789554.8 |

## Fixture context

| Records | Repetitions | Median build ms | Median records/s | Median database MiB |
|---:|---:|---:|---:|---:|
| 1000000 | 3 | 996.425 | 1003588.1 | 105.69 |

## Measurement boundaries

- Fixture cloning, diff/merge branch setup, validation, stats, publication, and reopen checks are outside timed intervals.
- Scans include full iterator consumption; cold point gets clear the manager cache before every lookup.
- Each workload cell uses an isolated clone of a closed local Turso fixture.

## Interpretation limits

- End-to-end asynchronous `AsyncProlly<TursoStore>` on native local Turso with no cloud synchronization.
- Tokio uses 4 worker threads; scheduler and async store overhead are included.
- Manager cache state is controlled, but the operating-system filesystem cache is not.
- Keys are 24 bytes and values are 100 bytes. Results do not predict Turso Cloud synchronization, concurrent writers, remote filesystems, or raw Turso SQL.
