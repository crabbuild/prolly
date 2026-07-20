# SQLite-backed prolly key-pattern benchmark

All values below are medians of independent repetitions.

## Workloads

| Records | Operation | Pattern | Cache | Median ns/op | Median ops/s |
|---:|---|---|---|---:|---:|
| 10000 | put | append | n/a | 221822.1 | 4508.1 |
| 10000 | put | random | n/a | 338479.6 | 2954.4 |
| 10000 | put | clustered | n/a | 321966.2 | 3105.9 |
| 10000 | batch | append | n/a | 14538.3 | 68783.7 |
| 10000 | batch | random | n/a | 95528.8 | 10468.1 |
| 10000 | batch | clustered | n/a | 33955.4 | 29450.4 |
| 10000 | point-read | append | cold-manager | 8508.3 | 117531.7 |
| 10000 | point-read | append | warm-manager | 241.7 | 4138045.2 |
| 10000 | point-read | random | cold-manager | 72171.7 | 13855.9 |
| 10000 | point-read | random | warm-manager | 4184.6 | 238972.6 |
| 10000 | point-read | clustered | cold-manager | 10937.9 | 91425.1 |
| 10000 | point-read | clustered | warm-manager | 245.4 | 4074647.5 |
| 10000 | range-scan | append | n/a | 5820.0 | 171821.3 |
| 10000 | range-scan | random | n/a | 20649.6 | 48427.1 |
| 10000 | range-scan | clustered | n/a | 8592.9 | 116374.9 |
| 50000 | put | append | n/a | 261642.2 | 3822.0 |
| 50000 | put | random | n/a | 449827.3 | 2223.1 |
| 50000 | put | clustered | n/a | 461796.2 | 2165.5 |
| 50000 | batch | append | n/a | 27581.1 | 36256.7 |
| 50000 | batch | random | n/a | 187072.6 | 5345.5 |
| 50000 | batch | clustered | n/a | 25973.8 | 38500.4 |
| 50000 | point-read | append | cold-manager | 18390.1 | 54377.1 |
| 50000 | point-read | append | warm-manager | 769.4 | 1299687.0 |
| 50000 | point-read | random | cold-manager | 126328.3 | 7915.9 |
| 50000 | point-read | random | warm-manager | 4330.8 | 230906.9 |
| 50000 | point-read | clustered | cold-manager | 14442.7 | 69239.3 |
| 50000 | point-read | clustered | warm-manager | 734.1 | 1362241.9 |
| 50000 | range-scan | append | n/a | 16764.9 | 59648.4 |
| 50000 | range-scan | random | n/a | 17078.4 | 58553.4 |
| 50000 | range-scan | clustered | n/a | 14568.8 | 68640.1 |
| 100000 | put | append | n/a | 382094.2 | 2617.2 |
| 100000 | put | random | n/a | 495991.7 | 2016.2 |
| 100000 | put | clustered | n/a | 405423.2 | 2466.6 |
| 100000 | batch | append | n/a | 18690.5 | 53503.0 |
| 100000 | batch | random | n/a | 185978.3 | 5377.0 |
| 100000 | batch | clustered | n/a | 31743.0 | 31503.0 |
| 100000 | point-read | append | cold-manager | 13061.7 | 76559.7 |
| 100000 | point-read | append | warm-manager | 561.5 | 1781074.0 |
| 100000 | point-read | random | cold-manager | 131073.2 | 7629.3 |
| 100000 | point-read | random | warm-manager | 4379.9 | 228314.8 |
| 100000 | point-read | clustered | cold-manager | 19745.2 | 50645.3 |
| 100000 | point-read | clustered | warm-manager | 1024.4 | 976165.9 |
| 100000 | range-scan | append | n/a | 12904.0 | 77495.1 |
| 100000 | range-scan | random | n/a | 11286.3 | 88602.7 |
| 100000 | range-scan | clustered | n/a | 19396.0 | 51557.1 |
| 500000 | put | append | n/a | 618920.1 | 1615.7 |
| 500000 | put | random | n/a | 774330.0 | 1291.4 |
| 500000 | put | clustered | n/a | 757911.9 | 1319.4 |
| 500000 | batch | append | n/a | 13973.4 | 71564.4 |
| 500000 | batch | random | n/a | 207640.4 | 4816.0 |
| 500000 | batch | clustered | n/a | 24657.0 | 40556.4 |
| 500000 | point-read | append | cold-manager | 12962.7 | 77144.2 |
| 500000 | point-read | append | warm-manager | 712.5 | 1403459.5 |
| 500000 | point-read | random | cold-manager | 138324.9 | 7229.4 |
| 500000 | point-read | random | warm-manager | 5134.9 | 194745.1 |
| 500000 | point-read | clustered | cold-manager | 13071.0 | 76504.9 |
| 500000 | point-read | clustered | warm-manager | 754.3 | 1325673.8 |
| 500000 | range-scan | append | n/a | 14592.6 | 68527.9 |
| 500000 | range-scan | random | n/a | 29860.4 | 33489.2 |
| 500000 | range-scan | clustered | n/a | 12317.2 | 81187.0 |
| 1000000 | put | append | n/a | 749001.2 | 1335.1 |
| 1000000 | put | random | n/a | 1035551.5 | 965.7 |
| 1000000 | put | clustered | n/a | 858958.6 | 1164.2 |
| 1000000 | batch | append | n/a | 15302.6 | 65348.5 |
| 1000000 | batch | random | n/a | 209982.4 | 4762.3 |
| 1000000 | batch | clustered | n/a | 21629.0 | 46234.2 |
| 1000000 | point-read | append | cold-manager | 14438.4 | 69259.7 |
| 1000000 | point-read | append | warm-manager | 742.4 | 1347050.8 |
| 1000000 | point-read | random | cold-manager | 142567.4 | 7014.2 |
| 1000000 | point-read | random | warm-manager | 5105.1 | 195881.3 |
| 1000000 | point-read | clustered | cold-manager | 12542.8 | 79727.3 |
| 1000000 | point-read | clustered | warm-manager | 649.6 | 1539369.3 |
| 1000000 | range-scan | append | n/a | 13245.5 | 75497.2 |
| 1000000 | range-scan | random | n/a | 13421.1 | 74509.7 |
| 1000000 | range-scan | clustered | n/a | 11936.3 | 83778.1 |

## Fixture context

Validated fixture rows: 15.

## Interpretation limits

- End-to-end synchronous `Prolly<SqliteStore>` on one local connection.
- SQLite uses WAL and `synchronous=NORMAL`; this is not `FULL` durability.
- Manager cache state is controlled, but the operating-system filesystem cache is not.
- Keys are 24 bytes and values are 100 bytes. Results do not predict concurrent writers, remote filesystems, or raw SQLite.
