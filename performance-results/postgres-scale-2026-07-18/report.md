# PostgreSQL-backed Prolly performance

Revision `42873d1561c3641c9091fe2a616567e790029762` (dirty=true); 142 validated raw rows.

This is an end-to-end single-client measurement of the public async Prolly API over SQLx and PostgreSQL 16 in Docker Desktop. Latency is wall-clock time; PostgreSQL execution time is separately observed by `pg_stat_statements`.

## 1,000,000 records

| Operation | Pattern | Cache | n | Median ms | Min–max ms | ns/op | ops/s | Nodes R/W | MiB R/W | PG calls / ms |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| batch | append | cold-manager | n=3 | 40.980 | 39.418–43.610 | 4098 | 244023.2 | 0/77 | 0.00/0.31 | 83/3.287 |
| batch | clustered | cold-manager | n=3 | 9042.247 | 8963.695–9343.542 | 904225 | 1105.9 | 7717/7719 | 30.19/30.20 | 15442/440.886 |
| batch | random | cold-manager | n=3 | 8932.914 | 8882.973–9500.708 | 893291 | 1119.5 | 7717/7719 | 30.19/30.20 | 15442/426.423 |
| build | base | cold-manager | n=1 | 3620.603 | 3620.603–3620.603 | 3621 | 276197.1 | 0/7719 | 0.00/30.20 | 7720/297.826 |
| diff | append | cold-manager | n=3 | 61.436 | 54.048–95.916 | 6144 | 162772.1 | 81/0 | 0.33/0.00 | 81/1.535 |
| diff | clustered | cold-manager | n=3 | 104.653 | 101.041–105.181 | 10465 | 95554.3 | 148/0 | 0.67/0.00 | 148/2.564 |
| diff | random | cold-manager | n=3 | 6394.631 | 5947.627–7422.212 | 639463 | 1563.8 | 8928/0 | 49.15/0.00 | 8928/156.369 |
| full_scan | append | cold-manager | n=1 | 5334.586 | 5334.586–5334.586 | 5335 | 187456.0 | 7719/0 | 30.20/0.00 | 7719/107.811 |
| get_cold | append | cold-manager | n=3 | 27663.844 | 27269.518–30343.241 | 2766384 | 361.5 | 40000/0 | 189.84/0.00 | 40000/820.964 |
| get_cold | clustered | cold-manager | n=3 | 27926.406 | 26741.088–30094.198 | 2792641 | 358.1 | 40000/0 | 221.93/0.00 | 40000/880.678 |
| get_cold | random | cold-manager | n=3 | 27639.292 | 27331.330–39148.086 | 2763929 | 361.8 | 40000/0 | 164.77/0.00 | 40000/773.580 |
| get_warm | append | warm-manager | n=3 | 7.518 | 7.351–7.676 | 752 | 1330104.2 | 0/0 | 0.00/0.00 | 0/0.000 |
| get_warm | clustered | warm-manager | n=3 | 7.883 | 7.800–8.148 | 788 | 1268626.3 | 0/0 | 0.00/0.00 | 0/0.000 |
| get_warm | random | warm-manager | n=3 | 18.117 | 16.586–19.086 | 1812 | 551964.0 | 0/0 | 0.00/0.00 | 0/0.000 |
| merge | append | cold-manager | n=3 | 92.326 | 87.084–118.019 | 4616 | 216624.6 | 78/70 | 0.34/0.32 | 154/5.279 |
| merge | clustered | cold-manager | n=3 | 8905.312 | 8639.937–17731.849 | 445266 | 2245.9 | 7786/7719 | 30.57/30.20 | 15511/458.803 |
| merge | random | cold-manager | n=3 | 11296.275 | 11176.447–12092.928 | 564814 | 1770.5 | 10583/7719 | 44.14/30.20 | 18308/505.496 |
| put | append | cold-manager | n=3 | 7.480 | 6.615–8.274 | 7480250 | 133.7 | 0/4 | 0.00/0.01 | 10/0.367 |
| put | clustered | cold-manager | n=3 | 9185.897 | 8831.096–9262.450 | 9185897125 | 0.1 | 7717/7719 | 30.19/30.20 | 15442/451.069 |
| put | random | cold-manager | n=3 | 9214.148 | 9018.826–9575.820 | 9214148500 | 0.1 | 7717/7719 | 30.19/30.20 | 15442/439.158 |
| query | append | cold-manager | n=3 | 57.433 | 55.004–62.928 | 5743 | 174115.0 | 86/0 | 0.31/0.00 | 86/1.296 |
| query | clustered | cold-manager | n=3 | 51.245 | 47.140–65.130 | 5124 | 195142.7 | 74/0 | 0.34/0.00 | 74/1.162 |
| query | random | cold-manager | n=3 | 2967.445 | 2945.449–3905.213 | 296745 | 3369.9 | 4498/0 | 24.52/0.00 | 4498/69.582 |
| scan | append | cold-manager | n=3 | 60.339 | 59.340–70.784 | 6034 | 165731.2 | 86/0 | 0.31/0.00 | 86/1.314 |
| scan | clustered | cold-manager | n=3 | 52.653 | 47.351–57.705 | 5265 | 189923.6 | 74/0 | 0.34/0.00 | 74/1.299 |

## 10,000,000 records

| Operation | Pattern | Cache | n | Median ms | Min–max ms | ns/op | ops/s | Nodes R/W | MiB R/W | PG calls / ms |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| batch | append | cold-manager | n=3 | 45.280 | 45.200–53.784 | 4528 | 220849.3 | 0/73 | 0.00/0.32 | 79/3.476 |
| batch | clustered | cold-manager | n=3 | 93279.580 | 93243.529–93875.441 | 9327958 | 107.2 | 76973/76974 | 301.95/301.95 | 153953/4745.690 |
| batch | random | cold-manager | n=3 | 91975.775 | 88694.809–95612.599 | 9197578 | 108.7 | 76973/76974 | 301.95/301.95 | 153953/4780.277 |
| build | base | cold-manager | n=1 | 33482.116 | 33482.116–33482.116 | 3348 | 298666.9 | 0/76974 | 0.00/301.95 | 76975/2819.836 |
| diff | append | cold-manager | n=3 | 53.221 | 52.657–66.594 | 5322 | 187896.6 | 77/0 | 0.33/0.00 | 77/1.555 |
| diff | clustered | cold-manager | n=3 | 110.245 | 101.854–121.058 | 11025 | 90706.7 | 138/0 | 0.66/0.00 | 138/2.843 |
| diff | random | cold-manager | n=3 | 13341.998 | 12939.177–13776.208 | 1334200 | 749.5 | 18848/0 | 132.48/0.00 | 18848/491.567 |
| full_scan | append | cold-manager | n=1 | 54362.828 | 54362.828–54362.828 | 5436 | 183949.2 | 76974/0 | 301.95/0.00 | 76974/1202.347 |
| get_cold | append | cold-manager | n=3 | 27334.515 | 26516.510–28687.396 | 2733451 | 365.8 | 40000/0 | 157.57/0.00 | 40000/729.965 |
| get_cold | clustered | cold-manager | n=3 | 28065.682 | 27216.431–28709.731 | 2806568 | 356.3 | 40000/0 | 233.30/0.00 | 40000/898.998 |
| get_cold | random | cold-manager | n=3 | 28479.946 | 27937.510–31929.476 | 2847995 | 351.1 | 40000/0 | 246.77/0.00 | 40000/981.555 |
| get_warm | append | warm-manager | n=3 | 7.671 | 7.276–7.686 | 767 | 1303540.1 | 0/0 | 0.00/0.00 | 0/0.000 |
| get_warm | clustered | warm-manager | n=3 | 8.107 | 7.942–12.496 | 811 | 1233559.0 | 0/0 | 0.00/0.00 | 0/0.000 |
| get_warm | random | warm-manager | n=3 | 26.862 | 25.997–27.298 | 2686 | 372276.0 | 0/0 | 0.00/0.00 | 0/0.000 |
| merge | append | cold-manager | n=3 | 111.573 | 109.751–113.497 | 5579 | 179255.4 | 102/94 | 0.34/0.31 | 202/5.012 |
| merge | clustered | cold-manager | n=3 | 87585.769 | 87045.346–91667.401 | 4379288 | 228.3 | 77043/76974 | 302.31/301.95 | 154023/4715.588 |
| merge | random | cold-manager | n=3 | 100710.980 | 96505.694–102536.402 | 5035549 | 198.6 | 85205/76974 | 357.31/301.95 | 162185/5344.175 |
| put | append | cold-manager | n=3 | 12.345 | 12.235–12.927 | 12345250 | 81.0 | 0/4 | 0.00/0.01 | 10/0.365 |
| put | clustered | cold-manager | n=3 | 89855.443 | 89146.471–92027.533 | 89855442834 | 0.0 | 76973/76974 | 301.95/301.95 | 153953/4492.081 |
| put | random | cold-manager | n=3 | 89286.083 | 87000.177–94887.124 | 89286083000 | 0.0 | 76973/76974 | 301.95/301.95 | 153953/4512.165 |
| query | append | cold-manager | n=3 | 54.109 | 50.718–54.995 | 5411 | 184812.4 | 71/0 | 0.30/0.00 | 71/1.373 |
| query | clustered | cold-manager | n=3 | 49.589 | 46.499–50.430 | 4959 | 201659.3 | 69/0 | 0.33/0.00 | 69/1.268 |
| query | random | cold-manager | n=3 | 6753.374 | 6592.719–6766.301 | 675337 | 1480.7 | 9397/0 | 66.56/0.00 | 9397/206.656 |
| scan | append | cold-manager | n=3 | 55.781 | 49.941–57.610 | 5578 | 179272.9 | 71/0 | 0.30/0.00 | 71/1.185 |
| scan | clustered | cold-manager | n=3 | 52.884 | 52.565–53.992 | 5288 | 189093.0 | 69/0 | 0.33/0.00 | 69/1.327 |

## Interpretation limits

- Results describe the recorded machine, Docker Desktop allocation, code revision, PostgreSQL defaults, and fixed 24-byte keys/27-byte values.
- `cold-manager` clears or recreates the decoded Prolly node cache; PostgreSQL and host OS caches are not forcibly dropped.
- The workload is serial and single-client. It does not measure connection-pool or concurrent transaction scaling.
- `query` means the public Prolly `get_many` API. Random-key range scans are intentionally not defined.
- Build and full scan have n=1 per size; other full-profile cells normally have n=3.
- Database-side statement time excludes client/runtime/tree processing and must not be compared as if it were end-to-end latency.
- Docker Desktop restarted once during the run. The fixture was rebuilt and the matrix resumed; this reset PostgreSQL/OS cache warmth for later repetitions.
- The resume binary changed only untimed fixture restoration and disk-guard ordering. Its hash is recorded separately in `resume-binary.sha256`; timed workload code was unchanged.
