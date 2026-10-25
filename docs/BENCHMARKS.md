# Benchmarks

Central index of all Lode benchmarks.

## Results

Results from two environments to contextualize the range of expected performance.

| Machine | CPU | Cores | Sampling | Notes |
|---------|-----|------:|----------|-------|
| Workstation | AMD Ryzen 9 5900XT | 16C/32T | `-count=10`, benchstat median | Bare metal, high single-thread clock |
| CI | AMD EPYC 7763 | 4 vCPU | `-count=10`, benchstat median | GitHub Actions shared runner |

Both: linux/amd64, Go 1.25, Docker-local S3 backends.

### In-memory

Workstation, v0.10.0 (`99cb592`, 2026-10-19), `-count=10`, benchstat medians.
The CI rows are historical values from the v0.8.0 run and are not comparable
with the v0.10.0 workstation rows (the same workstation benchmark moved from
135,400 to 183,200 ns/op between those revisions as the benchmark and the
write path changed); a cross-machine ratio needs both machines on one
revision, which the next CI benchmark run will provide.

| Benchmark | Machine | ns/op | B/op | allocs/op |
|-----------|---------|------:|-----:|----------:|
| `BenchmarkDataset_SequentialWrites` | Workstation | 183,200 ± 2% | 101,396 ± 1% | 760 |
| | CI (v0.8.0) | 177,600 ± 4% | 99,702 ± 0% | 680 |
| `BenchmarkDataset_SequentialWrites_StoreCallCount` | Workstation | 187,400 ± 1% | 104,960 ± 0% | 773 |
| | CI (v0.8.0) | 188,000 ± 3% | 101,881 ± 0% | 687 |
| `BenchmarkDataset_Read` | Workstation | 9,826 ± 3% | 8,573 ± 0% | 70 |
| `BenchmarkJSONLCodec_Decode/records=3` | Workstation | 3,728 ± 3% | 6,129 ± 0% | 70 |
| `BenchmarkJSONLCodec_Decode/records=1000` | Workstation | 1,145,000 ± 1% | 671,744 ± 0% | 23,000 |
| `BenchmarkJSONLCodec_Decode/records=100000` | Workstation | 120,900,000 ± 3% | 72,142,438 ± 0% | 2,300,000 |
| `BenchmarkDataset_StreamWrite` | Workstation | 13,230 ± 3% | 6,584 ± 0% | 33 |
| `BenchmarkVolume_ReadAt` | Workstation | 19,710 ± 2% | 9,056 ± 0% | 44 |
| `BenchmarkParquetCodec_Encode` | Workstation | 197,500 ± 6% | 530,842 ± 1% | 514 |
| `BenchmarkParquetCodec_Decode` | Workstation | 80,060 ± 5% | 89,119 ± 0% | 1,000 |
| `BenchmarkFindCoveringBlocks/blocks=10…10000` | Workstation | 94 – 109 ± 3% | 144 | 2 |
| `BenchmarkMergeBlocks/existing=10…10000/new=3` | Workstation | 354 – 198,000 ± 7% | 904 – 483,635 | 5 |

The v0.10.0 hygiene restructuring was measured against the pre-restructuring
code on the same machine (count=10): time geomean −2.4% with every Dataset
and Volume path within noise. Allocations per op, tag `v0.9.1` → v0.10.0:
`SequentialWrites` 720 → 760 (20 writes per op, so +2 per `Write`),
`StreamWrite` 30 → 33, `Read` 69 → 70, `Volume_ReadAt` 43 → 44 — the shared
commit-loop closure and the generic snapshot wrapper. Reproduce with
`go test -bench='Dataset_SequentialWrites$' -benchmem -run='^$' ./lode/` at
each revision; allocation counts are deterministic. The first cut of that restructuring had regressed
`Volume_ReadAt` to 126 allocs/op and `SequentialWrites` by +36%; both were
found by this comparison and fixed before merge, which is why the comparison
is part of the release checklist.

The `Read` and `JSONLCodec_Decode` rows were measured after the JSON Lines
decoder stopped allocating a 64 KiB buffer for every file it decodes
(`-count=8`). A read of a three-record file then allocates 88 percent fewer
bytes and takes 42 percent less time; decoding files of one thousand and one
hundred thousand records takes the same time as before, within noise.

The buffer grows by doubling from four kibibytes to the longest line, so a
file whose lines are longer than about thirty-two kibibytes allocates the
intermediate buffers that the former fixed buffer avoided.
`BenchmarkJSONLCodec_DecodeLineLength` measures that cost on files of ten
records: with lines of one kibibyte the decoder allocates 76 percent fewer
bytes and takes 26 percent less time than before; with lines of sixteen
kibibytes it is unchanged; with lines of forty-eight and two hundred
fifty-six kibibytes it allocates 11 and 2 percent more bytes, in the same
time. Avoiding both costs would require a buffer pool shared across decodes,
which would be package-level state.

## Allocation Gates

`TestAllocationGates` in `lode/alloc_test.go` and `lode/volume/alloc_test.go`
holds each hot path of the Dataset and Volume APIs to a ceiling on the
allocations per operation and on the bytes they allocate, measured on the
in-memory store with the head known. Both figures are deterministic for a
given toolchain, so the gates run in every `go test` without the race
detector, which adds allocations of its own. The byte ceiling catches a
single oversized allocation, such as a fixed buffer, which the allocation
count does not; each byte ceiling is the measured figure rounded up to the
next 64 bytes, because map growth in the in-memory store varies it by a few
bytes. The ceilings below were measured with Go 1.27.3; the benchmark
figures in the other sections were measured with Go 1.25.

| Gate | Allocations | Bytes |
|------|------------:|------:|
| Write of three records | 53 | 6,656 |
| Read of three records | 60 | 10,176 |
| Blob StreamWrite of one kibibyte | 31 | 7,232 |
| Record StreamWrite of three records | 63 | 7,296 |
| JSONL Decode of three records | 33 | 5,504 |
| Volume ReadAt within one of ten blocks | 41 | 10,816 |
| Volume Commit of one block onto a small volume | 136 | 252,032 |

A change that exceeds a ceiling fails the test and must justify the increase
in its pull request before the ceiling is raised. A change that reduces a
figure lowers the ceiling to the new figure in the same pull request, so the
gates never retain unused headroom. Manifest format 1.1.0 records the
codec description, partition keys, and path names in every manifest, which
raised the byte ceilings of the four Dataset gates, and one allocation of
the Read gate, by the size of the larger manifest. The typed handles of
V1_API_DECISIONS D11 lowered the Write, Read, and Blob StreamWrite gates:
metadata is persisted as written rather than decoded into a map, and the
JSONL codec decodes a map record through an interface value, which costs
jsoniter three fewer allocations per record than its map decoder. The
Record StreamWrite gate rose by two allocations, the cost of the iterator
protocol (`iter.Seq2`): the caller's iterator closure and the loop body
that ranges over it. A toolchain or dependency upgrade that
moves the figures resets the ceilings in the upgrade's pull request.
`task gates` runs each gate three times and reports the ceilings that each
run calls for, with the byte figure already rounded. A byte figure near a
multiple of 64 can round to either side, so a ceiling is reset to the
highest figure that a gate reports.

Two gates vary between measurements. Under Go 1.27, `encoding/json` uses
its v2 implementation, and in a few percent of measurements the Dataset
Read and Volume ReadAt paths allocate two more objects per run (and the
corresponding bytes). The variation occurs between repetitions within one
test process, so `go test -count=120` observes it; with
`GOEXPERIMENT=nojsonv2` every figure is identical across the 120
repetitions, so the variation lies in the standard library, not in Lode.
The ceilings of those two gates are therefore the higher figures; `task
gates` reports the lower ones in most repetitions, and a ceiling reset uses
the highest figure observed across `go test -count=120`.

### S3 (Docker-local)

| Benchmark | Machine | ns/op | B/op | allocs/op | S3 requests/op |
|-----------|---------|------:|-----:|----------:|---------------:|
| `BenchmarkS3_FirstWrite/LocalStack` | Workstation | 18,980,000 ± 7% | 734,748 ± 1% | 7,540 | 13 |
| `BenchmarkS3_FirstWrite/RustFS` | Workstation | 104,200,000 ± 5% | 722,866 ± 2% | 7,352 | 13 |
| `BenchmarkS3_SteadyWrite/LocalStack` | Workstation | 8,175,000 ± 20% | 307,848 ± 1% | 2,915 | 5 |
| `BenchmarkS3_SteadyWrite/RustFS` | Workstation | 36,770,000 ± 3% | 305,342 ± 2% | 2,888 | 5 |

The figures are medians of ten runs on an AMD Ryzen 9 5900XT, measured on
Lode v0.12.0 with Go 1.27.3. Neither benchmark has a CI figure yet.

Both benchmarks write three records and read them back in each iteration,
and each reports its S3 requests per iteration beside its allocations.
Nearly all allocations occur in the AWS SDK, at about five hundred and fifty
per request, so the allocation count follows the request count. The
request count does not vary with the machine, so a change in it identifies
a change in Lode's protocol, while the allocation count also moves with the
toolchain and the SDK.

- `BenchmarkS3_FirstWrite` opens a new dataset in each iteration, so it
  measures a dataset's first commit. Opening the dataset reads the pointer
  and, finding none, lists the snapshots and the dataset's directory to
  check the writer's settings and to detect a history under another layout
  (CONTRACT_WRITE_API §Settings). The commit writes the data object,
  repeats that check, stages a copy of the manifest so that a later commit
  can adopt an abandoned first commit (CONTRACT_CORE §Head Pointer), creates
  the pointer, writes the manifest, and deletes the staged copy. The read
  takes two Gets. The total is thirteen requests.
- `BenchmarkS3_SteadyWrite` writes through one handle whose dataset already
  has a commit, which is the common case. Each commit writes the data
  object, replaces the pointer, and writes the manifest; the adapter knows
  the pointer's version from its own last replacement, so it does not read
  the pointer first. The read takes two Gets, for five requests in all.

The single benchmark that these replace, `BenchmarkS3_WriteRoundTrip`,
measured only the first commit. Its allocations rose from 2,970 in
February to 4,904 when the persistent `latest` pointer (#127) and CAS
pointer updates (#135) added requests, and fell to 4,261 when the adapter
stopped reading a pointer whose version it knew (#246). The staged first
commit and the settings checks of v0.11.0 and v0.12.0 then raised a first
commit to thirteen requests, which is the figure above.

> **Note:** S3 numbers reflect Docker-local round-trip latency, not production S3.
> The `SequentialWrites` benchmarks inject 1 ms of latency on `List` and `Get`
> (`latencyStore`); the warm head-pointer path issues neither per write, so the
> figure measures Lode's own work plus `Put`/CAS, not the simulated store.
> These results are informational — use them for relative comparison, not absolute targets.
>
> Allocation counts are stable across machines. Latency varies with CPU,
> Docker runtime, and scheduler noise, so absolute S3 latency depends on
> the container environment, not the library.

## Benchmark inventory

| Benchmark | Location | Backend | Status |
|-----------|----------|---------|--------|
| `BenchmarkDataset_SequentialWrites` | `lode/dataset_bench_test.go` | In-memory (latency-injected) | Done |
| `BenchmarkDataset_SequentialWrites_StoreCallCount` | `lode/dataset_bench_test.go` | In-memory (fault store) | Done |
| `BenchmarkS3_FirstWrite/LocalStack`, `BenchmarkS3_FirstWrite/RustFS` | `lode/s3/bench_integration_test.go` | LocalStack, RustFS | Done |
| `BenchmarkS3_SteadyWrite/LocalStack`, `BenchmarkS3_SteadyWrite/RustFS` | `lode/s3/bench_integration_test.go` | LocalStack, RustFS | Done |
| `BenchmarkDataset_Read` | `lode/bench_test.go` | In-memory | Done |
| `BenchmarkDataset_StreamWrite` | `lode/bench_test.go` | In-memory | Done |
| `BenchmarkVolume_ReadAt` | `lode/volume/bench_test.go` | In-memory (10 committed blocks) | Done |
| `BenchmarkParquetCodec_Encode` | `lode/bench_test.go` | In-memory | Done |
| `BenchmarkParquetCodec_Decode` | `lode/bench_test.go` | In-memory | Done |
| `BenchmarkJSONLCodec_Decode` | `lode/bench_test.go` | In-memory (three, one thousand, and one hundred thousand records) | Done |
| `BenchmarkJSONLCodec_DecodeLineLength` | `lode/bench_test.go` | In-memory (lines of 1, 16, 48, and 256 KiB) | Done |
| `BenchmarkFindCoveringBlocks` | `lode/volume/volume_test.go` | In-memory | Done |
| `BenchmarkMergeBlocks` | `lode/volume/volume_test.go` | In-memory | Done |

### Planned

| Benchmark | Backend | Notes |
|-----------|---------|-------|
| Streaming record write round-trip | S3 (LocalStack/RustFS) | `Records.StreamWrite` path |
| Compressed write round-trip | S3 (LocalStack/RustFS) | gzip/zstd codec variants |
| Volume commit round-trip | S3 (LocalStack/RustFS) | Multi-block Volume write path |

## Running benchmarks

`task bench` runs **all** benchmarks — in-memory and S3 integration:

```bash
task s3:up      # start LocalStack + RustFS
task bench      # run all benchmarks
task s3:down    # stop services
```

In-memory benchmarks always run. S3 benchmarks require services to be up first.

### Statistical comparison

`task bench:stat` runs all benchmarks with `-count=10` and pipes results through
[benchstat](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat) for statistical
summaries. Raw output is saved to `bench-inmemory.txt` and `bench-s3.txt`.

```bash
task s3:up          # start LocalStack + RustFS
task bench:stat     # -count=10, benchstat summary
task s3:down        # stop services
```

To compare two runs (e.g. before and after a change):

```bash
# Save baseline
mv bench-inmemory.txt baseline-inmemory.txt

# Make changes, re-run
task bench:stat

# Compare
task bench:compare -- baseline-inmemory.txt bench-inmemory.txt
```

> **Note:** benchstat requires multiple samples (`-count=10`) to produce
> meaningful confidence intervals. Single-sample comparisons will show `~`
> (no significant difference) for most benchmarks.

### Manual invocation

```bash
# In-memory only
go test -bench=. -benchmem -run=^$ ./lode/...

# S3 benchmarks (with services running)
go test -bench=. -benchmem -run=^$ ./lode/s3/... -integration
```
