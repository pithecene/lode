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
bytes.

| Gate | Allocations | Bytes |
|------|------------:|------:|
| Write of three records | 61 | 6,208 |
| Read of three records | 70 | 8,512 |
| StreamWrite of one kibibyte | 33 | 6,784 |
| StreamWriteRecords of three records | 63 | 6,208 |
| JSONL Decode of three records | 35 | 5,568 |
| Volume ReadAt within one of ten blocks | 44 | 8,960 |
| Volume Commit of one block onto a small volume | 140 | 334,784 |

A change that exceeds a ceiling fails the test and must justify the increase
in its pull request before the ceiling is raised. A change that reduces a
figure lowers the ceiling to the new figure in the same pull request, so the
gates never retain unused headroom. A toolchain or dependency upgrade that
moves the figures resets the ceilings in the upgrade's pull request.

### S3 (Docker-local)

| Benchmark | Machine | ns/op | B/op | allocs/op |
|-----------|---------|------:|-----:|----------:|
| `BenchmarkS3_WriteRoundTrip/LocalStack` | Workstation | 10,190,000 ± 5% | 471,757 ± 1% | 4,261 |
| | CI (v0.10.0) | 8,247,000 ± 3% | 355,202 ± 1% | 2,965 |
| `BenchmarkS3_WriteRoundTrip/RustFS` | Workstation | 33,280,000 ± 5% | 469,504 ± 1% | 4,157 |
| | CI | Not yet measured | | |

The workstation figures are medians of ten runs on an AMD Ryzen 9 5900XT.
The CI figure for LocalStack is an older baseline, and RustFS has no CI
figure yet.

Each iteration opens a new dataset, so the benchmark measures a dataset's
first commit and one read: seven S3 requests (a pointer Get that finds no
pointer, a List, the data and manifest writes, the pointer write, and two
Gets for the read). Nearly all allocations occur in the AWS SDK, at about six
hundred per request, so the allocation count follows the request count.
It rose from 2,970 in February to 4,904 when the persistent `latest`
pointer (#127) and CAS pointer updates (#135) added requests to this path;
`CompareAndSwap` then read the pointer before every conditional write. The
S3 adapter now omits that read when it creates the pointer and when it
replaces a pointer that it last replaced itself. This change reduced the
benchmark from eight requests to seven, which lowered its allocations by
13 percent and, on LocalStack, its duration by 9 percent. Each commit after
the second through the same store issues three requests instead of four;
the benchmark does not measure such commits.

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
| `BenchmarkS3_WriteRoundTrip/LocalStack` | `lode/s3/bench_integration_test.go` | LocalStack | Done |
| `BenchmarkS3_WriteRoundTrip/RustFS` | `lode/s3/bench_integration_test.go` | RustFS | Done |
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
| Streaming record write round-trip | S3 (LocalStack/RustFS) | `StreamWriteRecords` path |
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
