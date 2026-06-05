# Merodis

**Merodis** is an embeddable C++ library that implements the Redis data-structure API on top of LevelDB or RocksDB. It gives you familiar Redis commands (`Get`, `LPush`, `ZAdd`, …) backed by a persistent, LSM-tree storage engine — no server process required.

## Features

- **Redis-compatible API** — String, List, Hash, Set, and Sorted Set (ZSet) commands
- **Pluggable storage engine** — LevelDB (default) or RocksDB, selected at build time
- **Swappable implementations** — per-type impl enums let you choose storage layout (e.g. `kStringTypedImpl` vs `kStringBasicImpl`)
- **C++17, header-only public interface** — one `#include <merodis/merodis.h>` and link against `libmerodis`
- **Comprehensive test suite** (GoogleTest) and **micro-benchmarks** (Google Benchmark) included

## Building

Prerequisites: CMake ≥ 3.21, a C++17 compiler, and the chosen engine's submodule.

```bash
git clone --recurse-submodules https://github.com/revectores/merodis
cd merodis
cmake -B build                         # LevelDB (default)
cmake --build build -j$(nproc)
```

### Build options

| Option | Default | Description |
|---|---|---|
| `ENGINE` | `leveldb` | Storage engine: `leveldb` or `rocksdb` |
| `MERODIS_BUILD_TESTS` | `ON` | Build the GoogleTest suite |
| `MERODIS_BUILD_BENCHMARKS` | `ON` | Build the Google Benchmark suite |

```bash
# Use RocksDB instead
cmake -B build -DENGINE=rocksdb
cmake --build build -j$(nproc)

# Library only (no tests or benchmarks)
cmake -B build -DMERODIS_BUILD_TESTS=OFF -DMERODIS_BUILD_BENCHMARKS=OFF
cmake --build build -j$(nproc)
```

## Usage

```cpp
#include <merodis/merodis.h>

merodis::Merodis db;
merodis::Options opts;
opts.create_if_missing = true;

db.Open(opts, "/tmp/mydb");

// Strings
db.Set("hello", "world");
std::string val;
db.Get("hello", &val);  // val == "world"

// Lists
db.RPush("mylist", "a");
db.RPush("mylist", "b");
uint64_t len;
db.LLen("mylist", &len);  // len == 2

// Hashes
uint64_t count;
db.HSet("myhash", "field", "value", &count);

// Sets
db.SAdd("myset", "member", &count);

// Sorted Sets
db.ZAdd("ranking", {{"alice", 100}}, &count);
```

Every method returns a `merodis::Status`. Check `status.ok()` or `status.ToString()` for errors.

## Supported commands

### String
`Get` · `Set` · `Incr` · `IncrBy` · `Decr` · `DecrBy`

### List
`LLen` · `LIndex` · `LPos` · `LRange` · `LSet` · `LInsert` · `LRem` · `LMove` · `LTrim`  
`LPush` · `LPushX` · `LPop` · `RPush` · `RPushX` · `RPop`

### Hash
`HLen` · `HGet` · `HMGet` · `HGetAll` · `HKeys` · `HVals` · `HExists`  
`HSet` · `HDel`

### Set
`SCard` · `SIsMember` · `SMIsMember` · `SMembers` · `SRandMember`  
`SAdd` · `SRem` · `SPop` · `SMove`  
`SUnion` · `SInter` · `SDiff` · `SUnionStore` · `SInterStore` · `SDiffStore`

### Sorted Set (ZSet)
`ZCard` · `ZScore` · `ZMScore` · `ZRank` · `ZRevRank` · `ZCount` · `ZLexCount`  
`ZRange` · `ZRangeByScore` · `ZRangeByLex` · `ZRevRange` · `ZRevRangeByScore` · `ZRevRangeByLex`  
`ZRangeWithScores` · `ZRangeByScoreWithScores` · `ZRangeByLexWithScores` (and `ZRev*` variants)  
`ZAdd` · `ZRem` · `ZPopMin` · `ZPopMax`  
`ZRemRangeByRank` · `ZRemRangeByScore` · `ZRemRangeByLex`  
`ZUnion` · `ZInter` · `ZDiff` · `ZUnionStore` · `ZInterStore` · `ZDiffStore` (with `WithScores` variants)

## Running tests

```bash
cd build && ctest --output-on-failure
# or run directly
./test_merodis
```

## Running benchmarks

```bash
cd build
./merodis_benchmark --benchmark_filter=.

# Use the helper script to save results as JSON
cd ..
python3 bench.py
```

Results are written to `bench_results/` as individual JSON files per benchmark case.
