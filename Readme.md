# Bus-Based Snooping Cache Coherence Simulator
**Language:** C++17
**Source file:** `coherence_simulator.cpp`

## 1. What it does

Simulates a bus-based shared-memory multiprocessor. It reads a list of
processor memory operations, runs them through a snooping coherence protocol,
and prints:

1. A step-by-step **trace table** (memory and every cache after each operation).
2. A **final summary** with execution statistics.

Supported protocols:

| # | Protocol | States |
|---|----------|--------|
| 1 | Write-Invalidate, Write-Through | VALID, INV |
| 2 | Write-Invalidate, Write-Back (Ownership) | RO, RW, INV |
| 3 | Goodman's Write-Once | VALID, RES, DIRTY, INV |

The simulator is dynamic: any number of processors, any number of variables,
and any access stream.

## 2. Build

Requires a C++17 compiler (g++, clang++, or MSVC).

```bash
g++ -std=c++17 -Wall -Wextra -o simulator coherence_simulator.cpp
```

## 3. Run

```bash
./simulator          # Linux / macOS
simulator.exe        # Windows
```

The program asks for a protocol:

```
Choose Protocol
WRITE_INVALIDATE_WRITE_THROUGH (1)
| WRITE_INVALIDATE_WRITE_BACK (2)
|WRITE_ONCE (3)
```

Type `1`, `2`, or `3` and press Enter. The program then runs **both** test case
files with that protocol.

### Input file paths

The two input files are set in the source, inside `class CoherenceSimulator`:

```cpp
string TestCase1 = "C:\\Users\\...\\TestCase1.txt";
string TestCase2 = "C:\\Users\\...\\TestCase2.txt";
```

Change these two strings to the location of your files before building.
There are no command-line arguments.

## 4. Input file format

```
MEMORY: X=10, Z=80
PROCESSORS: P, Q, R

P: Read(X)
Q: Write(X, 35)
R: Replace(X)
```

| Line | Meaning |
|------|---------|
| `MEMORY:` | Initial variables and values (any number). |
| `PROCESSORS:` | Processor names (any number, any names). Each name gets its own cache. |
| `<Proc>: Read(Var)` | Read a variable. |
| `<Proc>: Write(Var, Val)` | Write an integer value (negative values allowed). |
| `<Proc>: Replace(Var)` | Evict the block from that processor's cache. |

Parser rules:
- Blank lines and lines starting with `#` are skipped.
- Extra spaces and Windows line endings (`\r\n`) are accepted.
- A `PROTOCOL:` line is accepted and ignored (the menu chooses the protocol).
- If `PROCESSORS:` is missing, processors are taken from the operations, in
  first-seen order.
- A variable used in an operation but missing from `MEMORY:` is added with
  value 0, and a warning is printed.
- An operation from a processor not in `PROCESSORS:` prints
  `Unknown processor` and is skipped.
- Malformed lines print a message and are skipped.

## 5. Output

### Trace table

One row per operation, plus row 0 for the initial state.

```
Serial | Event         | Operation  | Memory | P Cache    | Q Cache
       |               |            | X      | Val/St(X)  | Val/St(X)
0      | Initial State | -          | 5      | - -        | - -
1      | P Read(X)     | Read-Miss  | 5      | 5 RO       | - -
2      | Q Read(X)     | Read-Miss  | 5      | 5 RO       | 5 RO
3      | Q Write(X)    | Write-Hit  | 5      | 5 INV      | 10 RW
```

- **Operation** is one of: `Read-Hit`, `Read-Miss`, `Write-Hit`, `Write-Miss`,
  `Replace`, `Replace-Miss`.
- A cache cell is `value STATE`, or `- -` if the block is not in that cache.
- Column widths are computed from the data, so the table stays aligned.

### Final summary

- Final memory values.
- Final contents and states of every cache.
- Statistics: read hits and misses, write hits and misses, invalidations,
  memory write-backs.

## 6. Protocol behavior (as implemented)

### Protocol 1: Write-Invalidate, Write-Through
- **Read miss:** the block is loaded from memory as `VALID`.
- **Write (hit or miss):** other copies are invalidated, memory is updated,
  and the writer holds the block as `VALID`.
- **Replace:** no write-back.

### Protocol 2: Write-Invalidate, Write-Back (Ownership)
- **Read miss:** memory supplies the block and the reader gets `RO`. If another
  cache holds it in `RW`, that owner supplies the data, memory is updated
  (1 write-back), and both caches become `RO`.
- **Write hit in RW:** local update, no bus traffic.
- **Write in RO, or write miss:** other copies are invalidated and the writer
  becomes `RW`. Memory is not updated.
- **Replace:** a block in `RW` is written back to memory.

### Protocol 3: Goodman's Write-Once
- **Read miss:** memory supplies the block as `VALID`. A `DIRTY` owner supplies
  the data and updates memory (1 write-back), and both caches become `VALID`.
  A `RES` owner becomes `VALID`.
- **First write:** write-through to memory, other copies invalidated, state `RES`.
- **Subsequent write** (block already `RES` or `DIRTY`, or another cache holds
  it as `RES` or `DIRTY`): other copies invalidated, no memory update, state
  `DIRTY`.
- **Replace:** a `DIRTY` block is written back to memory.

## 7. Code structure

| Class / function | Responsibility |
|------------------|----------------|
| `CacheBlock` | One variable: tag, value, state. |
| `Cache` | Private cache of one processor. `snoop()` reacts to bus traffic and changes its own states. |
| `SharedBus` | Broadcasts reads, invalidates, and updates to all caches. Collects replies and writes flushed data to memory. It never edits cache internals. |
| `MainMemory` | Global shared memory (map of variable to value). |
| `CoherenceSimulator` | Controller: `load_input()`, `run_all()`, `do_read()`, `do_write()`, `do_replace()`, `print_trace_table()`, `print_summary()`. |

Bus transactions are `BUS_READ`, `BUS_INVALIDATE`, and `BUS_UPDATE`.
`broadcast_update()` exists for the optional write-update protocols and is not
used by the three required protocols.

## 8. Test cases

**Test Case 1:** processors `P, Q`, variable `X=5`

```
MEMORY: X=5
PROCESSORS: P, Q
P: Read(X)
Q: Read(X)
Q: Write(X, 10)
Q: Read(X)
Q: Write(X, 15)
P: Write(X, 20)
Q: Read(X)
```

**Test Case 2:** processors `P, Q, R`, variables `X=10, Z=80`

```
MEMORY: X=10, Z=80
PROCESSORS: P, Q, R
P: Read(X)
Q: Read(X)
R: Read(X)
P: Write(X, 35)
P: Read(Z)
Q: Read(X)
R: Write(X, 15)
P: Write(Z, 81)
```

## 9. Notes and limitations

- The protocol is chosen from the menu, not from the `PROTOCOL:` line.
- Input paths are set in the source code, not on the command line.
- The invalidation counter counts the **copies invalidated**, not the number of
  bus commands. A write to a block with no other copies adds 0.
- In Protocol 2, a write miss while another cache holds the block in `RW` does
  not count a write-back; that owner's old value is simply invalidated.
- In Protocol 3, a write miss while another cache holds the block as `RES` or
  `DIRTY` goes straight to `DIRTY` with no write-through.