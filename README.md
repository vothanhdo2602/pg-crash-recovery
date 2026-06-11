# pg-crash-recovery

Repairs a **PostgreSQL 12–17** cluster that cannot start due to a missing or corrupt
checkpoint WAL record — without using `pg_resetwal`.

```
PANIC: could not locate a valid checkpoint record at 0/6DCF0C8
```

If you are seeing this error, this tool is for you.

---

## The problem

PostgreSQL stores its latest checkpoint location in `global/pg_control`. On startup
it reads that location, opens the matching WAL segment, and reads the checkpoint
record. If the WAL segment has been recycled, deleted, or the `pg_control` pointer
is stale, startup fails with a `PANIC` even though all your data is intact on disk.

The standard fix is `pg_resetwal -f`, which creates a fresh WAL and lets the
database start — but it discards any WAL that was never checkpointed to heap files,
causing **data loss** for databases that were not cleanly shut down.

---

## How this tool works

1. Scans every WAL segment in `pg_wal/` for `CHECKPOINT_ONLINE` /
   `CHECKPOINT_SHUTDOWN` records by reading the binary WAL format directly (no
   external tools required).
2. Selects the checkpoint with the highest LSN — the most recent consistent state
   captured in the WAL.
3. Patches `global/pg_control`:
   - Sets `checkPoint` to the found checkpoint LSN
   - Copies all checkpoint fields into `checkPointCopy`
   - Sets cluster state to **in production** (triggers crash recovery on next start)
   - Recalculates the CRC-32C over the entire control file
4. Backs up the original `pg_control` to `pg_control.orig` before writing.

On next start PostgreSQL enters crash recovery, replays WAL forward from the
checkpoint's REDO point, and promotes. All transactions recorded in the available
WAL are recovered.

---

## Requirements

- Go 1.21+
- PostgreSQL **12–17** data directory (64-bit Linux, little-endian)
- At least one WAL segment in `pg_wal/` that contains a valid checkpoint record

> PostgreSQL 11 and earlier used a different `CheckPoint` struct layout
> (`nextXidEpoch` + `nextXid` as two separate 32-bit fields) and are not supported.

---

## Build

```bash
git clone https://github.com/vothanhdo2602/pg-crash-recovery
cd pg-crash-recovery
go build -o pg-crash-recovery .
```

Or install directly:

```bash
go install github.com/vothanhdo2602/pg-crash-recovery@latest
```

---

## Usage

```bash
# Preview what would change — no files are written
./pg-crash-recovery --dry-run /path/to/data-dir

# Apply the fix
./pg-crash-recovery /path/to/data-dir
```

Then start PostgreSQL normally — no `recovery.signal` needed:

```bash
# Remove recovery.signal if you created one earlier
rm -f /path/to/data-dir/recovery.signal

pg_ctl start -D /path/to/data-dir -l /tmp/pg_recovery.log
tail -f /tmp/pg_recovery.log
```

You should see:

```
LOG:  database system was not properly shut down; automatic recovery in progress
LOG:  redo starts at 19/A26EB6B0
LOG:  redo done at 19/A28257E0
LOG:  database system is ready to accept connections
```

**Dump your data immediately after recovery:**

```bash
pg_dumpall -p 5432 > /safe/location/recovered.sql
```

---

## Example output

```
pg-crash-recovery
─────────────────────────────────────────────────────
Current state     : shut down
Current checkpoint: 0/06DCF0C8

Scanning /tmp/data-dev-bk/pg_wal ...
Found   : CHECKPOINT_ONLINE  lsn=19/A277BE70  redo=19/A26EB6B0  xid=38644005  oid=247048

Patching pg_control:
  state     : shut down → in production
  checkpoint: 0/06DCF0C8 → 19/A277BE70
  redo start: 19/A26EB6B0
  backup  → /tmp/data-dev-bk/global/pg_control.orig
  new CRC   A3F21C08

─────────────────────────────────────────────────────
Done. Next steps:
  1. Remove recovery.signal if present:
       rm -f /tmp/data-dev-bk/recovery.signal
  2. Start PostgreSQL (runs crash recovery automatically):
       pg_ctl start -D /tmp/data-dev-bk
  3. Verify data and dump immediately:
       pg_dumpall > /safe/path/recovered.sql
```

---

## When this tool cannot help

| Situation | Outcome |
|---|---|
| WAL files exist and contain a checkpoint | **Recovered** — this is the normal use case |
| WAL files exist but contain no checkpoints | Cannot recover — no consistent state to resume from |
| `pg_wal/` is empty or all segments are pre-allocated zeros | Cannot recover — `pg_resetwal` or a backup restore is required |
| PostgreSQL version is 11 or earlier | Not supported — different `CheckPoint` struct layout |

---

## Why not just use `pg_resetwal`?

`pg_resetwal` is safe **only** when the cluster state is `shut down` (clean
shutdown). If the database crashed mid-operation, some committed transactions exist
only in WAL and have not been flushed to heap files yet. `pg_resetwal` discards
that WAL, causing permanent data loss for those transactions.

This tool avoids that by pointing PostgreSQL at a real checkpoint already recorded
in the WAL, letting crash recovery replay the remaining changes safely.

---

## Technical reference

### pg_control binary layout (PG12–PG17, 64-bit Linux)

| Offset | Field | Type |
|---|---|---|
| 0 | `system_identifier` | uint64 |
| 8 | `pg_control_version` | uint32 |
| 16 | `state` (DBState) | uint32 |
| 32 | `checkPoint` | uint64 (XLogRecPtr) |
| 40 | `checkPointCopy` | CheckPoint struct (88 bytes) |
| varies | `crc` | uint32 (CRC-32C / Castagnoli) — auto-detected |

### CheckPoint struct layout (PG12–PG17, offset relative to start of struct)

| Offset | Field | Type |
|---|---|---|
| 0 | `redo` | uint64 |
| 8 | `ThisTimeLineID` | uint32 |
| 12 | `PrevTimeLineID` | uint32 |
| 16 | `fullPageWrites` | uint8 |
| 17 | `wal_level` | uint8 — meaningful in PG17+; padding (0) in PG12–PG16 |
| 24 | `nextXid` (FullTransactionId) | uint64 |
| 32 | `nextOid` | uint32 |
| 36 | `nextMulti` | uint32 |
| 40 | `nextMultiOffset` | uint32 |
| 44 | `oldestXid` | uint32 |
| 48 | `oldestXidDB` | uint32 |
| 52 | `oldestMulti` | uint32 |
| 56 | `oldestMultiDB` | uint32 |
| 64 | `time` | int64 |
| 72 | `oldestCommitTsXid` | uint32 |
| 76 | `newestCommitTsXid` | uint32 |
| 80 | `oldestActiveXid` | uint32 |

### WAL checkpoint record layout (PG12–PG17)

```
XLogRecord        (24 bytes)  xl_rmid=0  xl_info=0x10 (ONLINE) or 0x00 (SHUTDOWN)
DataHdrShort[0]   = 0xFF      (XLR_BLOCK_ID_DATA_SHORT)
DataHdrShort[1]   = 0x58      (88 = sizeof CheckPoint)
CheckPoint struct (88 bytes)
─────────────────────────────
Total             114 bytes
```
