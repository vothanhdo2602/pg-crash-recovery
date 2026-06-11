// pg-crash-recovery repairs a PostgreSQL cluster that cannot start due to a
// missing or corrupt checkpoint WAL record, without using pg_resetwal.
//
// Supported versions: PostgreSQL 12–17 (64-bit Linux, little-endian)
//
// Usage:
//
//	pg-crash-recovery [--dry-run] <data-directory>
//
// After running, start PostgreSQL normally — no recovery.signal needed:
//
//	pg_ctl start -D <data-directory>
package main

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// ── Stable constants (all supported versions) ────────────────────────────────

const (
	xlogBlcksz      = 8192
	maxAlign        = 8
	xlrLongHdrFlag  = 0x0002
	shortPageHdrSz  = 20
	longPageHdrSz   = 40
	xlogRecordHdrSz = 24

	rmXlogID               = 0
	xlogCheckpointShutdown = 0x00
	xlogCheckpointOnline   = 0x10
	xlrBlockIDDataShort    = 255

	dbShutdowned   = 1
	dbInProduction = 6

	pgControlFileSize = 8192

	// pg_control field offsets — stable across PG12–PG17 on 64-bit Linux.
	// New fields added between versions appear later in the struct, after checkPointCopy.
	offPGCtrlVer     = 8
	offState         = 16
	offCheckPoint    = 32
	offCheckPointCopy = 40

	// CheckPoint struct size for PG12+ (FullTransactionId era).
	// Both PG12–PG16 and PG17 use 88 bytes; the only difference is that
	// byte 17 is wal_level in PG17 vs. padding in PG12–PG16.
	checkpointDataSize = 88

	// Offsets within CheckPoint struct (relative to offCheckPointCopy)
	cpRedo              = 0
	cpThisTLI           = 8
	cpPrevTLI           = 12
	cpFullPageWrites    = 16
	cpWalLevel          = 17 // meaningful in PG17+; padding (0) in PG12–PG16
	cpNextXid           = 24 // FullTransactionId (uint64) — PG12+
	cpNextOid           = 32
	cpNextMulti         = 36
	cpNextMultiOffset   = 40
	cpOldestXid         = 44
	cpOldestXidDB       = 48
	cpOldestMulti       = 52
	cpOldestMultiDB     = 56
	cpTime              = 64 // pg_time_t; offsets 60-63 are padding
	cpOldestCommitTsXid = 72
	cpNewestCommitTsXid = 76
	cpOldestActiveXid   = 80
)

// minPGCtrlVer is the lowest pg_control version we support (PG12).
// PG11 and earlier used a different CheckPoint layout (separate nextXidEpoch field).
const minPGCtrlVer = 1201

// ── Cluster probe ─────────────────────────────────────────────────────────────

// clusterConfig holds values auto-detected from the data directory.
type clusterConfig struct {
	pgCtrlVer uint32
	xlogMagic uint16 // detected from first WAL file
	crcOffset int    // offset of CRC field in pg_control, detected dynamically
}

func (c *clusterConfig) majorVersion() int { return int(c.pgCtrlVer / 100) }

// probeCluster reads pg_control and the WAL directory to build a clusterConfig.
func probeCluster(dataDir string) (*clusterConfig, error) {
	ctlPath := filepath.Join(dataDir, "global", "pg_control")
	data, err := os.ReadFile(ctlPath)
	if err != nil {
		return nil, fmt.Errorf("cannot read pg_control: %w", err)
	}
	if len(data) != pgControlFileSize {
		return nil, fmt.Errorf("pg_control size %d (expected %d)", len(data), pgControlFileSize)
	}

	ver := binary.LittleEndian.Uint32(data[offPGCtrlVer:])
	if ver < minPGCtrlVer {
		return nil, fmt.Errorf(
			"pg_control version %d (PG%d) — this tool supports PG12+ (version ≥ %d)",
			ver, ver/100, minPGCtrlVer)
	}

	crcOff, err := findCRCOffset(data)
	if err != nil {
		return nil, fmt.Errorf("cannot locate CRC in pg_control: %w", err)
	}

	magic, err := detectXlogMagic(filepath.Join(dataDir, "pg_wal"))
	if err != nil {
		return nil, fmt.Errorf("cannot detect WAL page magic: %w", err)
	}

	return &clusterConfig{
		pgCtrlVer: ver,
		xlogMagic: magic,
		crcOffset: crcOff,
	}, nil
}

// findCRCOffset locates the CRC field in pg_control by trying every 4-byte
// aligned position and checking whether CRC-32C of the preceding bytes matches
// the stored value. This works for any PostgreSQL version without hardcoding
// struct sizes.
func findCRCOffset(data []byte) (int, error) {
	for off := 128; off <= pgControlFileSize-4; off += 4 {
		stored := binary.LittleEndian.Uint32(data[off:])
		if stored == 0 {
			continue // skip zero padding at end of file
		}
		if crc32c(data[:off]) == stored {
			return off, nil
		}
	}
	return 0, fmt.Errorf("no matching CRC-32C found (corrupt pg_control?)")
}

// detectXlogMagic reads the WAL page magic from the first valid WAL segment.
func detectXlogMagic(walDir string) (uint16, error) {
	entries, err := os.ReadDir(walDir)
	if err != nil {
		return 0, err
	}
	for _, e := range entries {
		if e.IsDir() || len(e.Name()) != 24 {
			continue
		}
		path := filepath.Join(walDir, e.Name())
		f, err := os.Open(path)
		if err != nil {
			continue
		}
		hdr := make([]byte, 2)
		_, readErr := f.ReadAt(hdr, 0)
		f.Close()
		if readErr != nil {
			continue
		}
		magic := binary.LittleEndian.Uint16(hdr)
		if magic != 0 {
			return magic, nil
		}
	}
	return 0, fmt.Errorf("no readable WAL segment found in %s", walDir)
}

// ── Data types ───────────────────────────────────────────────────────────────

type checkpointInfo struct {
	lsn             uint64
	redo            uint64
	thisTLI         uint32
	prevTLI         uint32
	fullPageWrites  bool
	walLevel        uint8
	nextXid         uint64
	nextOid         uint32
	nextMulti       uint32
	nextMultiOffset uint32
	oldestXid       uint32
	oldestXidDB     uint32
	oldestMulti     uint32
	oldestMultiDB   uint32
	cpTime          int64
	oldestCommitTs  uint32
	newestCommitTs  uint32
	oldestActiveXid uint32
	isOnline        bool
}

func (c *checkpointInfo) String() string {
	kind := "SHUTDOWN"
	if c.isOnline {
		kind = "ONLINE "
	}
	return fmt.Sprintf("CHECKPOINT_%s lsn=%s  redo=%s  xid=%d  oid=%d",
		kind, lsnStr(c.lsn), lsnStr(c.redo), c.nextXid, c.nextOid)
}

func lsnStr(lsn uint64) string {
	return fmt.Sprintf("%X/%08X", lsn>>32, uint32(lsn))
}

// ── CRC-32C (Castagnoli) — same algorithm PostgreSQL uses ────────────────────

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

func crc32c(data []byte) uint32 { return crc32.Checksum(data, crc32cTable) }

// ── Helpers ──────────────────────────────────────────────────────────────────

func alignUp(n int) int { return (n + maxAlign - 1) &^ (maxAlign - 1) }

func pageEnd(pos int) int { return (pos/xlogBlcksz + 1) * xlogBlcksz }

// nextRecordPos advances past a WAL record of totLen bytes starting at pos,
// inserting short page-header gaps wherever page boundaries are crossed.
func nextRecordPos(pos, totLen int) int {
	remaining := totLen
	cur := pos
	for remaining > 0 {
		avail := pageEnd(cur) - cur
		if avail >= remaining {
			cur += remaining
			break
		}
		remaining -= avail
		cur = pageEnd(cur) + shortPageHdrSz
	}
	return alignUp(cur)
}

// readAcrossPages reads n logical bytes from pos, skipping short page headers
// at page boundaries.
func readAcrossPages(f *os.File, pos int64, n int) ([]byte, error) {
	out := make([]byte, 0, n)
	remaining := n
	cur := pos
	for remaining > 0 {
		avail := int64(pageEnd(int(cur))) - cur
		toRead := int64(remaining)
		if toRead > avail {
			toRead = avail
		}
		chunk := make([]byte, toRead)
		if _, err := f.ReadAt(chunk, cur); err != nil {
			return nil, err
		}
		out = append(out, chunk...)
		remaining -= int(toRead)
		cur += toRead
		if remaining > 0 {
			cur += shortPageHdrSz
		}
	}
	return out, nil
}

// ── WAL scanner ──────────────────────────────────────────────────────────────

func walSegStartLSN(name string) (uint64, error) {
	if len(name) != 24 {
		return 0, fmt.Errorf("not a WAL filename")
	}
	var tli, hi, seg uint64
	if _, err := fmt.Sscanf(name, "%08X%08X%08X", &tli, &hi, &seg); err != nil {
		return 0, err
	}
	return (hi << 32) | (seg << 24), nil
}

// scanWALFile scans one WAL segment for checkpoint records using the given
// page magic (so it works across PostgreSQL versions).
func scanWALFile(path string, xlogMagic uint16) ([]checkpointInfo, error) {
	segLSN, err := walSegStartLSN(filepath.Base(path))
	if err != nil {
		return nil, err
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := fi.Size()
	if fileSize < longPageHdrSz {
		return nil, nil
	}

	firstPage := make([]byte, longPageHdrSz)
	if _, err := f.ReadAt(firstPage, 0); err != nil {
		return nil, err
	}
	if binary.LittleEndian.Uint16(firstPage[0:2]) != xlogMagic {
		return nil, fmt.Errorf("bad page magic (unwritten or wrong file)")
	}
	if binary.LittleEndian.Uint16(firstPage[2:4])&xlrLongHdrFlag == 0 {
		return nil, fmt.Errorf("first page is not a long-header page")
	}

	remLen := int(binary.LittleEndian.Uint32(firstPage[16:20]))
	pos := alignUp(longPageHdrSz + remLen)

	var results []checkpointInfo

	for {
		if pos > 0 && pos%xlogBlcksz == 0 {
			if int64(pos)+shortPageHdrSz > fileSize {
				break
			}
			hdrBuf := make([]byte, shortPageHdrSz)
			if _, err := f.ReadAt(hdrBuf, int64(pos)); err != nil {
				break
			}
			if binary.LittleEndian.Uint16(hdrBuf[0:2]) != xlogMagic {
				break
			}
			info := binary.LittleEndian.Uint16(hdrBuf[2:4])
			hdrSz := shortPageHdrSz
			if info&xlrLongHdrFlag != 0 {
				hdrSz = longPageHdrSz
			}
			rem := int(binary.LittleEndian.Uint32(hdrBuf[16:20]))
			pos = alignUp(pos + hdrSz + rem)
			continue
		}

		if int64(pos)+xlogRecordHdrSz > fileSize {
			break
		}

		hdr := make([]byte, xlogRecordHdrSz)
		if _, err := f.ReadAt(hdr, int64(pos)); err != nil {
			break
		}

		totLen := int(binary.LittleEndian.Uint32(hdr[0:4]))
		if totLen == 0 {
			break
		}
		if totLen < xlogRecordHdrSz {
			break
		}

		xlInfo := hdr[20]
		xlRmid := hdr[21]

		if xlRmid == rmXlogID &&
			(xlInfo == xlogCheckpointOnline || xlInfo == xlogCheckpointShutdown) {
			cp, err := parseCheckpoint(f, int64(pos))
			if err == nil && cp != nil {
				cp.lsn = segLSN | uint64(pos)
				cp.isOnline = xlInfo == xlogCheckpointOnline
				results = append(results, *cp)
			}
		}

		pos = nextRecordPos(pos, totLen)
	}

	return results, nil
}

// parseCheckpoint reads the CheckPoint struct from a WAL record at recPos.
// Layout: XLogRecord(24) | DataHdrShort(2) | CheckPoint(88) = 114 bytes total.
// This layout is identical for PG12–PG17.
func parseCheckpoint(f *os.File, recPos int64) (*checkpointInfo, error) {
	raw, err := readAcrossPages(f, recPos+xlogRecordHdrSz, 2+checkpointDataSize)
	if err != nil {
		return nil, err
	}
	if raw[0] != xlrBlockIDDataShort {
		return nil, fmt.Errorf("unexpected block id %d", raw[0])
	}
	if int(raw[1]) != checkpointDataSize {
		return nil, fmt.Errorf("unexpected data length %d", raw[1])
	}

	d := raw[2:]
	return &checkpointInfo{
		redo:            binary.LittleEndian.Uint64(d[cpRedo : cpRedo+8]),
		thisTLI:         binary.LittleEndian.Uint32(d[cpThisTLI : cpThisTLI+4]),
		prevTLI:         binary.LittleEndian.Uint32(d[cpPrevTLI : cpPrevTLI+4]),
		fullPageWrites:  d[cpFullPageWrites] != 0,
		walLevel:        d[cpWalLevel],
		nextXid:         binary.LittleEndian.Uint64(d[cpNextXid : cpNextXid+8]),
		nextOid:         binary.LittleEndian.Uint32(d[cpNextOid : cpNextOid+4]),
		nextMulti:       binary.LittleEndian.Uint32(d[cpNextMulti : cpNextMulti+4]),
		nextMultiOffset: binary.LittleEndian.Uint32(d[cpNextMultiOffset : cpNextMultiOffset+4]),
		oldestXid:       binary.LittleEndian.Uint32(d[cpOldestXid : cpOldestXid+4]),
		oldestXidDB:     binary.LittleEndian.Uint32(d[cpOldestXidDB : cpOldestXidDB+4]),
		oldestMulti:     binary.LittleEndian.Uint32(d[cpOldestMulti : cpOldestMulti+4]),
		oldestMultiDB:   binary.LittleEndian.Uint32(d[cpOldestMultiDB : cpOldestMultiDB+4]),
		cpTime:          int64(binary.LittleEndian.Uint64(d[cpTime : cpTime+8])),
		oldestCommitTs:  binary.LittleEndian.Uint32(d[cpOldestCommitTsXid : cpOldestCommitTsXid+4]),
		newestCommitTs:  binary.LittleEndian.Uint32(d[cpNewestCommitTsXid : cpNewestCommitTsXid+4]),
		oldestActiveXid: binary.LittleEndian.Uint32(d[cpOldestActiveXid : cpOldestActiveXid+4]),
	}, nil
}

// findLatestCheckpoint scans all WAL segments in walDir and returns the one
// with the highest LSN.
func findLatestCheckpoint(walDir string, xlogMagic uint16) (*checkpointInfo, error) {
	entries, err := os.ReadDir(walDir)
	if err != nil {
		return nil, err
	}

	var segFiles []string
	for _, e := range entries {
		if !e.IsDir() && len(e.Name()) == 24 {
			segFiles = append(segFiles, filepath.Join(walDir, e.Name()))
		}
	}
	sort.Strings(segFiles)

	var best *checkpointInfo
	for _, path := range segFiles {
		cps, err := scanWALFile(path, xlogMagic)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  warning: %s: %v\n", filepath.Base(path), err)
			continue
		}
		for i := range cps {
			if best == nil || cps[i].lsn > best.lsn {
				cp := cps[i]
				best = &cp
			}
		}
	}
	return best, nil
}

// ── pg_control patcher ───────────────────────────────────────────────────────

func patchPGControl(pgControlPath string, cp *checkpointInfo, crcOffset int, dryRun bool) error {
	original, err := os.ReadFile(pgControlPath)
	if err != nil {
		return err
	}

	buf := make([]byte, pgControlFileSize)
	copy(buf, original)
	le := binary.LittleEndian

	le.PutUint32(buf[offState:], dbInProduction)
	le.PutUint64(buf[offCheckPoint:], cp.lsn)

	b := offCheckPointCopy
	le.PutUint64(buf[b+cpRedo:], cp.redo)
	le.PutUint32(buf[b+cpThisTLI:], cp.thisTLI)
	le.PutUint32(buf[b+cpPrevTLI:], cp.prevTLI)
	if cp.fullPageWrites {
		buf[b+cpFullPageWrites] = 1
	} else {
		buf[b+cpFullPageWrites] = 0
	}
	buf[b+cpWalLevel] = cp.walLevel
	for i := 18; i < 24; i++ { buf[b+i] = 0 }
	le.PutUint64(buf[b+cpNextXid:], cp.nextXid)
	le.PutUint32(buf[b+cpNextOid:], cp.nextOid)
	le.PutUint32(buf[b+cpNextMulti:], cp.nextMulti)
	le.PutUint32(buf[b+cpNextMultiOffset:], cp.nextMultiOffset)
	le.PutUint32(buf[b+cpOldestXid:], cp.oldestXid)
	le.PutUint32(buf[b+cpOldestXidDB:], cp.oldestXidDB)
	le.PutUint32(buf[b+cpOldestMulti:], cp.oldestMulti)
	le.PutUint32(buf[b+cpOldestMultiDB:], cp.oldestMultiDB)
	for i := 60; i < 64; i++ { buf[b+i] = 0 }
	le.PutUint64(buf[b+cpTime:], uint64(time.Now().Unix()))
	le.PutUint32(buf[b+cpOldestCommitTsXid:], cp.oldestCommitTs)
	le.PutUint32(buf[b+cpNewestCommitTsXid:], cp.newestCommitTs)
	le.PutUint32(buf[b+cpOldestActiveXid:], cp.oldestActiveXid)
	for i := 84; i < 88; i++ { buf[b+i] = 0 }

	newCRC := crc32c(buf[:crcOffset])
	le.PutUint32(buf[crcOffset:], newCRC)

	if dryRun {
		fmt.Printf("  [dry-run] new CRC would be %08X\n", newCRC)
		return nil
	}

	bakPath := pgControlPath + ".orig"
	if _, err := os.Stat(bakPath); os.IsNotExist(err) {
		if err := os.WriteFile(bakPath, original, 0600); err != nil {
			return fmt.Errorf("backup failed: %w", err)
		}
		fmt.Printf("  backup  → %s\n", bakPath)
	}

	if err := os.WriteFile(pgControlPath, buf, 0600); err != nil {
		return fmt.Errorf("write failed: %w", err)
	}
	fmt.Printf("  new CRC   %08X\n", newCRC)
	return nil
}

// ── CLI ──────────────────────────────────────────────────────────────────────

func stateName(s uint32) string {
	names := map[uint32]string{
		0: "startup", 1: "shut down", 2: "shut down in recovery",
		3: "shutting down", 4: "in crash recovery",
		5: "in archive recovery", 6: "in production",
	}
	if n, ok := names[s]; ok {
		return n
	}
	return "unknown"
}

func usage() {
	fmt.Fprintln(os.Stderr, "pg-crash-recovery — repair PostgreSQL 12–17 pg_control without pg_resetwal")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Usage:")
	fmt.Fprintln(os.Stderr, "  pg-crash-recovery [--dry-run] <data-directory>")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Flags:")
	fmt.Fprintln(os.Stderr, "  --dry-run   Show what would be changed without writing anything")
	os.Exit(1)
}

func main() {
	var dataDir string
	dryRun := false

	for _, a := range os.Args[1:] {
		switch a {
		case "--dry-run", "-n":
			dryRun = true
		case "--help", "-h":
			usage()
		default:
			dataDir = a
		}
	}
	if dataDir == "" {
		usage()
	}

	pgControlPath := filepath.Join(dataDir, "global", "pg_control")
	walDir := filepath.Join(dataDir, "pg_wal")

	fmt.Println("pg-crash-recovery")
	fmt.Println("─────────────────────────────────────────────────────")

	cfg, err := probeCluster(dataDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	ctlData, _ := os.ReadFile(pgControlPath)
	curState := binary.LittleEndian.Uint32(ctlData[offState:])
	curChkpt := binary.LittleEndian.Uint64(ctlData[offCheckPoint:])

	fmt.Printf("PostgreSQL version: %d  (WAL magic 0x%04X  CRC offset %d)\n",
		cfg.majorVersion(), cfg.xlogMagic, cfg.crcOffset)
	fmt.Printf("Current state     : %s\n", stateName(curState))
	fmt.Printf("Current checkpoint: %s\n", lsnStr(curChkpt))
	fmt.Println()

	fmt.Printf("Scanning %s ...\n", walDir)
	latest, err := findLatestCheckpoint(walDir, cfg.xlogMagic)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error scanning WAL: %v\n", err)
		os.Exit(1)
	}
	if latest == nil {
		fmt.Fprintln(os.Stderr, "error: no valid checkpoint found in WAL — cannot recover")
		fmt.Fprintln(os.Stderr, "       manual pg_resetwal or restore from backup required")
		os.Exit(1)
	}

	fmt.Printf("Found   : %s\n", latest)
	fmt.Println()

	action := "Patching"
	if dryRun {
		action = "Dry-run"
	}
	fmt.Printf("%s pg_control:\n", action)
	fmt.Printf("  state     : %s → in production\n", stateName(curState))
	fmt.Printf("  checkpoint: %s → %s\n", lsnStr(curChkpt), lsnStr(latest.lsn))
	fmt.Printf("  redo start: %s\n", lsnStr(latest.redo))

	if err := patchPGControl(pgControlPath, latest, cfg.crcOffset, dryRun); err != nil {
		fmt.Fprintf(os.Stderr, "error patching pg_control: %v\n", err)
		os.Exit(1)
	}

	if dryRun {
		fmt.Println("\n[dry-run] no files were modified")
		return
	}

	fmt.Println()
	fmt.Println("─────────────────────────────────────────────────────")
	fmt.Println("Done. Next steps:")
	fmt.Printf("  1. Remove recovery.signal if present:\n")
	fmt.Printf("       rm -f %s/recovery.signal\n", dataDir)
	fmt.Println("  2. Start PostgreSQL (runs crash recovery automatically):")
	fmt.Printf("       pg_ctl start -D %s\n", dataDir)
	fmt.Println("  3. Verify data and dump immediately:")
	fmt.Println("       pg_dumpall > /safe/path/recovered.sql")
}
