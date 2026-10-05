package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// On-disk record framing:
//
//	+----------------+----------------+------+-----------------+
//	| length uint32  | crc32c uint32  | type | data (length-1) |
//	+----------------+----------------+------+-----------------+
//
// length counts type+data. The checksum covers type+data. Little endian.
// A torn write (crash mid-record) shows up as a short read or a checksum
// mismatch and is detected on recovery.
const (
	headerSize    = 8
	maxRecordSize = 32 << 20 // bounds allocation when a length field is garbage
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// errBadTail marks an unreadable record: torn write or corruption.
var errBadTail = errors.New("storage: unreadable record")

func recordSize(r Record) int64 { return int64(headerSize + 1 + len(r.Data)) }

func writeRecord(w io.Writer, r Record) error {
	var hdr [headerSize + 1]byte
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(1+len(r.Data)))
	hdr[headerSize] = r.Type
	crc := crc32.Update(0, crcTable, hdr[headerSize:headerSize+1])
	crc = crc32.Update(crc, crcTable, r.Data)
	binary.LittleEndian.PutUint32(hdr[4:8], crc)
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.Write(r.Data)
	return err
}

// readRecord returns io.EOF at a clean end of file and errBadTail for a torn
// or corrupt record.
func readRecord(r *bufio.Reader) (Record, error) {
	var hdr [headerSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return Record{}, io.EOF
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return Record{}, errBadTail
		}
		return Record{}, err
	}
	n := binary.LittleEndian.Uint32(hdr[0:4])
	if n == 0 || n > maxRecordSize {
		return Record{}, errBadTail
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return Record{}, errBadTail
		}
		return Record{}, err
	}
	if crc32.Checksum(body, crcTable) != binary.LittleEndian.Uint32(hdr[4:8]) {
		return Record{}, errBadTail
	}
	return Record{Type: body[0], Data: body[1:]}, nil
}

// scanSegment reads path from the start, calling fn (if non-nil) for every
// valid record. It returns the byte length of the valid prefix. err is nil
// when the file ends cleanly, errBadTail when it ends in a bad record, or an
// I/O or callback error.
func scanSegment(path string, fn func(Record) error) (valid int64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	br := bufio.NewReaderSize(f, 256<<10)
	for {
		rec, err := readRecord(br)
		if errors.Is(err, io.EOF) {
			return valid, nil
		}
		if err != nil {
			return valid, err
		}
		if fn != nil {
			if err := fn(rec); err != nil {
				return valid, err
			}
		}
		valid += recordSize(rec)
	}
}

func startsWithSnapshot(path string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()
	rec, err := readRecord(bufio.NewReader(f))
	switch {
	case err == nil:
		return rec.Type == snapshotMark, nil
	case errors.Is(err, io.EOF), errors.Is(err, errBadTail):
		return false, nil
	default:
		return false, fmt.Errorf("storage: reading %s: %w", path, err)
	}
}
