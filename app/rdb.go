package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

type rdbParser struct {
	reader *bufio.Reader
}

func loadRdbFile() error {
	path := filepath.Join(serverConfig.Dir, serverConfig.DbFileName)
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	parser := rdbParser{reader: bufio.NewReader(file)}
	header := make([]byte, 9)
	if _, err := io.ReadFull(parser.reader, header); err != nil {
		return fmt.Errorf("reading header: %w", err)
	}
	if string(header[:5]) != "REDIS" {
		return fmt.Errorf("invalid RDB magic string")
	}

	for {
		opCode, err := parser.reader.ReadByte()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		var expiry time.Time
		if opCode == 0xFD || opCode == 0xFC {
			var buf []byte
			if opCode == 0xFD { // Expiry in seconds
				buf = make([]byte, 4)
			} else { // Expiry in milliseconds
				buf = make([]byte, 8)
			}

			if _, err := io.ReadFull(parser.reader, buf); err != nil {
				return err
			}

			if opCode == 0xFD {
				expiry = time.Unix(int64(binary.LittleEndian.Uint32(buf)), 0)
			} else {
				expiry = time.UnixMilli(int64(binary.LittleEndian.Uint64(buf)))
			}
			// After expiry, the next byte is the value type opcode
			opCode, err = parser.reader.ReadByte()
			if err != nil {
				return err
			}
		}

		switch opCode {
		case 0xFE: // Database selector
			// Read database number, but we don't use it.
			if _, err := parser.reader.ReadByte(); err != nil {
				return err
			}
		case 0xFB: // RESIZE DB
			if _, _, err := parser.readLength(); err != nil {
				return err
			}
			if _, _, err := parser.readLength(); err != nil {
				return err
			}
		case 0xFA: // AUX field
			// Skip auxiliary fields
			if _, err := parser.readString(); err != nil {
				return err
			}
			if _, err := parser.readString(); err != nil {
				return err
			}
		case 0x00: // String value type
			key, err := parser.readString()
			if err != nil {
				return err
			}
			value, err := parser.readString()
			if err != nil {
				return err
			}
			dataStore.Lock()
			dataStore.data[string(key)] = valueEntry{value: string(value), expires: expiry}
			dataStore.Unlock()
		case 0xFF: // End of File
			return nil
		default:
			// You can add more opcodes here as you implement them
		}
	}
	return nil
}

func (p *rdbParser) readLength() (uint32, bool, error) {
	b, err := p.reader.ReadByte()
	if err != nil {
		return 0, false, err
	}
	switch (b & 0xC0) >> 6 {
	case 0: // 00xxxxxx -> 6-bit length
		return uint32(b & 0x3F), false, nil
	case 1: // 01xxxxxx -> 14-bit length
		b2, err := p.reader.ReadByte()
		if err != nil {
			return 0, false, err
		}
		return (uint32(b&0x3F) << 8) | uint32(b2), false, nil
	case 2: // 10xxxxxx -> 32-bit length
		buf := make([]byte, 4)
		if _, err := io.ReadFull(p.reader, buf); err != nil {
			return 0, false, err
		}
		return binary.BigEndian.Uint32(buf), false, nil
	case 3: // 11xxxxxx -> Special encoded format
		return uint32(b & 0x3F), true, nil
	}
	return 0, false, fmt.Errorf("unreachable")
}

func (p *rdbParser) readString() ([]byte, error) {
	length, isSpecial, err := p.readLength()
	if err != nil {
		return nil, err
	}
	if isSpecial {
		switch length {
		case 0: // 8-bit integer
			b, err := p.reader.ReadByte()
			return []byte(strconv.Itoa(int(int8(b)))), err
		case 1: // 16-bit integer
			buf := make([]byte, 2)
			_, err := io.ReadFull(p.reader, buf)
			return []byte(strconv.Itoa(int(int16(binary.LittleEndian.Uint16(buf))))), err
		case 2: // 32-bit integer
			buf := make([]byte, 4)
			_, err := io.ReadFull(p.reader, buf)
			return []byte(strconv.Itoa(int(int32(binary.LittleEndian.Uint32(buf))))), err
		default:
			return nil, fmt.Errorf("unknown special string format: %d", length)
		}
	}

	buf := make([]byte, length)
	if _, err := io.ReadFull(p.reader, buf); err != nil {
		return nil, err
	}
	return buf, nil
}
