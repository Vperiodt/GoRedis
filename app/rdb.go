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
		if os.IsNotExist(err) {
			fmt.Println("RDB file not found, starting with empty DB.")
			return nil
		}
		return err
	}

	defer file.Close()

	parser := rdbParser{
		reader: bufio.NewReader(file),
	}
	// REDIS0011
	header := make([]byte, 9)

	if _, err := io.ReadFull(parser.reader, header); err != nil {
		return fmt.Errorf("reading header: %w", err)
	}
	if string(header[:5]) != "REDIS" {
		return fmt.Errorf("invalid RDB magic string")
	}

	for {
		var expiry time.Time
		opCode, err := parser.reader.ReadByte()
		if err != nil {
			if err == io.EOF {
				break // End of file is expected
			}
			return err
		}

		if opCode == 0xFD { // Expiry in seconds (4 bytes)
			buf := make([]byte, 4)
			if _, err := io.ReadFull(parser.reader, buf); err != nil {
				return fmt.Errorf("reading expiry (FD): %w", err)
			}

			expirySecs := binary.LittleEndian.Uint32(buf)
			expiry = time.Unix(int64(expirySecs), 0)

			// After reading expiry, we MUST read the next byte, which is the value type opcode.
			opCode, err = parser.reader.ReadByte()
			if err != nil {
				return err
			}
		} else if opCode == 0xFC { // Expiry in milliseconds (8 bytes)
			buf := make([]byte, 8)
			if _, err := io.ReadFull(parser.reader, buf); err != nil {
				return fmt.Errorf("reading expiry (FC): %w", err)
			}

			expiryMillis := binary.LittleEndian.Uint64(buf)
			expiry = time.UnixMilli(int64(expiryMillis))

			opCode, err = parser.reader.ReadByte()
			if err != nil {
				return err
			}
		}
		switch opCode {
		case 0xFA:
			if _, err := parser.readString(); err != nil {
				return err
			}
			if _, err := parser.readString(); err != nil {
				return err
			}
		case 0xFE:
			if _, _, err := parser.readLength(); err != nil {
				return err
			}
		case 0xFB:

			if _, _, err := parser.readLength(); err != nil {
				return err
			}
			if _, _, err := parser.readLength(); err != nil {
				return err
			}
		case 0x00:
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
		case 0xFF:
			return nil
		default:

		}
	}
	return nil
}

func (p *rdbParser) readLength() (length uint32, isSpecial bool, err error) {
	b, err := p.reader.ReadByte()
	if err != nil {
		return 0, false, err
	}

	switch (b & 0xC0) >> 6 {
	case 0:
		return uint32(b & 0x3F), false, nil
	case 1:
		b2, err := p.reader.ReadByte()
		if err != nil {
			return 0, false, err
		}
		return (uint32(b&0x3F) << 8) | uint32(b2), false, nil
	case 2:
		buf := make([]byte, 4)
		if _, err := io.ReadFull(p.reader, buf); err != nil {
			return 0, false, err
		}
		return binary.BigEndian.Uint32(buf), false, nil
	case 3:
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
		case 0:
			b, err := p.reader.ReadByte()
			if err != nil {
				return nil, err
			}
			return []byte(strconv.Itoa(int(int8(b)))), nil
		case 1:
			buf := make([]byte, 2)
			if _, err := io.ReadFull(p.reader, buf); err != nil {
				return nil, err
			}

			val := binary.LittleEndian.Uint16(buf)
			return []byte(strconv.Itoa(int(int16(val)))), nil
		case 2:
			buf := make([]byte, 4)
			if _, err := io.ReadFull(p.reader, buf); err != nil {
				return nil, err
			}
			val := binary.LittleEndian.Uint32(buf)
			return []byte(strconv.Itoa(int(int32(val)))), nil
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
