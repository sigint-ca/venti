package rpc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
)

func encode(msg interface{}, funcId, tag uint8) ([]byte, error) {
	var buf bytes.Buffer

	// reserved for final length
	buf.Write([]byte{0, 0})

	buf.WriteByte(funcId)
	buf.WriteByte(tag)

	structValue := reflect.ValueOf(msg)
	var err error
	for i := 0; i < structValue.NumField(); i++ {
		f := structValue.Field(i)
		switch v := f.Interface().(type) {
		case uint8, uint16, uint32, uint64:
			err = binary.Write(&buf, binary.BigEndian, v)
		case string:
			if structValue.Type().Field(i).Tag == "short" {
				err = writeShort(&buf, v)
			} else {
				err = writeString(&buf, v)
			}
		case []byte:
			if i != structValue.NumField()-1 {
				panic("unxepected []byte field")
			}
			buf.Write(v)
		default:
			if f.Kind() == reflect.Array && f.Type().Elem().Kind() == reflect.Uint8 {
				for i := 0; i < f.Len(); i++ {
					buf.WriteByte(f.Index(i).Interface().(uint8))
				}
			} else {
				err = fmt.Errorf("cannot decode %T", v)
			}
		}
		if err != nil {
			return nil, err
		}
	}

	// final length minus two bytes reserved for length
	// at the beginning of the buffer
	length := uint16(buf.Len() - 2)

	encoded := buf.Bytes()
	binary.BigEndian.PutUint16(encoded, length)

	return encoded, nil
}

func decode(dst interface{}, buf []byte) error {
	r := bytes.NewReader(buf)
	var err error
	structValue := reflect.ValueOf(dst).Elem()
	for i := 0; i < structValue.NumField(); i++ {
		f := structValue.Field(i)
		switch v := f.Interface().(type) {
		// These cannot be compressed into a single
		// case, or vv will not have the correct type.
		// See https://golang.org/ref/spec#Type_switches.
		case uint8:
			err = binary.Read(r, binary.BigEndian, &v)
			f.Set(reflect.ValueOf(v))
		case uint16:
			err = binary.Read(r, binary.BigEndian, &v)
			f.Set(reflect.ValueOf(v))
		case uint32:
			err = binary.Read(r, binary.BigEndian, &v)
			f.Set(reflect.ValueOf(v))
		case uint64:
			err = binary.Read(r, binary.BigEndian, &v)
			f.Set(reflect.ValueOf(v))
		case string:
			var s string
			if structValue.Type().Field(i).Tag == "short" {
				s, err = readShort(r)
			} else {
				s, err = readString(r)
			}
			f.SetString(s)
		case []byte:
			if i != structValue.NumField()-1 {
				panic("unxepected []byte field")
			}
			length := r.Len()
			n, _ := r.Read(v)

			// kinda hacky: we communicate the read length back to
			// the venti.Client using the slice length.
			f.SetLen(n)

			if length != n {
				err = errors.New("short read")
			}
		default:
			if f.Kind() == reflect.Array && f.Type().Elem().Kind() == reflect.Uint8 {
				if f.Len() > r.Len() {
					err = errors.New("short buffer")
				}
				for i := 0; i < f.Len(); i++ {
					c, _ := r.ReadByte()
					f.Index(i).Set(reflect.ValueOf(c))
				}
			} else {
				err = fmt.Errorf("cannot decode %T", v)
			}
		}
		if err != nil {
			return fmt.Errorf("decode field index=%d type=%s: %v", i, f.Kind(), err)
		}
	}
	if err, ok := dst.(*ServerError); ok {
		return *err
	}
	return nil
}
