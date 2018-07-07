package ventirpc

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
)

type encoder struct {
	w *bufio.Writer
}

func (e *encoder) encode(msg interface{}, funcId, tag uint8) error {
	// reserved for final length
	e.w.Write([]byte{0, 0})

	e.w.WriteByte(funcId)
	e.w.WriteByte(tag)

	structValue := reflect.ValueOf(msg)
	var err error
	for i := 0; i < structValue.NumField(); i++ {
		f := structValue.Field(i)
		switch v := f.Interface().(type) {
		case uint8, uint16, uint32, uint64:
			err = binary.Write(e.w, binary.BigEndian, v)
		case string:
			err = writeString(e.w, v)
		case []byte:
			if structValue.Type().Field(i).Tag.Get("rpc") == "small" {
				err = writeSmall(e.w, v)
			} else if i != structValue.NumField()-1 {
				panic("unxepected []byte field")
			} else {
				_, err = e.w.Write(v)
			}
		default:
			if f.Kind() == reflect.Array && f.Type().Elem().Kind() == reflect.Uint8 {
				for i := 0; i < f.Len(); i++ {
					e.w.WriteByte(f.Index(i).Interface().(uint8))
				}
			} else {
				err = fmt.Errorf("cannot decode %T", v)
			}
		}
		if err != nil {
			return nil, err
		}
	}

	// TODO: need to calculate length at the beginning...
	length := uint16(e.w.Buffered() - 2)
	encoded := e.w.Bytes()
	binary.BigEndian.PutUint16(encoded, length)

	return e.w.Flush()
}

type decoder struct {
	r *bufio.Reader
}

func (d *decoder) decode(dst interface{}) error {
	var err error
	structValue := reflect.ValueOf(dst).Elem()
	for i := 0; i < structValue.NumField(); i++ {
		f := structValue.Field(i)
		switch v := f.Interface().(type) {
		// These cannot be compressed into a single
		// case, or vv will not have the correct type.
		// See https://golang.org/ref/spec#Type_switches.
		case uint8:
			err = binary.Read(d.r, binary.BigEndian, &v)
			f.Set(reflect.ValueOf(v))
		case uint16:
			err = binary.Read(d.r, binary.BigEndian, &v)
			f.Set(reflect.ValueOf(v))
		case uint32:
			err = binary.Read(d.r, binary.BigEndian, &v)
			f.Set(reflect.ValueOf(v))
		case uint64:
			err = binary.Read(d.r, binary.BigEndian, &v)
			f.Set(reflect.ValueOf(v))
		case string:
			var s string
			s, err = readString(d.r)
			f.SetString(s)
		case []byte:
			if structValue.Type().Field(i).Tag.Get("rpc") == "small" {
				var s []byte
				s, err = readSmall(d.r)
				f.SetBytes(s)
			} else if i != structValue.NumField()-1 {
				panic("unxepected []byte field")
			} else {
				length := d.r.Len()
				n, _ := d.r.Read(v)

				// kinda hacky: we communicate the read length back to
				// the venti.Client using the slice length.
				f.SetLen(n)

				if length != n {
					err = errors.New("short read")
				}
			}
		default:
			if f.Kind() == reflect.Array && f.Type().Elem().Kind() == reflect.Uint8 {
				if f.Len() > d.r.Len() {
					err = errors.New("short buffer")
				}
				for i := 0; i < f.Len(); i++ {
					c, _ := d.r.ReadByte()
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
