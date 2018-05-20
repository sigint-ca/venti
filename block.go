package venti

import "fmt"

type BlockType uint8

const (
	DataType BlockType = iota << 3
	DirType
	RootType

	typeDepthMask BlockType = 7
	typeBaseMask            = ^typeDepthMask
)

func (t BlockType) String() string {
	var s string
	switch t & typeBaseMask {
	case DataType:
		s = "DataType"
	case DirType:
		s = "DirType"
	case RootType:
		s = "RootType"
	default:
		s = "BadType"
	}
	depth := t & typeDepthMask
	if depth != 0 {
		s += fmt.Sprintf("+%d", depth)
	}
	return s
}

func isPointerType(t BlockType) bool {
	return t&typeDepthMask > 0
}
