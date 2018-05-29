package venti

import "fmt"

type BlockType uint8

const (
	DataType BlockType = iota << 3
	DirType
	RootType
	MaxType
	CorruptType = 0xFF

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
		return "RootType"
	case CorruptType:
		return "CorruptType"
	default:
		return "BadType"
	}
	depth := t & typeDepthMask
	if depth != 0 {
		s += fmt.Sprintf("+%d", depth)
	}
	return s
}

func (t BlockType) depth() int {
	return int(t & typeDepthMask)
}

const (
	onDiskErrType = iota /* illegal */
	onDiskRootType
	onDiskDirType
	onDiskPointerType0
	onDiskPointerType1
	onDiskPointerType2
	onDiskPointerType3
	onDiskPointerType4
	onDiskPointerType5
	onDiskPointerType6
	onDiskPointerType7 // not used
	onDiskPointerType8 // not used
	onDiskPointerType9 // not used
	onDiskDataType
	onDiskMaxType
)

var toDisk = []uint8{
	onDiskDataType,
	onDiskPointerType0,
	onDiskPointerType1,
	onDiskPointerType2,
	onDiskPointerType3,
	onDiskPointerType4,
	onDiskPointerType5,
	onDiskPointerType6,
	onDiskDirType,
	onDiskPointerType0,
	onDiskPointerType1,
	onDiskPointerType2,
	onDiskPointerType3,
	onDiskPointerType4,
	onDiskPointerType5,
	onDiskPointerType6,
	onDiskRootType,
}

var fromDisk = []BlockType{
	CorruptType,
	RootType,
	DirType,
	DirType + 1,
	DirType + 2,
	DirType + 3,
	DirType + 4,
	DirType + 5,
	DirType + 6,
	DirType + 7,
	CorruptType,
	CorruptType,
	CorruptType,
	DataType,
}

func (t BlockType) onDiskType() uint8 {
	if int(t) > len(toDisk) {
		return CorruptType
	}
	return toDisk[t]
}

func fromOnDiskType(t uint8) BlockType {
	if int(t) >= len(fromDisk) {
		return CorruptType
	}
	return fromDisk[t]
}
