// Code generated by protoc-gen-gogo.
// source: etcdserver.proto
// DO NOT EDIT!

/*
	Package etcdserverpb is a generated protocol buffer package.

	It is generated from these files:
		etcdserver.proto

	It has these top-level messages:
		Request
*/
package etcdserverpb

import proto "github.com/coreos/etcd/third_party/code.google.com/p/gogoprotobuf/proto"
import json "encoding/json"
import math "math"

// discarding unused import gogoproto "code.google.com/p/gogoprotobuf/gogoproto/gogo.pb"

import io "io"
import code_google_com_p_gogoprotobuf_proto "github.com/coreos/etcd/third_party/code.google.com/p/gogoprotobuf/proto"

// Reference proto, json, and math imports to suppress error if they are not otherwise used.
var _ = proto.Marshal
var _ = &json.SyntaxError{}
var _ = math.Inf

type Request struct {
	Id               int64  `protobuf:"varint,1,req,name=id" json:"id"`
	Method           string `protobuf:"bytes,2,req,name=method" json:"method"`
	Path             string `protobuf:"bytes,3,req,name=path" json:"path"`
	Val              string `protobuf:"bytes,4,req,name=val" json:"val"`
	Dir              bool   `protobuf:"varint,5,req,name=dir" json:"dir"`
	PrevValue        string `protobuf:"bytes,6,req,name=prevValue" json:"prevValue"`
	PrevIndex        uint64 `protobuf:"varint,7,req,name=prevIndex" json:"prevIndex"`
	PrevExists       *bool  `protobuf:"varint,8,req,name=prevExists" json:"prevExists,omitempty"`
	Expiration       int64  `protobuf:"varint,9,req,name=expiration" json:"expiration"`
	Wait             bool   `protobuf:"varint,10,req,name=wait" json:"wait"`
	Since            uint64 `protobuf:"varint,11,req,name=since" json:"since"`
	Recursive        bool   `protobuf:"varint,12,req,name=recursive" json:"recursive"`
	Sorted           bool   `protobuf:"varint,13,req,name=sorted" json:"sorted"`
	Quorum           bool   `protobuf:"varint,14,req,name=quorum" json:"quorum"`
	Time             int64  `protobuf:"varint,15,req,name=time" json:"time"`
	XXX_unrecognized []byte `json:"-"`
}

func (m *Request) Reset()         { *m = Request{} }
func (m *Request) String() string { return proto.CompactTextString(m) }
func (*Request) ProtoMessage()    {}

func init() {
}
func (m *Request) Unmarshal(data []byte) error {
	l := len(data)
	index := 0
	for index < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if index >= l {
				return io.ErrUnexpectedEOF
			}
			b := data[index]
			index++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return code_google_com_p_gogoprotobuf_proto.ErrWrongType
			}
			for shift := uint(0); ; shift += 7 {
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				m.Id |= (int64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 2 {
				return code_google_com_p_gogoprotobuf_proto.ErrWrongType
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := index + int(stringLen)
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Method = string(data[index:postIndex])
			index = postIndex
		case 3:
			if wireType != 2 {
				return code_google_com_p_gogoprotobuf_proto.ErrWrongType
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := index + int(stringLen)
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Path = string(data[index:postIndex])
			index = postIndex
		case 4:
			if wireType != 2 {
				return code_google_com_p_gogoprotobuf_proto.ErrWrongType
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := index + int(stringLen)
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Val = string(data[index:postIndex])
			index = postIndex
		case 5:
			if wireType != 0 {
				return code_google_com_p_gogoprotobuf_proto.ErrWrongType
			}
			var v int
			for shift := uint(0); ; shift += 7 {
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				v |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.Dir = bool(v != 0)
		case 6:
			if wireType != 2 {
				return code_google_com_p_gogoprotobuf_proto.ErrWrongType
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := index + int(stringLen)
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.PrevValue = string(data[index:postIndex])
			index = postIndex
		case 7:
			if wireType != 0 {
				return code_google_com_p_gogoprotobuf_proto.ErrWrongType
			}
			for shift := uint(0); ; shift += 7 {
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				m.PrevIndex |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 8:
			if wireType != 0 {
				return code_google_com_p_gogoprotobuf_proto.ErrWrongType
			}
			var v int
			for shift := uint(0); ; shift += 7 {
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				v |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			b := bool(v != 0)
			m.PrevExists = &b
		case 9:
			if wireType != 0 {
				return code_google_com_p_gogoprotobuf_proto.ErrWrongType
			}
			for shift := uint(0); ; shift += 7 {
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				m.Expiration |= (int64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 10:
			if wireType != 0 {
				return code_google_com_p_gogoprotobuf_proto.ErrWrongType
			}
			var v int
			for shift := uint(0); ; shift += 7 {
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				v |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.Wait = bool(v != 0)
		case 11:
			if wireType != 0 {
				return code_google_com_p_gogoprotobuf_proto.ErrWrongType
			}
			for shift := uint(0); ; shift += 7 {
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				m.Since |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 12:
			if wireType != 0 {
				return code_google_com_p_gogoprotobuf_proto.ErrWrongType
			}
			var v int
			for shift := uint(0); ; shift += 7 {
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				v |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.Recursive = bool(v != 0)
		case 13:
			if wireType != 0 {
				return code_google_com_p_gogoprotobuf_proto.ErrWrongType
			}
			var v int
			for shift := uint(0); ; shift += 7 {
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				v |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.Sorted = bool(v != 0)
		case 14:
			if wireType != 0 {
				return code_google_com_p_gogoprotobuf_proto.ErrWrongType
			}
			var v int
			for shift := uint(0); ; shift += 7 {
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				v |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.Quorum = bool(v != 0)
		case 15:
			if wireType != 0 {
				return code_google_com_p_gogoprotobuf_proto.ErrWrongType
			}
			for shift := uint(0); ; shift += 7 {
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				m.Time |= (int64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			var sizeOfWire int
			for {
				sizeOfWire++
				wire >>= 7
				if wire == 0 {
					break
				}
			}
			index -= sizeOfWire
			skippy, err := code_google_com_p_gogoprotobuf_proto.Skip(data[index:])
			if err != nil {
				return err
			}
			if (index + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.XXX_unrecognized = append(m.XXX_unrecognized, data[index:index+skippy]...)
			index += skippy
		}
	}
	return nil
}
func (m *Request) Size() (n int) {
	var l int
	_ = l
	n += 1 + sovEtcdserver(uint64(m.Id))
	l = len(m.Method)
	n += 1 + l + sovEtcdserver(uint64(l))
	l = len(m.Path)
	n += 1 + l + sovEtcdserver(uint64(l))
	l = len(m.Val)
	n += 1 + l + sovEtcdserver(uint64(l))
	n += 2
	l = len(m.PrevValue)
	n += 1 + l + sovEtcdserver(uint64(l))
	n += 1 + sovEtcdserver(uint64(m.PrevIndex))
	if m.PrevExists != nil {
		n += 2
	}
	n += 1 + sovEtcdserver(uint64(m.Expiration))
	n += 2
	n += 1 + sovEtcdserver(uint64(m.Since))
	n += 2
	n += 2
	n += 2
	n += 1 + sovEtcdserver(uint64(m.Time))
	if m.XXX_unrecognized != nil {
		n += len(m.XXX_unrecognized)
	}
	return n
}

func sovEtcdserver(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}
func sozEtcdserver(x uint64) (n int) {
	return sovEtcdserver(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *Request) Marshal() (data []byte, err error) {
	size := m.Size()
	data = make([]byte, size)
	n, err := m.MarshalTo(data)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

func (m *Request) MarshalTo(data []byte) (n int, err error) {
	var i int
	_ = i
	var l int
	_ = l
	data[i] = 0x8
	i++
	i = encodeVarintEtcdserver(data, i, uint64(m.Id))
	data[i] = 0x12
	i++
	i = encodeVarintEtcdserver(data, i, uint64(len(m.Method)))
	i += copy(data[i:], m.Method)
	data[i] = 0x1a
	i++
	i = encodeVarintEtcdserver(data, i, uint64(len(m.Path)))
	i += copy(data[i:], m.Path)
	data[i] = 0x22
	i++
	i = encodeVarintEtcdserver(data, i, uint64(len(m.Val)))
	i += copy(data[i:], m.Val)
	data[i] = 0x28
	i++
	if m.Dir {
		data[i] = 1
	} else {
		data[i] = 0
	}
	i++
	data[i] = 0x32
	i++
	i = encodeVarintEtcdserver(data, i, uint64(len(m.PrevValue)))
	i += copy(data[i:], m.PrevValue)
	data[i] = 0x38
	i++
	i = encodeVarintEtcdserver(data, i, uint64(m.PrevIndex))
	if m.PrevExists != nil {
		data[i] = 0x40
		i++
		if *m.PrevExists {
			data[i] = 1
		} else {
			data[i] = 0
		}
		i++
	}
	data[i] = 0x48
	i++
	i = encodeVarintEtcdserver(data, i, uint64(m.Expiration))
	data[i] = 0x50
	i++
	if m.Wait {
		data[i] = 1
	} else {
		data[i] = 0
	}
	i++
	data[i] = 0x58
	i++
	i = encodeVarintEtcdserver(data, i, uint64(m.Since))
	data[i] = 0x60
	i++
	if m.Recursive {
		data[i] = 1
	} else {
		data[i] = 0
	}
	i++
	data[i] = 0x68
	i++
	if m.Sorted {
		data[i] = 1
	} else {
		data[i] = 0
	}
	i++
	data[i] = 0x70
	i++
	if m.Quorum {
		data[i] = 1
	} else {
		data[i] = 0
	}
	i++
	data[i] = 0x78
	i++
	i = encodeVarintEtcdserver(data, i, uint64(m.Time))
	if m.XXX_unrecognized != nil {
		i += copy(data[i:], m.XXX_unrecognized)
	}
	return i, nil
}
func encodeFixed64Etcdserver(data []byte, offset int, v uint64) int {
	data[offset] = uint8(v)
	data[offset+1] = uint8(v >> 8)
	data[offset+2] = uint8(v >> 16)
	data[offset+3] = uint8(v >> 24)
	data[offset+4] = uint8(v >> 32)
	data[offset+5] = uint8(v >> 40)
	data[offset+6] = uint8(v >> 48)
	data[offset+7] = uint8(v >> 56)
	return offset + 8
}
func encodeFixed32Etcdserver(data []byte, offset int, v uint32) int {
	data[offset] = uint8(v)
	data[offset+1] = uint8(v >> 8)
	data[offset+2] = uint8(v >> 16)
	data[offset+3] = uint8(v >> 24)
	return offset + 4
}
func encodeVarintEtcdserver(data []byte, offset int, v uint64) int {
	for v >= 1<<7 {
		data[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	data[offset] = uint8(v)
	return offset + 1
}
