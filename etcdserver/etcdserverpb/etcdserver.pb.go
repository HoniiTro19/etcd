// Code generated by protoc-gen-gogo.
// source: etcdserver.proto
// DO NOT EDIT!

/*
	Package etcdserverpb is a generated protocol buffer package.

	It is generated from these files:
		etcdserver.proto
		raft_internal.proto
		rpc.proto

	It has these top-level messages:
		Request
		Metadata
		InternalRaftRequest
		EmptyResponse
		ResponseHeader
		RangeRequest
		RangeResponse
		PutRequest
		PutResponse
		DeleteRangeRequest
		DeleteRangeResponse
		RequestUnion
		ResponseUnion
		Compare
		TxnRequest
		TxnResponse
		CompactionRequest
		CompactionResponse
		HashRequest
		HashResponse
		SnapshotRequest
		SnapshotResponse
		WatchRequest
		WatchCreateRequest
		WatchCancelRequest
		WatchResponse
		LeaseGrantRequest
		LeaseGrantResponse
		LeaseRevokeRequest
		LeaseRevokeResponse
		LeaseKeepAliveRequest
		LeaseKeepAliveResponse
		Member
		MemberAddRequest
		MemberAddResponse
		MemberRemoveRequest
		MemberRemoveResponse
		MemberUpdateRequest
		MemberUpdateResponse
		MemberListRequest
		MemberListResponse
		DefragmentRequest
		DefragmentResponse
		AlarmRequest
		AlarmMember
		AlarmResponse
		StatusRequest
		StatusResponse
		AuthEnableRequest
		AuthDisableRequest
		AuthenticateRequest
		AuthUserAddRequest
		AuthUserGetRequest
		AuthUserDeleteRequest
		AuthUserChangePasswordRequest
		AuthUserGrantRequest
		AuthUserRevokeRequest
		AuthRoleAddRequest
		AuthRoleGetRequest
		AuthRoleDeleteRequest
		AuthRoleGrantRequest
		AuthRoleRevokeRequest
		AuthEnableResponse
		AuthDisableResponse
		AuthenticateResponse
		AuthUserAddResponse
		AuthUserGetResponse
		AuthUserDeleteResponse
		AuthUserChangePasswordResponse
		AuthUserGrantResponse
		AuthUserRevokeResponse
		AuthRoleAddResponse
		AuthRoleGetResponse
		AuthRoleDeleteResponse
		AuthRoleGrantResponse
		AuthRoleRevokeResponse
*/
package etcdserverpb

import (
	"fmt"

	proto "github.com/gogo/protobuf/proto"

	math "math"
)

import io "io"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
const _ = proto.GoGoProtoPackageIsVersion1

type Request struct {
	ID               uint64 `protobuf:"varint,1,opt,name=ID,json=iD" json:"ID"`
	Method           string `protobuf:"bytes,2,opt,name=Method,json=method" json:"Method"`
	Path             string `protobuf:"bytes,3,opt,name=Path,json=path" json:"Path"`
	Val              string `protobuf:"bytes,4,opt,name=Val,json=val" json:"Val"`
	Dir              bool   `protobuf:"varint,5,opt,name=Dir,json=dir" json:"Dir"`
	PrevValue        string `protobuf:"bytes,6,opt,name=PrevValue,json=prevValue" json:"PrevValue"`
	PrevIndex        uint64 `protobuf:"varint,7,opt,name=PrevIndex,json=prevIndex" json:"PrevIndex"`
	PrevExist        *bool  `protobuf:"varint,8,opt,name=PrevExist,json=prevExist" json:"PrevExist,omitempty"`
	Expiration       int64  `protobuf:"varint,9,opt,name=Expiration,json=expiration" json:"Expiration"`
	Wait             bool   `protobuf:"varint,10,opt,name=Wait,json=wait" json:"Wait"`
	Since            uint64 `protobuf:"varint,11,opt,name=Since,json=since" json:"Since"`
	Recursive        bool   `protobuf:"varint,12,opt,name=Recursive,json=recursive" json:"Recursive"`
	Sorted           bool   `protobuf:"varint,13,opt,name=Sorted,json=sorted" json:"Sorted"`
	Quorum           bool   `protobuf:"varint,14,opt,name=Quorum,json=quorum" json:"Quorum"`
	Time             int64  `protobuf:"varint,15,opt,name=Time,json=time" json:"Time"`
	Stream           bool   `protobuf:"varint,16,opt,name=Stream,json=stream" json:"Stream"`
	Refresh          *bool  `protobuf:"varint,17,opt,name=Refresh,json=refresh" json:"Refresh,omitempty"`
	XXX_unrecognized []byte `json:"-"`
}

func (m *Request) Reset()                    { *m = Request{} }
func (m *Request) String() string            { return proto.CompactTextString(m) }
func (*Request) ProtoMessage()               {}
func (*Request) Descriptor() ([]byte, []int) { return fileDescriptorEtcdserver, []int{0} }

type Metadata struct {
	NodeID           uint64 `protobuf:"varint,1,opt,name=NodeID,json=nodeID" json:"NodeID"`
	ClusterID        uint64 `protobuf:"varint,2,opt,name=ClusterID,json=clusterID" json:"ClusterID"`
	XXX_unrecognized []byte `json:"-"`
}

func (m *Metadata) Reset()                    { *m = Metadata{} }
func (m *Metadata) String() string            { return proto.CompactTextString(m) }
func (*Metadata) ProtoMessage()               {}
func (*Metadata) Descriptor() ([]byte, []int) { return fileDescriptorEtcdserver, []int{1} }

func init() {
	proto.RegisterType((*Request)(nil), "etcdserverpb.Request")
	proto.RegisterType((*Metadata)(nil), "etcdserverpb.Metadata")
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

func (m *Request) MarshalTo(data []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	data[i] = 0x8
	i++
	i = encodeVarintEtcdserver(data, i, uint64(m.ID))
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
	if m.PrevExist != nil {
		data[i] = 0x40
		i++
		if *m.PrevExist {
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
	data[i] = 0x80
	i++
	data[i] = 0x1
	i++
	if m.Stream {
		data[i] = 1
	} else {
		data[i] = 0
	}
	i++
	if m.Refresh != nil {
		data[i] = 0x88
		i++
		data[i] = 0x1
		i++
		if *m.Refresh {
			data[i] = 1
		} else {
			data[i] = 0
		}
		i++
	}
	if m.XXX_unrecognized != nil {
		i += copy(data[i:], m.XXX_unrecognized)
	}
	return i, nil
}

func (m *Metadata) Marshal() (data []byte, err error) {
	size := m.Size()
	data = make([]byte, size)
	n, err := m.MarshalTo(data)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

func (m *Metadata) MarshalTo(data []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	data[i] = 0x8
	i++
	i = encodeVarintEtcdserver(data, i, uint64(m.NodeID))
	data[i] = 0x10
	i++
	i = encodeVarintEtcdserver(data, i, uint64(m.ClusterID))
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
func (m *Request) Size() (n int) {
	var l int
	_ = l
	n += 1 + sovEtcdserver(uint64(m.ID))
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
	if m.PrevExist != nil {
		n += 2
	}
	n += 1 + sovEtcdserver(uint64(m.Expiration))
	n += 2
	n += 1 + sovEtcdserver(uint64(m.Since))
	n += 2
	n += 2
	n += 2
	n += 1 + sovEtcdserver(uint64(m.Time))
	n += 3
	if m.Refresh != nil {
		n += 3
	}
	if m.XXX_unrecognized != nil {
		n += len(m.XXX_unrecognized)
	}
	return n
}

func (m *Metadata) Size() (n int) {
	var l int
	_ = l
	n += 1 + sovEtcdserver(uint64(m.NodeID))
	n += 1 + sovEtcdserver(uint64(m.ClusterID))
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
func (m *Request) Unmarshal(data []byte) error {
	l := len(data)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowEtcdserver
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := data[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: Request: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Request: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field ID", wireType)
			}
			m.ID = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowEtcdserver
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				m.ID |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Method", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowEtcdserver
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthEtcdserver
			}
			postIndex := iNdEx + intStringLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Method = string(data[iNdEx:postIndex])
			iNdEx = postIndex
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Path", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowEtcdserver
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthEtcdserver
			}
			postIndex := iNdEx + intStringLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Path = string(data[iNdEx:postIndex])
			iNdEx = postIndex
		case 4:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Val", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowEtcdserver
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthEtcdserver
			}
			postIndex := iNdEx + intStringLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Val = string(data[iNdEx:postIndex])
			iNdEx = postIndex
		case 5:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Dir", wireType)
			}
			var v int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowEtcdserver
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				v |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.Dir = bool(v != 0)
		case 6:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field PrevValue", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowEtcdserver
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthEtcdserver
			}
			postIndex := iNdEx + intStringLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.PrevValue = string(data[iNdEx:postIndex])
			iNdEx = postIndex
		case 7:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field PrevIndex", wireType)
			}
			m.PrevIndex = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowEtcdserver
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				m.PrevIndex |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 8:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field PrevExist", wireType)
			}
			var v int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowEtcdserver
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				v |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			b := bool(v != 0)
			m.PrevExist = &b
		case 9:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Expiration", wireType)
			}
			m.Expiration = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowEtcdserver
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				m.Expiration |= (int64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 10:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Wait", wireType)
			}
			var v int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowEtcdserver
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				v |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.Wait = bool(v != 0)
		case 11:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Since", wireType)
			}
			m.Since = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowEtcdserver
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				m.Since |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 12:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Recursive", wireType)
			}
			var v int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowEtcdserver
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				v |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.Recursive = bool(v != 0)
		case 13:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Sorted", wireType)
			}
			var v int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowEtcdserver
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				v |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.Sorted = bool(v != 0)
		case 14:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Quorum", wireType)
			}
			var v int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowEtcdserver
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				v |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.Quorum = bool(v != 0)
		case 15:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Time", wireType)
			}
			m.Time = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowEtcdserver
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				m.Time |= (int64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 16:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Stream", wireType)
			}
			var v int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowEtcdserver
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				v |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.Stream = bool(v != 0)
		case 17:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Refresh", wireType)
			}
			var v int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowEtcdserver
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				v |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			b := bool(v != 0)
			m.Refresh = &b
		default:
			iNdEx = preIndex
			skippy, err := skipEtcdserver(data[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthEtcdserver
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.XXX_unrecognized = append(m.XXX_unrecognized, data[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *Metadata) Unmarshal(data []byte) error {
	l := len(data)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowEtcdserver
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := data[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: Metadata: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Metadata: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field NodeID", wireType)
			}
			m.NodeID = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowEtcdserver
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				m.NodeID |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field ClusterID", wireType)
			}
			m.ClusterID = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowEtcdserver
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				m.ClusterID |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipEtcdserver(data[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthEtcdserver
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.XXX_unrecognized = append(m.XXX_unrecognized, data[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipEtcdserver(data []byte) (n int, err error) {
	l := len(data)
	iNdEx := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowEtcdserver
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := data[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowEtcdserver
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if data[iNdEx-1] < 0x80 {
					break
				}
			}
			return iNdEx, nil
		case 1:
			iNdEx += 8
			return iNdEx, nil
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowEtcdserver
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			iNdEx += length
			if length < 0 {
				return 0, ErrInvalidLengthEtcdserver
			}
			return iNdEx, nil
		case 3:
			for {
				var innerWire uint64
				var start int = iNdEx
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return 0, ErrIntOverflowEtcdserver
					}
					if iNdEx >= l {
						return 0, io.ErrUnexpectedEOF
					}
					b := data[iNdEx]
					iNdEx++
					innerWire |= (uint64(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				innerWireType := int(innerWire & 0x7)
				if innerWireType == 4 {
					break
				}
				next, err := skipEtcdserver(data[start:])
				if err != nil {
					return 0, err
				}
				iNdEx = start + next
			}
			return iNdEx, nil
		case 4:
			return iNdEx, nil
		case 5:
			iNdEx += 4
			return iNdEx, nil
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
	}
	panic("unreachable")
}

var (
	ErrInvalidLengthEtcdserver = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowEtcdserver   = fmt.Errorf("proto: integer overflow")
)

var fileDescriptorEtcdserver = []byte{
	// 388 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x09, 0x6e, 0x88, 0x02, 0xff, 0x5c, 0x92, 0xd1, 0xee, 0xd2, 0x30,
	0x14, 0xc6, 0xff, 0xdb, 0xca, 0x60, 0x15, 0x15, 0x1b, 0x62, 0x4e, 0x88, 0x41, 0x43, 0xbc, 0xf0,
	0x4a, 0xdf, 0x01, 0xe1, 0x82, 0x44, 0x0d, 0x82, 0xd1, 0xeb, 0xba, 0x1d, 0xa1, 0x09, 0x5b, 0x47,
	0xdb, 0x4d, 0xde, 0xc0, 0x57, 0xe3, 0xd2, 0x27, 0x30, 0xea, 0x93, 0xd8, 0x16, 0x86, 0xd5, 0x8b,
	0x26, 0xcb, 0xef, 0xfb, 0xce, 0xe9, 0xd7, 0x73, 0x46, 0x47, 0x68, 0xf2, 0x42, 0xa3, 0x6a, 0x51,
	0xbd, 0xac, 0x95, 0x34, 0x92, 0x0d, 0xff, 0x92, 0xfa, 0xf3, 0x64, 0xbc, 0x93, 0x3b, 0xe9, 0x85,
	0x57, 0xee, 0xeb, 0xe2, 0x99, 0x7d, 0x23, 0xb4, 0xbf, 0xc1, 0x63, 0x83, 0xda, 0xb0, 0x31, 0x8d,
	0x57, 0x0b, 0x88, 0x9e, 0x45, 0x2f, 0xc8, 0x9c, 0x9c, 0x7f, 0x3c, 0xbd, 0xdb, 0xc4, 0x62, 0xc1,
	0x9e, 0xd0, 0xf4, 0x2d, 0x9a, 0xbd, 0x2c, 0x20, 0xb6, 0x4a, 0x76, 0x55, 0xd2, 0xd2, 0x33, 0x06,
	0x94, 0xac, 0xb9, 0xd9, 0x43, 0x12, 0x68, 0xa4, 0xb6, 0x84, 0x3d, 0xa6, 0xc9, 0x47, 0x7e, 0x00,
	0x12, 0x08, 0x49, 0xcb, 0x0f, 0x8e, 0x2f, 0x84, 0x82, 0x9e, 0xe5, 0x83, 0x8e, 0x17, 0x42, 0xb1,
	0x19, 0xcd, 0xd6, 0x0a, 0x5b, 0x5b, 0xd3, 0x20, 0xa4, 0x41, 0x55, 0x56, 0x77, 0xb8, 0xf3, 0xac,
	0xaa, 0x02, 0x4f, 0xd0, 0x0f, 0x82, 0x7a, 0x8f, 0xc7, 0x9d, 0x67, 0x79, 0x12, 0xda, 0xc0, 0xe0,
	0x76, 0x4b, 0x74, 0xf1, 0x78, 0xcc, 0x9e, 0x53, 0xba, 0x3c, 0xd5, 0x42, 0x71, 0x23, 0x64, 0x05,
	0x99, 0x35, 0x25, 0xd7, 0x46, 0x14, 0x6f, 0xdc, 0xbd, 0xed, 0x13, 0x17, 0x06, 0x68, 0x10, 0x95,
	0x7c, 0xb5, 0x84, 0x4d, 0x68, 0x6f, 0x2b, 0xaa, 0x1c, 0xe1, 0x5e, 0x90, 0xa1, 0xa7, 0x1d, 0x72,
	0xf7, 0x6f, 0x30, 0x6f, 0x94, 0x16, 0x2d, 0xc2, 0x30, 0x28, 0xcd, 0x54, 0x87, 0xdd, 0x4c, 0xb7,
	0x52, 0x19, 0x2c, 0xe0, 0x7e, 0x60, 0x48, 0xb5, 0x67, 0x4e, 0x7d, 0xdf, 0x48, 0xd5, 0x94, 0xf0,
	0x20, 0x54, 0x8f, 0x9e, 0xb9, 0x54, 0x1f, 0x44, 0x89, 0xf0, 0x30, 0x48, 0x4d, 0x8c, 0x25, 0xbe,
	0xab, 0x51, 0xc8, 0x4b, 0x18, 0xfd, 0xd3, 0xd5, 0x33, 0x36, 0x75, 0x8b, 0xfe, 0xa2, 0x50, 0xef,
	0xe1, 0x51, 0x30, 0x95, 0xbe, 0xba, 0xc0, 0xd9, 0x1b, 0x3a, 0xb0, 0x7b, 0xe6, 0x05, 0x37, 0xdc,
	0x75, 0x7a, 0x27, 0x0b, 0xfc, 0xef, 0x6f, 0x48, 0x2b, 0xcf, 0xdc, 0x0b, 0x5f, 0x1f, 0x1a, 0x6d,
	0x50, 0x59, 0x43, 0x1c, 0x6e, 0x21, 0xef, 0xf0, 0x7c, 0x74, 0xfe, 0x35, 0xbd, 0x3b, 0xff, 0x9e,
	0x46, 0xdf, 0xed, 0xf9, 0x69, 0xcf, 0x9f, 0x00, 0x00, 0x00, 0xff, 0xff, 0xfb, 0x8e, 0x1a, 0x0d,
	0xa0, 0x02, 0x00, 0x00,
}
