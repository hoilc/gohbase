package hrpc

import (
	"context"
	"github.com/tsuna/gohbase/pb"
	"google.golang.org/protobuf/proto"
)

func NewListNamespaces(ctx context.Context,
	options ...func(createNamespace *ListNamespaces)) *ListNamespaces {
	return &ListNamespaces{
		base: base{
			ctx:      ctx,
			resultch: make(chan RPCResult, 1),
		},
	}
}

// ListNamespaces represents a ListNamespaces HBase call
type ListNamespaces struct {
	base
}

func (cn ListNamespaces) Name() string {
	return "ListNamespaceDescriptors"
}

func (cn ListNamespaces) ToProto() proto.Message {
	return &pb.ListNamespaceDescriptorsRequest{}
}

func (cn ListNamespaces) NewResponse() proto.Message {
	return &pb.ListNamespaceDescriptorsResponse{}
}

func (cn ListNamespaces) Description() string {
	return cn.Name()
}
