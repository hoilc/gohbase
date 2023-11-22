package hrpc

import (
	"context"
	"github.com/tsuna/gohbase/pb"
	"google.golang.org/protobuf/proto"
)

func NewDeleteNamespace(ctx context.Context, namespace string,
	options ...func(deleteNamespace *DeleteNamespace)) *DeleteNamespace {
	return &DeleteNamespace{
		base: base{
			ctx:      ctx,
			resultch: make(chan RPCResult, 1),
		},
		name: namespace,
	}
}

// DeleteNamespace represents a DeleteNamespace HBase call
type DeleteNamespace struct {
	base
	name string
}

func (dn DeleteNamespace) Name() string {
	return "DeleteNamespace"
}

func (dn DeleteNamespace) ToProto() proto.Message {
	return &pb.DeleteNamespaceRequest{
		NamespaceName: &dn.name,
	}
}

func (dn DeleteNamespace) NewResponse() proto.Message {
	return &pb.DeleteNamespaceResponse{}
}

func (dn DeleteNamespace) Description() string {
	return dn.Name()
}
