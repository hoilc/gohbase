package hrpc

import (
	"context"
	"errors"

	"github.com/tsuna/gohbase/pb"
	"google.golang.org/protobuf/proto"
)

// ListTableDescriptors models a ListTableDescriptors pb call
type ListTableDescriptors struct {
	base
	regex            string
	includeSysTables bool
	namespace        string
	names            []*pb.TableName
}

// ListTableDescriptorsNames sets a regex for ListTableDescriptors
func ListTableDescriptorsNames(names []*pb.TableName) func(Call) error {
	return func(c Call) error {
		l, ok := c.(*ListTableDescriptors)
		if !ok {
			return errors.New("ListTableDescriptorsNames option can only be used with ListTableDescriptors")
		}
		l.names = names
		return nil
	}
}

// ListRegex sets a regex for ListTableDescriptors
func ListTableDescriptorsRegex(regex string) func(Call) error {
	return func(c Call) error {
		l, ok := c.(*ListTableDescriptors)
		if !ok {
			return errors.New("ListTableDescriptorsRegex option can only be used with ListTableDescriptors")
		}
		l.regex = regex
		return nil
	}
}

// ListNamespace sets a namespace for ListTableDescriptors
func ListTableDescriptorsNamespace(ns string) func(Call) error {
	return func(c Call) error {
		l, ok := c.(*ListTableDescriptors)
		if !ok {
			return errors.New("ListTableDescriptorsNamespace option can only be used with ListTableDescriptors")
		}
		l.namespace = ns
		return nil
	}
}

// ListSysTables includes sys tables for ListTableDescriptors
func ListTableDescriptorsSysTables(b bool) func(Call) error {
	return func(c Call) error {
		l, ok := c.(*ListTableDescriptors)
		if !ok {
			return errors.New("ListTableDescriptorsSysTables option can only be used with ListTableDescriptors")
		}
		l.includeSysTables = b
		return nil
	}
}

// NewListTableDescriptors creates a new GetTableDescriptors request that will list tables in hbase.
//
// By default matchs all tables. Use the options (ListRegex, ListNamespace, ListSysTables) to
// set non default behaviour.
func NewListTableDescriptors(ctx context.Context, opts ...func(Call) error) (*ListTableDescriptors, error) {
	tn := &ListTableDescriptors{
		base: base{
			ctx:      ctx,
			resultch: make(chan RPCResult, 1),
		},
		regex: ".*",
	}
	if err := applyOptions(tn, opts...); err != nil {
		return nil, err
	}
	return tn, nil
}

// Name returns the name of this RPC call.
func (tn *ListTableDescriptors) Name() string {
	return "GetTableDescriptors"
}

// Description returns the description of this RPC call.
func (tn *ListTableDescriptors) Description() string {
	return tn.Name()
}

// ToProto converts the RPC into a protobuf message.
func (tn *ListTableDescriptors) ToProto() proto.Message {
	return &pb.GetTableDescriptorsRequest{
		TableNames:       tn.names,
		Regex:            proto.String(tn.regex),
		IncludeSysTables: proto.Bool(tn.includeSysTables),
		Namespace:        proto.String(tn.namespace),
	}
}

// NewResponse creates an empty protobuf message to read the response of this
// RPC.
func (tn *ListTableDescriptors) NewResponse() proto.Message {
	return &pb.GetTableDescriptorsResponse{}
}
