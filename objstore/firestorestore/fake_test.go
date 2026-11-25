package firestorestore

import (
	"context"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// fakeFirestore is a gRPC server that answers the requests the real client
// library sends for this package, the way Firestore documents it:
// preconditions, the document size limit, and ordered, filtered and limited
// queries.
type fakeFirestore struct {
	pb.UnimplementedFirestoreServer

	mu     sync.Mutex
	docs   map[string]*pb.Document
	now    time.Time
	pages  int
	counts map[string]int

	// intercept, if set, is called before a request is handled, and after it
	// with done set. An error is returned instead of the reply.
	intercept func(method string, done bool) error

	conn *grpc.ClientConn
}

func newFakeFirestore(t *testing.T) *fakeFirestore {
	t.Helper()
	f := &fakeFirestore{
		docs:   map[string]*pb.Document{},
		now:    time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC),
		counts: map[string]int{},
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer(grpc.MaxRecvMsgSize(16 << 20))
	pb.RegisterFirestoreServer(srv, f)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	f.conn, err = grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), DialOption())
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.conn.Close() })
	return f
}

func (f *fakeFirestore) store(t *testing.T, prefix string) *Store {
	t.Helper()
	client, err := firestore.NewClient(context.Background(), "test-project", option.WithGRPCConn(f.conn))
	require.NoError(t, err)
	s, err := New(client, Config{Project: "test-project", Prefix: prefix})
	require.NoError(t, err)
	return s
}

func (f *fakeFirestore) before(method string) error {
	f.mu.Lock()
	f.counts[method]++
	intercept := f.intercept
	f.mu.Unlock()
	if intercept != nil {
		return intercept(method, false)
	}
	return nil
}

func (f *fakeFirestore) after(method string) error {
	f.mu.Lock()
	intercept := f.intercept
	f.mu.Unlock()
	if intercept != nil {
		return intercept(method, true)
	}
	return nil
}

func (f *fakeFirestore) Commit(ctx context.Context, req *pb.CommitRequest) (*pb.CommitResponse, error) {
	if err := f.before("Commit"); err != nil {
		return nil, err
	}
	resp, err := f.commit(req)
	if err != nil {
		return nil, err
	}
	if err := f.after("Commit"); err != nil {
		return nil, err
	}
	return resp, nil
}

func (f *fakeFirestore) commit(req *pb.CommitRequest) (*pb.CommitResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	resp := &pb.CommitResponse{CommitTime: timestamppb.New(f.now)}
	for _, w := range req.Writes {
		switch op := w.Operation.(type) {
		case *pb.Write_Update:
			doc := op.Update
			if proto.Size(doc) > 1<<20 {
				return nil, status.Errorf(codes.InvalidArgument, "document %s exceeds 1048576 bytes", doc.Name)
			}
			old := f.docs[doc.Name]
			if pre, ok := w.GetCurrentDocument().GetConditionType().(*pb.Precondition_Exists); ok {
				if !pre.Exists && old != nil {
					return nil, status.Errorf(codes.AlreadyExists, "Document already exists: %s", doc.Name)
				}
				if pre.Exists && old == nil {
					return nil, status.Errorf(codes.NotFound, "No document to update: %s", doc.Name)
				}
			}
			stored := proto.Clone(doc).(*pb.Document)
			stored.CreateTime, stored.UpdateTime = timestamppb.New(f.now), timestamppb.New(f.now)
			if old != nil {
				stored.CreateTime = old.CreateTime
			}
			f.docs[doc.Name] = stored
		case *pb.Write_Delete:
			delete(f.docs, op.Delete)
		default:
			return nil, status.Errorf(codes.Unimplemented, "write %T", op)
		}
		resp.WriteResults = append(resp.WriteResults, &pb.WriteResult{UpdateTime: timestamppb.New(f.now)})
	}
	return resp, nil
}

func (f *fakeFirestore) BatchGetDocuments(req *pb.BatchGetDocumentsRequest, stream pb.Firestore_BatchGetDocumentsServer) error {
	if err := f.before("BatchGetDocuments"); err != nil {
		return err
	}
	for _, name := range req.Documents {
		f.mu.Lock()
		doc, at := f.docs[name], timestamppb.New(f.now)
		f.mu.Unlock()
		resp := &pb.BatchGetDocumentsResponse{ReadTime: at}
		if doc != nil {
			resp.Result = &pb.BatchGetDocumentsResponse_Found{Found: doc}
		} else {
			resp.Result = &pb.BatchGetDocumentsResponse_Missing{Missing: name}
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeFirestore) RunQuery(req *pb.RunQueryRequest, stream pb.Firestore_RunQueryServer) error {
	if err := f.before("RunQuery"); err != nil {
		return err
	}
	docs, at, err := f.query(req)
	if err != nil {
		return err
	}
	for _, doc := range docs {
		if err := stream.Send(&pb.RunQueryResponse{Document: doc, ReadTime: at}); err != nil {
			return err
		}
	}
	return stream.Send(&pb.RunQueryResponse{ReadTime: at})
}

// key is what a query on one field compares: a string field's value, or the
// document's name.
func key(doc *pb.Document, field string) (string, bool) {
	if field == "__name__" {
		return doc.Name, true
	}
	v, ok := doc.Fields[field].GetValueType().(*pb.Value_StringValue)
	if !ok {
		return "", false
	}
	return v.StringValue, true
}

func (f *fakeFirestore) query(req *pb.RunQueryRequest) ([]*pb.Document, *timestamppb.Timestamp, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pages++
	q := req.GetStructuredQuery()
	if len(q.GetFrom()) != 1 || q.From[0].AllDescendants {
		return nil, nil, status.Error(codes.Unimplemented, "from")
	}
	if len(q.GetOrderBy()) == 0 || q.OrderBy[0].Direction == pb.StructuredQuery_DESCENDING {
		return nil, nil, status.Error(codes.Unimplemented, "order")
	}
	order := q.OrderBy[0].Field.FieldPath
	var filters []*pb.StructuredQuery_FieldFilter
	switch w := q.GetWhere().GetFilterType().(type) {
	case nil:
	case *pb.StructuredQuery_Filter_FieldFilter:
		filters = append(filters, w.FieldFilter)
	default:
		return nil, nil, status.Errorf(codes.Unimplemented, "filter %T", w)
	}
	prefix := req.Parent + "/" + q.From[0].CollectionId + "/"
	var out []*pb.Document
docs:
	for name, doc := range f.docs {
		if id, ok := strings.CutPrefix(name, prefix); !ok || strings.Contains(id, "/") {
			continue
		}
		k, ok := key(doc, order)
		if !ok {
			continue
		}
		for _, flt := range filters {
			if flt.Field.FieldPath != order {
				return nil, nil, status.Error(codes.FailedPrecondition, "The query requires an index.")
			}
			v := flt.Value.GetStringValue()
			switch flt.Op {
			case pb.StructuredQuery_FieldFilter_GREATER_THAN:
				ok = k > v
			case pb.StructuredQuery_FieldFilter_GREATER_THAN_OR_EQUAL:
				ok = k >= v
			default:
				return nil, nil, status.Errorf(codes.Unimplemented, "operator %v", flt.Op)
			}
			if !ok {
				continue docs
			}
		}
		if c := q.GetStartAt(); c != nil {
			v := c.Values[0].GetStringValue()
			if order == "__name__" {
				v = c.Values[0].GetReferenceValue()
			}
			if k < v || (!c.Before && k == v) {
				continue
			}
		}
		if fields := q.GetSelect().GetFields(); fields != nil {
			p := &pb.Document{Name: doc.Name, CreateTime: doc.CreateTime, UpdateTime: doc.UpdateTime, Fields: map[string]*pb.Value{}}
			for _, sel := range fields {
				if v, ok := doc.Fields[sel.FieldPath]; ok {
					p.Fields[sel.FieldPath] = v
				}
			}
			doc = p
		}
		out = append(out, doc)
	}
	slices.SortFunc(out, func(a, b *pb.Document) int {
		x, _ := key(a, order)
		y, _ := key(b, order)
		return strings.Compare(x, y)
	})
	limit := q.GetLimit().GetValue()
	if limit <= 0 {
		return nil, nil, status.Error(codes.Internal, fmt.Sprintf("query without a limit: %v", q))
	}
	if len(out) > int(limit) {
		out = out[:limit]
	}
	return out, timestamppb.New(f.now), nil
}
