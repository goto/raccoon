package protoutil

import (
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/goto/raccoon/ingestionrule/schemaregistry/protoutil/pb"
)

func TestGetFieldValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		setupMsg func() protoreflect.Message
		path     []string
		want     any
		wantErr  string
	}{
		{
			name: "single field - string",
			setupMsg: func() protoreflect.Message {
				return (&pb.Event{Name: "test-event"}).ProtoReflect()
			},
			path:    []string{"name"},
			want:    "test-event",
			wantErr: "",
		},
		{
			name: "single field - int32",
			setupMsg: func() protoreflect.Message {
				return (&pb.Event{Id: 42}).ProtoReflect()
			},
			path:    []string{"id"},
			want:    int32(42),
			wantErr: "",
		},
		{
			name: "single field - enum",
			setupMsg: func() protoreflect.Message {
				return (&pb.Event{Product: pb.Product_Generic}).ProtoReflect()
			},
			path:    []string{"product"},
			want:    protoreflect.EnumNumber(1),
			wantErr: "",
		},
		{
			name: "empty path returns error",
			setupMsg: func() protoreflect.Message {
				return (&pb.Event{Name: "test"}).ProtoReflect()
			},
			path:    []string{},
			want:    nil,
			wantErr: "path cannot be empty",
		},
		{
			name: "field not found returns error",
			setupMsg: func() protoreflect.Message {
				return (&pb.Event{Name: "test"}).ProtoReflect()
			},
			path:    []string{"non_existent"},
			want:    nil,
			wantErr: "field \"non_existent\" not found in path",
		},
		{
			name: "message field returns error",
			setupMsg: func() protoreflect.Message {
				return (&pb.Event{EventTimestamp: timestamppb.Now()}).ProtoReflect()
			},
			path:    []string{"event_timestamp"},
			want:    nil,
			wantErr: "final field \"event_timestamp\" is a message, expected a scalar value",
		},
		{
			name: "successful nested traversal",
			setupMsg: func() protoreflect.Message {
				ts := timestamppb.New(time.Unix(100, 0))
				return (&pb.Event{EventTimestamp: ts}).ProtoReflect()
			},
			path:    []string{"event_timestamp", "seconds"},
			want:    int64(100),
			wantErr: "",
		},
		{
			name: "traversal fails on primitive intermediate",
			setupMsg: func() protoreflect.Message {
				return (&pb.Event{Name: "test"}).ProtoReflect()
			},
			path:    []string{"name", "subfield"},
			want:    nil,
			wantErr: "intermediate field \"name\" is not a message",
		},
		{
			name: "traversal fails on nil intermediate message",
			setupMsg: func() protoreflect.Message {
				return (&pb.Event{Id: 1}).ProtoReflect()
			},
			path:    []string{"event_timestamp", "seconds"},
			want:    nil,
			wantErr: "intermediate message \"event_timestamp\" is not valid or not set",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			msg := tt.setupMsg()
			got, err := GetFieldValue(msg, tt.path)

			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("GetFieldValue(%v) expected error containing %q, got nil", tt.path, tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("GetFieldValue(%v) error = %q, want error containing %q", tt.path, err.Error(), tt.wantErr)
				}
				return
			}

			if err != nil {
				t.Fatalf("GetFieldValue(%v) unexpected error: %v", tt.path, err)
			}

			if got != tt.want {
				t.Errorf("GetFieldValue(%v) = %v (type %T), want %v (type %T)",
					tt.path, got, got, tt.want, tt.want)
			}
		})
	}
}

func TestGetEnumStringValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setupMsg  func() protoreflect.Message
		fieldName string
		want      string
		wantErr   string
	}{
		{
			name: "valid enum - Generic",
			setupMsg: func() protoreflect.Message {
				return (&pb.Event{Product: pb.Product_Generic}).ProtoReflect()
			},
			fieldName: "product",
			want:      "Generic",
			wantErr:   "",
		},
		{
			name: "valid enum - Unknown (zero value)",
			setupMsg: func() protoreflect.Message {
				return (&pb.Event{Product: pb.Product_Unknown}).ProtoReflect()
			},
			fieldName: "product",
			want:      "Unknown",
			wantErr:   "",
		},
		{
			name: "empty enum field defaults to zero value",
			setupMsg: func() protoreflect.Message {
				return (&pb.Event{Name: "test-name", Id: 123}).ProtoReflect()
			},
			fieldName: "product",
			want:      "Unknown",
			wantErr:   "",
		},
		{
			name: "invalid enum number falls back to number string",
			setupMsg: func() protoreflect.Message {
				return (&pb.Event{Product: pb.Product(99)}).ProtoReflect()
			},
			fieldName: "product",
			want:      "99",
			wantErr:   "",
		},
		{
			name: "field not found returns error",
			setupMsg: func() protoreflect.Message {
				return (&pb.Event{Product: pb.Product_Generic}).ProtoReflect()
			},
			fieldName: "non_existent_field",
			want:      "",
			wantErr:   "field \"non_existent_field\" does not exist",
		},
		{
			name: "field is not an enum - string type",
			setupMsg: func() protoreflect.Message {
				return (&pb.Event{Name: "test"}).ProtoReflect()
			},
			fieldName: "name",
			want:      "",
			wantErr:   "field \"name\" is not an enum type",
		},
		{
			name: "field is not an enum - int32 type",
			setupMsg: func() protoreflect.Message {
				return (&pb.Event{Id: 123}).ProtoReflect()
			},
			fieldName: "id",
			want:      "",
			wantErr:   "field \"id\" is not an enum type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			msg := tt.setupMsg()
			got, err := GetEnumStringValue(msg, tt.fieldName)

			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("GetEnumStringValue(%s) expected error containing %q, got nil", tt.fieldName, tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("GetEnumStringValue(%s) error = %q, want error containing %q", tt.fieldName, err.Error(), tt.wantErr)
				}
				return
			}

			if err != nil {
				t.Fatalf("GetEnumStringValue(%s) unexpected error: %v", tt.fieldName, err)
			}

			if got != tt.want {
				t.Errorf("GetEnumStringValue(%s) = %q, want %q", tt.fieldName, got, tt.want)
			}
		})
	}
}

func TestGetTimestampFieldValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setupMsg  func() protoreflect.Message
		fieldName string
		want      time.Time
		wantErr   string
	}{
		{
			name: "valid timestamp",
			setupMsg: func() protoreflect.Message {
				ts := timestamppb.New(time.Unix(12345, 67890))
				return (&pb.Event{EventTimestamp: ts}).ProtoReflect()
			},
			fieldName: "event_timestamp",
			want:      time.Unix(12345, 67890),
			wantErr:   "",
		},
		{
			name: "field not found",
			setupMsg: func() protoreflect.Message {
				return (&pb.Event{}).ProtoReflect()
			},
			fieldName: "non_existent_field",
			want:      time.Time{},
			wantErr:   "field \"non_existent_field\" not found",
		},
		{
			name: "field is not a message (got string)",
			setupMsg: func() protoreflect.Message {
				return (&pb.Event{Name: "test"}).ProtoReflect()
			},
			fieldName: "name",
			want:      time.Time{},
			wantErr:   "field \"name\" is not a valid google.protobuf.Timestamp",
		},
		{
			name: "field is not a google.protobuf.Timestamp (got NestedMessage)",
			setupMsg: func() protoreflect.Message {
				return (&pb.Event{Nested: &pb.NestedMessage{Field1: "val"}}).ProtoReflect()
			},
			fieldName: "nested",
			want:      time.Time{},
			wantErr:   "field \"nested\" is not a valid google.protobuf.Timestamp",
		},
		{
			name: "field timestamp is nil/unset",
			setupMsg: func() protoreflect.Message {
				return (&pb.Event{}).ProtoReflect()
			},
			fieldName: "event_timestamp",
			want:      time.Unix(0, 0),
			wantErr:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			msg := tt.setupMsg()
			got, err := GetTimestampFieldValue(msg, tt.fieldName)

			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("GetTimestampFieldValue(%s) expected error containing %q, got nil", tt.fieldName, tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("GetTimestampFieldValue(%s) error = %q, want error containing %q", tt.fieldName, err.Error(), tt.wantErr)
				}
				return
			}

			if err != nil {
				t.Fatalf("GetTimestampFieldValue(%s) unexpected error: %v", tt.fieldName, err)
			}

			if !got.Equal(tt.want) {
				t.Errorf("GetTimestampFieldValue(%s) = %v, want %v", tt.fieldName, got, tt.want)
			}
		})
	}
}

func TestProtoTimestampToTime_Error(t *testing.T) {
	t.Parallel()

	msg := (&pb.NestedMessage{Field1: "test"}).ProtoReflect()
	_, err := protoTimestampToTime(msg)

	wantErr := "message does not have the expected 'seconds' and 'nanos' fields"
	if err == nil {
		t.Fatalf("expected error containing %q, got nil", wantErr)
	}
	if !strings.Contains(err.Error(), wantErr) {
		t.Errorf("error = %q, want error containing %q", err.Error(), wantErr)
	}
}
