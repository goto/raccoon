package collection

import (
	"context"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/goto/raccoon/identification"
	"github.com/goto/raccoon/model"
)

type AckFunc func(err error)

type CollectRequest struct {
	ConnectionIdentifier identification.Identifier
	TimeConsumed         time.Time
	TimePushed           time.Time
	AckFunc
	SentTime *timestamppb.Timestamp
	Events   []*model.EventWithMetadata
}

type Collector interface {
	Collect(ctx context.Context, req *CollectRequest) error
}
