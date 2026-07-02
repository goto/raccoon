package eventchecker

import (
	"context"

	"github.com/goto/raccoon/ingestionrule/synccache"
)

func NewTestEventCache(registeredEvents map[string]EventStatus) *EventCache {
	s := &EventCache{}
	s.cache = synccache.NewCache(
		context.Background(),
		"test store",
		func(ctx context.Context) (map[string]EventStatus, error) { return registeredEvents, nil },
		0,
		registeredEvents,
	)
	return s
}
