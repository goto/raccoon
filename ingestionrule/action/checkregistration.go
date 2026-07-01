package action

import (
	"context"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/goto/raccoon/config"
	registration "github.com/goto/raccoon/ingestionrule/action/checkregistration"
	"github.com/goto/raccoon/logger"
)

// CheckRegistration drops events that are not present in the store's registered events cache.
type CheckRegistration struct {
	store *registration.Store
}

// NewCheckRegistration creates a new instance of CheckRegistration.
func NewCheckRegistration(
	store *registration.Store,
) *CheckRegistration {
	return &CheckRegistration{
		store: store,
	}
}

// CheckRegistration implements the Action interface. It checks if the events are registered and drops unregistered events.
func (c *CheckRegistration) Apply(
	ctx context.Context,
	events []*pb.Event,
	connGroup string,
) []*pb.Event {
	logger.Info("registration check: applying registration verification")
	// for each event in the batch, check if it's registered
	// if verification flag is disabled or store's registeredEvents len is 0, skip the check and return the events
	registeredEvents, ok := c.store.RegisteredEvents.Load().(map[string]struct{})
	if !ok {
		logger.Warn("registration check: failed to load registered events cache")
		return events
	}
	if !config.PolicyCfg.EnableCheckVerification ||
		len(registeredEvents) == 0 {
		return events
	}

	// for each event in the batch, check if it's registered
	filteredEvents := make([]*pb.Event, 0)
	for _, event := range events {
		// key will be in the format of "publisher:eventname:product"
		publisher, ok := config.PolicyCfg.PublisherMapping[connGroup]
		if !ok {
			logger.Infof("registration check: conn_group %s is not mapped to a publisher, skipping check for event %s", connGroup, event.EventName)
			filteredEvents = append(filteredEvents, event)
			continue
		}
		eventKey := publisher + ":" + event.EventName + ":" + event.Product

		if _, ok := registeredEvents[eventKey]; ok {
			filteredEvents = append(filteredEvents, event)
		} else {
			logger.Infof("registration check: event %s is not registered, dropping event for conn_group %s", eventKey, connGroup)
		}
	}

	return filteredEvents
}
