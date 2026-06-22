package checkregistration

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/logger"
)

// Client defines the interface for making API calls.
type Client interface {
	GetPublishers(ctx context.Context) ([]Publisher, error)

	GetEvents(
		ctx context.Context,
		publisher string,
	) ([]Event, error)
}

// Store is a thread-safe in-memory catalog of registered events.
type Store struct {
	client Client
	// read-mostly data, rare full replacement so using atomic.Value for lock-free reads without needing a mutex
	RegisteredEvents atomic.Value
	Publishers       atomic.Value
}

type publisherResult struct {
	publisher string
	events    []Event
	err       error
}

func NewStore(
	ctx context.Context,
) (*Store, error) {
	client := NewMSLClient(config.PolicyCfg.MSLBaseURL)

	if client == nil {
		logger.Info("registration store: MSL client is nil, registration verification is skipped")
		return nil, nil
	}

	logger.Info("initializing registration store")
	s := &Store{
		client: client,
	}

	s.RegisteredEvents.Store(map[string]struct{}{})

	if err := s.LoadPublishers(ctx); err != nil {
		return nil, err
	}

	if err := s.LoadEvents(ctx); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Store) LoadPublishers(ctx context.Context) error {
	publishers, err := s.client.GetPublishers(ctx)
	if err != nil {
		return err
	}

	s.Publishers.Store(publishers)

	logger.Infof(
		"registration store: loaded %d publishers",
		len(publishers),
	)

	return nil
}

// Load fetches all registered events from MSL and atomically swaps the cache.
func (s *Store) LoadEvents(ctx context.Context) error {
	publishers, ok := s.Publishers.Load().([]Publisher)
	if !ok || len(publishers) == 0 {
		logger.Infof("registration store: no publishers available")
		return nil
	}

	newEvents := make(map[string]struct{})
	results := make(chan publisherResult, len(publishers))

	for _, p := range publishers {
		publisher := p.Name

		go func() {
			events, err := s.client.GetEvents(ctx, publisher)

			results <- publisherResult{
				publisher: publisher,
				events:    events,
				err:       err,
			}
		}()
	}

	for range publishers {
		result := <-results

		if result.err != nil {
			return result.err
		}

		for _, e := range result.events {
			key := result.publisher + ":" + e.EventName + ":" + e.Product
			newEvents[key] = struct{}{}
		}
	}

	s.RegisteredEvents.Store(newEvents)
	// print complete publishers and their events for debugging
	logger.Infof("registration store: loaded registered events: %d", len(newEvents))

	return nil
}

func (s *Store) Refresh(ctx context.Context) {

	// Capture and defensively validate intervals before constructing tickers
	pubInterval := config.PolicyCfg.PublisherRefreshInHours
	if pubInterval <= 0 {
		pubInterval = 24 * time.Hour
	}

	eventInterval := config.PolicyCfg.EventRefreshInMinutes
	if eventInterval <= 0 {
		eventInterval = 30 * time.Minute
	}

	publisherTicker := time.NewTicker(pubInterval)
	eventTicker := time.NewTicker(eventInterval)
	go func() {

		defer publisherTicker.Stop()
		defer eventTicker.Stop()

		for {
			select {

			case <-publisherTicker.C:
				logger.Info("refreshing publishers")

				if err := s.LoadPublishers(ctx); err != nil {
					logger.Errorf(
						"failed to refresh publishers: %v",
						err,
					)
				}

			case <-eventTicker.C:
				logger.Info("refreshing registered events")

				if err := s.LoadEvents(ctx); err != nil {
					logger.Errorf(
						"failed to refresh registered events: %v",
						err,
					)
				}

			case <-ctx.Done():
				return
			}
		}
	}()
}
