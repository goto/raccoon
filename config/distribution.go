package config

import (
	"github.com/goto/raccoon/config/util"

	"github.com/spf13/viper"
)

var EventDistribution eventDistribution

type eventDistribution struct {
	PublisherPattern             string
	NotExclusivePublisherPattern string
}

func eventDistributionConfigLoader() {
	viper.SetDefault("EVENT_DISTRIBUTION_PUBLISHER_PATTERN", "clickstream-%s-log")
	viper.SetDefault("NOT_EXCLUSIVE_EVENT_DISTRIBUTION_PUBLISHER_PATTERN", "clickstream-courier-%s-log")
	EventDistribution = eventDistribution{
		PublisherPattern:             util.MustGetString("EVENT_DISTRIBUTION_PUBLISHER_PATTERN"),
		NotExclusivePublisherPattern: util.MustGetString("NOT_EXCLUSIVE_EVENT_DISTRIBUTION_PUBLISHER_PATTERN"),
	}
}
