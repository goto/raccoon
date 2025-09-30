package config

import (
	"time"

	"github.com/goto/raccoon/config/util"

	"github.com/spf13/viper"
)

var Stats statsConfig

type statsConfig struct {
	FlushInterval time.Duration
	TopicName     string
	ChannelSize   int
}

func statsConfigLoader() {
	viper.SetDefault("STATS_TOPIC_NAME", "clickstream-total-event-log")
	viper.SetDefault("STATS_FLUSH_INTERVAL_IN_SEC", 10)
	viper.SetDefault("STATS_CHANNEL_SIZE", "1000")
	Stats = statsConfig{
		TopicName:     util.MustGetString("STATS_TOPIC_NAME"),
		FlushInterval: util.MustGetDuration("STATS_FLUSH_INTERVAL_IN_SEC", time.Second),
		ChannelSize:   util.MustGetInt("STATS_CHANNEL_SIZE"),
	}
}
