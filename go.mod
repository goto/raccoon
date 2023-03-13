module github.com/goto/raccoon

go 1.14

require (
	buf.build/gen/go/gotocompany/proton/grpc/go v1.3.0-20230313110213-9a3d240d5293.1
	buf.build/gen/go/gotocompany/proton/protocolbuffers/go v1.29.0-20230313110213-9a3d240d5293.1
	github.com/gorilla/mux v1.7.4
	github.com/gorilla/websocket v1.4.2
	github.com/sirupsen/logrus v1.6.0
	github.com/spf13/viper v1.7.0
	github.com/stretchr/testify v1.8.1
	google.golang.org/grpc v1.53.0
	google.golang.org/protobuf v1.29.0
	gopkg.in/alexcesaro/statsd.v2 v2.0.0
	gopkg.in/confluentinc/confluent-kafka-go.v1 v1.4.2
)

require github.com/confluentinc/confluent-kafka-go v1.4.2 // indirect
