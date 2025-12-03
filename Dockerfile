# Stage 1: Builder
FROM golang:1.23 AS builder

WORKDIR /app
# Install dependencies required by confluent-kafka-go (librdkafka)
RUN apt-get update && apt-get install -y unzip build-essential

COPY . .

# REMOVED: CGO_ENABLED=0 (We need CGO for Confluent Kafka)
# The default is CGO_ENABLED=1, which is what we want.
RUN make update-deps && make compile

# Stage 2: Runner
# CHANGED: bullseye -> bookworm-slim (To match golang:1.23's base OS)
FROM debian:bookworm-slim

WORKDIR /app

# We need to install root certificates for SSL/TLS connections
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/raccoon ./raccoon
COPY . .

CMD ["./raccoon"]