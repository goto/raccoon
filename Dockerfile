FROM golang:1.24

WORKDIR /app
RUN apt-get update && apt-get install -y unzip librdkafka-dev --no-install-recommends
COPY . .
RUN make update-deps && make compile

#bookworm-slim comparible with golang:1.24 and has glibc 2.34
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y librdkafka1 --no-install-recommends && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY . .
COPY --from=0 /app/raccoon ./raccoon
CMD ["./raccoon"]
