FROM golang:1.24

WORKDIR /app
RUN apt-get update && apt-get install unzip  --no-install-recommends --assume-yes
COPY . .
RUN make update-deps && make compile

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=0 /app/raccoon ./raccoon
COPY . .
CMD ["./raccoon"]
