FROM golang:1.23

WORKDIR /app
RUN apt-get update && apt-get install unzip  --no-install-recommends --assume-yes
COPY . .
RUN make update-deps && make compile

#bookworm-slim comparible with golang:1.23 and has glibc 2.34
FROM debian:bookworm-slim
WORKDIR /app
COPY --from=0 /app/raccoon ./raccoon
COPY . .
CMD ["./raccoon"]
