FROM golang:1.24-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /out/redis-go ./cmd/main.go

FROM alpine:3.20

RUN adduser -D -u 10001 redisgo && mkdir -p /data && chown redisgo:redisgo /data
USER redisgo
WORKDIR /app

COPY --from=build /out/redis-go /usr/local/bin/redis-go
COPY sharding.toml sharding.docker.toml /app/

EXPOSE 6380 8080
ENTRYPOINT ["redis-go"]
