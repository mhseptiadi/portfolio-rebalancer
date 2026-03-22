FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/api ./cmd/api
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/worker ./cmd/worker

FROM alpine:latest AS api-runner

WORKDIR /app

COPY --from=builder /bin/api .

EXPOSE 8083

CMD ["./api"]

FROM alpine:latest AS worker-runner

WORKDIR /app

COPY --from=builder /bin/worker .

CMD ["./worker"]
