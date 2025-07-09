FROM golang:1.24 AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go mod tidy
RUN go build -o producer ./log-generator/main.go
RUN go build -o consumer ./stream-processor/main.go
FROM debian:bookworm-slim

WORKDIR /app
COPY --from=builder /app/producer .
COPY --from=builder /app/consumer .

EXPOSE 2113
EXPOSE 2114

ENTRYPOINT ["sh", "-c"]
CMD ["./producer"]
