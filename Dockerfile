# LiteMQ Dockerfile

# Build stage
FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build NameServer
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o nameserver ./cmd/nameserver

# Build Broker
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o broker ./cmd/broker

# Final stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates
WORKDIR /root/

# Copy binaries
COPY --from=builder /app/nameserver .
COPY --from=builder /app/broker .

# Create data directory
RUN mkdir -p /app/data

# Expose ports
EXPOSE 9876 10911 8080

# Default command
CMD ["./nameserver"]
