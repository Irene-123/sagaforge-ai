# Multi-stage build — one Dockerfile for all services
# Build target is controlled by --build-arg SERVICE=<name>

FROM golang:1.25-alpine AS builder
ARG SERVICE
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/service ./cmd/${SERVICE}

FROM alpine:3.20
RUN apk --no-cache add ca-certificates tzdata
COPY --from=builder /bin/service /service
COPY migrations/ /migrations/
ENTRYPOINT ["/service"]
