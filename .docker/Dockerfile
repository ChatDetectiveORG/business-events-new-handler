# syntax=docker/dockerfile:1.4

# --- Build stage ---
FROM golang:latest AS builder
WORKDIR /workspace
COPY . ./
WORKDIR /workspace/business-events-new-handler
RUN CGO_ENABLED=0 GOOS=linux go build -o /workspace/app ./src

# --- Production image ---
FROM alpine:3.19 AS prod
WORKDIR /app
COPY --from=builder /workspace/app /app/

# Добавляем ca-certificates для HTTPS запросов
RUN apk --no-cache add ca-certificates

# HEALTHCHECK --interval=30s --timeout=5s --start-period=30s CMD wget --no-verbose --tries=1 --spider http://localhost:8080/health || exit 1
ENTRYPOINT ["/app/app"]

# --- Development image ---
FROM golang:latest AS dev
WORKDIR /workspace

# Установка Air для hot reload
RUN go install github.com/air-verse/air@latest

COPY . ./
WORKDIR /workspace/business-events-new-handler

# Air будет запущен из docker-compose с примонтированным volume
CMD ["air"]