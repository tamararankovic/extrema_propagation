FROM golang:1.24-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod tidy

COPY . .

RUN go build -o node main.go

FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/node /usr/local/bin/node

EXPOSE 8000/udp

ENTRYPOINT ["/usr/local/bin/node"]
