FROM golang:latest AS builder

WORKDIR /app

COPY extrema_propagation/go.mod extrema_propagation/go.sum ./

COPY hyparview ../hyparview

RUN go mod download

COPY extrema_propagation .

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o /main .

FROM alpine:latest

WORKDIR /app

RUN mkdir -p /var/log/fu

# Copy Go binaries
COPY --from=builder  /main  ./main

CMD [ "/app/main" ]