# Build Stage
FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o /praetor-scheduler .

# Run Stage
FROM alpine:3.24

WORKDIR /

COPY --from=builder /praetor-scheduler /praetor-scheduler

USER 1000:1000

CMD ["/praetor-scheduler"]
