FROM golang:1.25 AS builder

WORKDIR /src
COPY go.mod ./
COPY cmd/ cmd/
COPY internal/ internal/

RUN CGO_ENABLED=0 go build -o /out/gobitmq ./cmd

FROM gcr.io/distroless/base-debian12

WORKDIR /app
COPY --from=builder /out/gobitmq /app/gobitmq

ENV GOBITMQ_AMQP_ADDR=":5672"
ENV GOBITMQ_MGMT_ADDR=":15672"
ENV GOBITMQ_DATA_DIR="/data/broker"

EXPOSE 5672 15672
VOLUME ["/data"]

ENTRYPOINT ["/app/gobitmq"]
