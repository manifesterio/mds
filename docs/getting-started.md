# Getting started

You will run the all-in-one demo image and produce then consume one message.
Containerfile and Makefile detail lives in
[`deploy/container/README.md`](../deploy/container/README.md).

## Prerequisites

- [Podman](https://podman.io/)
- On macOS, run `podman machine start`

## Run `mds-all`

One container starts the control plane with `-store mem` and the broker with
`-objects mem`. State is lost when the container stops. Do not use this image
for production.

1. If you are on macOS, start the Podman machine:

       podman machine start

2. From the repo root, build the demo image:

       podman build -f deploy/container/mds-all.Containerfile -t mds-all:dev .

   Or run `make image-all`.

3. Run the container and publish the ports:

       podman run --rm --name mds-all \
         -p 9092:9092 -p 8080:8080 -p 9100:9100 -p 9101:9101 \
         mds-all:dev

   You should see log lines that the control plane listens on `:8080` and the
   broker listens on `0.0.0.0:9092` with advertised `127.0.0.1:9092`.

4. In another terminal, check metrics:

       curl -s localhost:9101/metrics
       curl -s localhost:9100/metrics

   Each response is Prometheus text. Something should accept TCP on
   `localhost:9092`.

5. Produce and consume with Apache Kafka 3.x console scripts on your PATH:

       kafka-topics.sh --bootstrap-server 127.0.0.1:9092 \
         --create --topic demo --partitions 1 --replication-factor 1

       echo 'hello' | kafka-console-producer.sh \
         --bootstrap-server 127.0.0.1:9092 --topic demo

       kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 \
         --topic demo --from-beginning --timeout-ms 5000

   You should see `hello` on the consumer. Stop the container with Ctrl-C or
   `podman stop mds-all`.

Turn off idempotent produce if your client defaults it on
(`enable.idempotence=false`). InitProducerId is not served in 0.1.0.

## What you ran

In-memory log store and in-memory object store in one container. For durable
stores and separate images, see [operations.md](operations.md).

## Next

- [Architecture](architecture.md)
- [Operations](operations.md)
