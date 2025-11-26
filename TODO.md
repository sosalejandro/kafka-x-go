# TODO

- [x] Create main for consumer testing
  - Create DockerImage
- [x] Create main for producer testing
  - Create DockerImage
- [] Create main for consumer/producer/dlq testing
  - Create DockerImage
- [x] Test solution
- [x] Create docker compose for OTEL and Metrics Observation
- [x] Add SetStatus on errors to the spans (OTEL)
  - [x] Double Check
- [x] Check Producer metrics to include attributes on every metric so it differentiate from every other call.
  - [x] Double Check
- [x] Check Consumer metrics to include attributes on every metric so it differntiates from every other call.
  - [x] Double Check

##

```sh
 kafka-topics --create --topic test-consumer --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1 || echo 'Topic already exists'
```