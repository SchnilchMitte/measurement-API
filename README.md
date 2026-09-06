# Simple Measurement API

This is intentionally small: one Go file, one measurement at a time.

It starts both measurements from one `/start` request:

- `docker stats` for exactly `kafka` or `nats`
- `sar` for the complete remote server

Both sample approximately once per second and use the same server clock.

## Server setup

Install sysstat once:

```bash
sudo apt update
sudo apt install -y sysstat
```

Build and start:

```bash
go build -o measurement-api .
./measurement-api
```

## Start Kafka measurement

```bash
curl 'http://127.0.0.1:7070/start?container=kafka'
```

## Start NATS measurement

```bash
curl 'http://127.0.0.1:7070/start?container=nats'
```

## Stop measurement

```bash
curl 'http://127.0.0.1:7070/stop'
```

The result folder looks like:

```text
measurements/kafka-20260907-120000/
├── container.csv   # only Kafka container resources
└── host.txt        # complete server resources
```
