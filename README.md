# mqtt-conformance

A Rust-based conformance testing tool that validates MQTT brokers against OASIS MQTT 3.1.1 and 5.0 specifications. Tests map directly to normative statements from the RFC (e.g., `MQTT-3.1.0-1`).

## Installation

### From releases

Download the latest binary from the [releases page](https://github.com/vibesrc/mqttconformance/releases).

### From source

```bash
cargo build --release
```

Binary will be at `target/release/mqtt-conformance`.

## Usage

### Run all tests

```bash
# Against local broker (default: localhost:1883)
mqtt-conformance run

# Against remote broker
mqtt-conformance run -H broker.example.com -p 1883

# With authentication
mqtt-conformance run -H broker.example.com -u username -P password
```

### Select MQTT version

```bash
# MQTT 3.1.1 (default)
mqtt-conformance run -v 3

# MQTT 5.0
mqtt-conformance run -v 5
```

### Run specific tests

```bash
# Run tests for a specific section
mqtt-conformance run --section connect

# Run a specific normative test
mqtt-conformance run --normative MQTT-3.1.0-1
```

### List available tests

```bash
# List all tests
mqtt-conformance list

# List tests for a specific section
mqtt-conformance list --section publish

# List tests for MQTT 5.0
mqtt-conformance list -v 5
```

## Test Coverage

| Version | Tests | Sections |
|---------|-------|----------|
| MQTT 3.1.1 | 137 | 13 |
| MQTT 5.0 | 123 | 16 |

Tests cover: CONNECT, CONNACK, PUBLISH, PUBACK, PUBREC, PUBREL, PUBCOMP, SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK, PINGREQ, PINGRESP, DISCONNECT, and protocol-level requirements.

## Local Development

Start a test broker with Docker:

```bash
docker compose -f ./test/docker-compose-mosquitto.yml up -d
```

This runs Mosquitto on `localhost:1883` with anonymous connections enabled.

Run the test suite:

```bash
cargo run --release -- run
```

## License

MIT
