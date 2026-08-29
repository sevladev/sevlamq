.PHONY: cluster broker-1 broker-2 broker-3

cluster:
	cargo build --bin sevlamqd
	@set -u; \
	RUST_LOG="$${RUST_LOG:-info}" target/debug/sevlamqd config/cluster/broker-1.toml & broker_1=$$!; \
	RUST_LOG="$${RUST_LOG:-info}" target/debug/sevlamqd config/cluster/broker-2.toml & broker_2=$$!; \
	RUST_LOG="$${RUST_LOG:-info}" target/debug/sevlamqd config/cluster/broker-3.toml & broker_3=$$!; \
	trap 'kill "$$broker_1" "$$broker_2" "$$broker_3" 2>/dev/null || true; wait "$$broker_1" "$$broker_2" "$$broker_3" 2>/dev/null || true' INT TERM EXIT; \
	wait

broker-1:
	cargo run --bin sevlamqd -- config/cluster/broker-1.toml

broker-2:
	cargo run --bin sevlamqd -- config/cluster/broker-2.toml

broker-3:
	cargo run --bin sevlamqd -- config/cluster/broker-3.toml
