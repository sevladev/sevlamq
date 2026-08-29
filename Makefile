.PHONY: cluster

cluster:
	cargo build --bin sevlamqd
	@set -eu; \
	RUST_LOG="$${RUST_LOG:-info}" target/debug/sevlamqd config/cluster/broker-1.toml & broker_1=$$!; \
	RUST_LOG="$${RUST_LOG:-info}" target/debug/sevlamqd config/cluster/broker-2.toml & broker_2=$$!; \
	RUST_LOG="$${RUST_LOG:-info}" target/debug/sevlamqd config/cluster/broker-3.toml & broker_3=$$!; \
	trap 'kill "$$broker_1" "$$broker_2" "$$broker_3" 2>/dev/null || true; wait "$$broker_1" "$$broker_2" "$$broker_3" 2>/dev/null || true' INT TERM EXIT; \
	wait "$$broker_1" "$$broker_2" "$$broker_3"
