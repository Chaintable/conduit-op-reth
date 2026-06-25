# Chaintable write node

> Fork of [conduit-xyz/conduit-op-reth](https://github.com/conduit-xyz/conduit-op-reth), with Chaintable pipeline patches.

## Architecture

This repo runs the chain's execution layer with the [Chaintable pipeline](https://github.com/Chaintable/pipeline) tracer embedded. The tracer extracts block data — block headers, transactions, call traces, receipts, events, and state diffs — and ships it to **S3 + Kafka** (see pipeline's [architecture](https://github.com/Chaintable/pipeline/blob/main/docs/architecture.md)). Two consumption paths:

- **Block headers + state diffs** → Kafka + S3 → [leafage-evm](https://github.com/Chaintable/leafage-evm): a lightweight EVM executor serving state queries (`eth_call`, `eth_estimateGas`, …), no P2P sync, no tx storage (see its [architecture](https://github.com/Chaintable/leafage-evm#architecture)).
- **Block files** (transactions · call traces · receipts · events) → S3 → Chaintable's transaction/trace indexing pipeline.

```
Chaintable write node (this repo · producer, embeds pipeline tracer)
        │
        ├─ block headers + state diffs ──────────────────→ Kafka + S3 ─→ leafage-evm (EVM state queries)
        │
        └─ block files (tx · trace · receipts · events) ──→ S3 ─→ Chaintable indexing pipeline (tx/trace data)
```

---

<div align="center">

<img src="assets/conduit-reth.png" alt="Conduit Reth" width="400"/>

</div>

# Conduit-OP-Reth

A customized high performance OP Stack execution client built with the Reth SDK.

Fully compatible with existing OP Stack networks, serving as a drop-in replacement for op-reth.

## Getting Started

### Prerequisites

- Rust 1.92+
- Git

### Production Build

```bash
git clone https://github.com/Chaintable/conduit-op-reth
cd conduit-op-reth
cargo build --profile maxperf
```

### Local Dev Chain

Run a local OP Stack chain with 2-second block times:

```bash
make dev
```

This builds a debug binary, clears any previous state, and starts the node using the Saigon test genesis.

## License

TBD
