//! Prepare a Ronin migration BlockFile without writing to S3.
//!
//! cargo run -p conduit-op-reth-node --example ronin_migration_blockfile -- \
//!   ronin/genesis/mainnet.json block-55577500.json [trace-rpc.json] > repaired.json
//!
//! The optional saved trace_debankBlock response verifies that the source alloc
//! exactly reproduces the writer's StateDiff. stdout contains only BlockFile JSON;
//! stderr reports its validation hash. A bundle upload must replace only this entry
//! and recompute the *whole bundle's* Validation, preserving its is_fork/count format.

use alloy_genesis::Genesis;
use alloy_primitives::b256;
use conduit_op_reth_node::debank::{
    BlockFile, BlockStorageDiff, DebankOutPut, RONIN_MIGRATION_BLOCK,
    build_ronin_migration_txs_and_traces, get_storage_contracts_from_genesis,
};
use eyre::{Result, ensure};
use serde_json::Value;
use std::{env, fs, io};

const MIGRATION_HASH: &str = "0x260a8017de5458b17a2365f2daa29ff0f48d450cd0971293d1e310d508655ac1";

fn prepare(
    source: &Value,
    mut block_file: Value,
    reference: Option<&Value>,
) -> Result<(Value, i64)> {
    ensure!(
        source.pointer("/config/chainId").and_then(Value::as_u64) == Some(2020),
        "expected original Ronin mainnet genesis config"
    );
    ensure!(
        source.pointer("/config/l2MigrationBlock").and_then(Value::as_u64) ==
            Some(RONIN_MIGRATION_BLOCK),
        "unexpected migration height in source genesis"
    );
    let alloc = source
        .pointer("/config/l2Upgrade/alloc")
        .ok_or_else(|| eyre::eyre!("missing config.l2Upgrade.alloc"))?;
    let genesis = Genesis { alloc: serde_json::from_value(alloc.clone())?, ..Default::default() };
    ensure!(!genesis.alloc.is_empty(), "migration alloc is empty");
    ensure!(
        block_file.pointer("/block/height").and_then(Value::as_u64) == Some(RONIN_MIGRATION_BLOCK),
        "expected migration BlockFile height {RONIN_MIGRATION_BLOCK}"
    );
    ensure!(
        block_file.pointer("/block/id").and_then(Value::as_str) == Some(MIGRATION_HASH),
        "unexpected migration block hash"
    );

    if let Some(reference) = reference {
        let rpc: DebankOutPut =
            serde_json::from_value(reference.get("result").unwrap_or(reference).clone())?;
        ensure!(
            rpc.header.hash.to_string() == MIGRATION_HASH &&
                rpc.header.inner.number == RONIN_MIGRATION_BLOCK,
            "RPC reference is not the Ronin migration block"
        );
        let mut expected: BlockStorageDiff = (&genesis).into();
        expected.hash = rpc.header.inner.state_root;
        expected.parent_hash =
            b256!("a8fe53c7551bc10fae45cd0273e2008b26585eaf16dce34aff7291d36bb1c8d4");
        ensure!(
            alloy_rlp::encode(expected).as_slice() == rpc.state_diff.as_ref(),
            "source alloc does not reproduce the writer's migration StateDiff"
        );
    }

    let (transactions, traces) = build_ronin_migration_txs_and_traces(&genesis);
    let generated = serde_json::to_value(BlockFile {
        transactions,
        traces,
        storage_contracts: get_storage_contracts_from_genesis(&genesis),
        ..Default::default()
    })?;
    for field in ["txs", "traces", "events", "error_events", "error_traces", "storage_contracts"] {
        if let Some(existing) = block_file.get(field).filter(|v| !v.is_null()) {
            let array =
                existing.as_array().ok_or_else(|| eyre::eyre!("{field} is not an array"))?;
            ensure!(
                array.is_empty() || existing == &generated[field],
                "conflicting existing {field}"
            );
        }
        block_file[field] = generated[field].clone();
    }
    let typed: BlockFile = serde_json::from_value(block_file.clone())?;
    let hash = typed.validation().validation_hash;
    Ok((block_file, hash))
}

fn main() -> Result<()> {
    let args: Vec<_> = env::args_os().skip(1).collect();
    ensure!(
        args.len() == 2 || args.len() == 3,
        "usage: ronin_migration_blockfile RONIN_MAINNET_JSON BLOCKFILE_JSON [TRACE_RPC_JSON]"
    );
    let source = serde_json::from_slice(&fs::read(&args[0])?)?;
    let original = serde_json::from_slice(&fs::read(&args[1])?)?;
    let reference = args
        .get(2)
        .map(|path| -> Result<Value> { Ok(serde_json::from_slice(&fs::read(path)?)?) })
        .transpose()?;
    let (prepared, hash) = prepare(&source, original, reference.as_ref())?;
    eprintln!(
        "validation_hash={hash} txs={} traces={} storage_contracts={}",
        prepared["txs"].as_array().unwrap().len(),
        prepared["traces"].as_array().unwrap().len(),
        prepared["storage_contracts"].as_array().unwrap().len()
    );
    serde_json::to_writer(io::stdout().lock(), &prepared)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn fixtures() -> (Value, Value) {
        (
            json!({"config": {"chainId":2020, "l2MigrationBlock":55577500,
            "l2Upgrade":{"alloc":{
                "13b0d85ccb8bf860b6b79af3029fca081ae9bef2":{"balance":"0x1","code":"0x6000"}
            }}}}),
            json!({"block":{"height":55577500,"id":MIGRATION_HASH,"timestamp":1778623787},
            "txs":[],"traces":[],"events":[],"error_events":[],"error_traces":[],
            "storage_contracts":[],"retained_field":{"value":"unchanged"}}),
        )
    }

    #[test]
    fn export_preserves_metadata_and_is_idempotent() {
        let (source, original) = fixtures();
        let (file, hash) = prepare(&source, original.clone(), None).unwrap();
        assert_eq!(file["block"], original["block"]);
        assert_eq!(file["retained_field"], original["retained_field"]);
        assert_eq!(file["txs"].as_array().unwrap().len(), 2);
        assert_eq!(file["traces"].as_array().unwrap().len(), 2);
        let (repeated, repeated_hash) = prepare(&source, file.clone(), None).unwrap();
        assert_eq!(file, repeated);
        assert_eq!(hash, repeated_hash);
    }

    #[test]
    fn export_rejects_wrong_chain_height_hash_and_missing_alloc() {
        let (source, file) = fixtures();
        for (pointer, value) in [
            ("/config/chainId", json!(1)),
            ("/config/l2MigrationBlock", json!(55577490)),
            ("/config/l2Upgrade/alloc", json!({})),
        ] {
            let mut invalid = source.clone();
            *invalid.pointer_mut(pointer).unwrap() = value;
            assert!(prepare(&invalid, file.clone(), None).is_err(), "{pointer}");
        }
        for (field, value) in
            [("height", json!(55577501)), ("id", json!(format!("0x{}", "11".repeat(32))))]
        {
            let mut invalid = file.clone();
            invalid["block"][field] = value;
            assert!(prepare(&source, invalid, None).is_err(), "{field}");
        }
    }

    #[test]
    fn export_rejects_conflicting_or_malformed_records() {
        let (source, file) = fixtures();
        for field in
            ["txs", "traces", "events", "error_events", "error_traces", "storage_contracts"]
        {
            for value in [json!([{"id":"existing"}]), json!({"invalid":"array"})] {
                let mut invalid = file.clone();
                invalid[field] = value;
                assert!(prepare(&source, invalid, None).is_err(), "{field}");
            }
        }
    }
}
