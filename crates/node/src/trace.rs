use crate::debank::{
    BlockFile, BlockStorageDiff, DebankBlock, DebankOutPut, DebankTransaction,
    RONIN_MIGRATION_BLOCK, build_debank_traces, build_genesis_txs_and_traces,
    build_ronin_migration_txs_and_traces, get_storage_contracts_from_bundle,
    get_storage_contracts_from_genesis, get_storage_diffs_from_changesets,
};
use alloy_consensus::{BlockHeader, transaction::TxHashRef};
use alloy_eips::BlockId;
use alloy_evm::evm::EvmFactoryExt;
use alloy_genesis::Genesis;
use alloy_primitives::{B256, b256};
use alloy_rpc_types_eth::Header;
use async_trait::async_trait;
use jsonrpsee::{core::RpcResult, proc_macros::rpc};
use op_alloy_consensus::OpReceipt;
use op_alloy_rpc_types::OpTransactionReceipt;
use reth_chainspec::{ChainSpecProvider, EthChainSpec};
use reth_evm::ConfigureEvm;
use reth_primitives_traits::{BlockBody, RecoveredBlock};
use reth_revm::{database::StateProviderDatabase, db::State};
use reth_rpc_eth_api::{
    EthApiTypes, FromEthApiError, RpcNodeCore, RpcTypes,
    helpers::{EthBlocks, LoadReceipt, SpawnBlocking, TraceExt},
};
use reth_rpc_eth_types::{EthApiError, cache::db::StateCacheDb};
use reth_storage_api::{ChangeSetReader, StorageChangeSetReader};
use reth_tasks::pool::BlockingTaskGuard;
use revm::{context_interface::Block, database::states::bundle_state::BundleRetention};
use revm_bytecode::opcode::OpCode;
use revm_inspectors::tracing::{OpcodeFilter, TracingInspector, TracingInspectorConfig};

const RONIN_CHAIN_ID: u64 = 2020;
const RONIN_SYNTHETIC_GENESIS_PARENT_STATE: B256 =
    b256!("a8fe53c7551bc10fae45cd0273e2008b26585eaf16dce34aff7291d36bb1c8d4");
const RONIN_SYNTHETIC_GENESIS_PARENT_HASH: B256 =
    b256!("5e55f27976de00b9c7ebaeff8209864cd8b5142ccbb72f7227c452844f6b3764");

#[rpc(server, namespace = "trace")]
pub trait OpDebankTraceApi {
    #[method(name = "debankBlock")]
    async fn debank_block(&self, block_id: BlockId) -> RpcResult<DebankOutPut>;
}

#[derive(Debug)]
pub struct OpDebankTraceApiImpl<Eth> {
    eth: Eth,
    tracing_task_guard: BlockingTaskGuard,
}

impl<Eth> OpDebankTraceApiImpl<Eth>
where
    Eth: SpawnBlocking,
{
    pub fn new(eth: Eth) -> Self {
        let tracing_task_guard = eth.tracing_task_guard().clone();
        Self { eth, tracing_task_guard }
    }
}

fn get_deposit_nonce(receipt: &OpTransactionReceipt) -> Option<u64> {
    if let OpReceipt::Deposit(dep) = &receipt.inner.inner.receipt {
        dep.deposit_nonce
    } else {
        None
    }
}

fn get_l1_fee(receipt: &OpTransactionReceipt) -> Option<u128> {
    receipt.l1_block_info.l1_fee
}

#[async_trait]
impl<Eth> OpDebankTraceApiServer for OpDebankTraceApiImpl<Eth>
where
    Eth: TraceExt + EthBlocks + LoadReceipt + 'static,
    Eth: RpcNodeCore,
    <Eth as EthApiTypes>::NetworkTypes: RpcTypes<Receipt = OpTransactionReceipt>,
    Eth::Error: FromEthApiError,
    <Eth as RpcNodeCore>::Provider:
        ChainSpecProvider<ChainSpec: EthChainSpec> + ChangeSetReader + StorageChangeSetReader,
{
    async fn debank_block(&self, block_id: BlockId) -> RpcResult<DebankOutPut> {
        let _permit = self
            .tracing_task_guard
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| EthApiError::InternalEthError)?;
        Ok(self.trace_debank_block_inner(block_id).await.map_err(Into::into)?)
    }
}

impl<Eth> OpDebankTraceApiImpl<Eth>
where
    Eth: TraceExt + EthBlocks + LoadReceipt + 'static,
    Eth: RpcNodeCore,
    <Eth as EthApiTypes>::NetworkTypes: RpcTypes<Receipt = OpTransactionReceipt>,
    Eth::Error: FromEthApiError,
    <Eth as RpcNodeCore>::Provider:
        ChainSpecProvider<ChainSpec: EthChainSpec> + ChangeSetReader + StorageChangeSetReader,
{
    async fn trace_debank_block_inner(
        &self,
        block_id: BlockId,
    ) -> Result<DebankOutPut, Eth::Error> {
        let eth = &self.eth;

        let block = eth.recovered_block(block_id).await?;
        let Some(block) = block else {
            return Err(EthApiError::HeaderNotFound(block_id).into());
        };

        let debank_block: DebankBlock = block.as_ref().into();
        let debank_header = build_rpc_header(&block);

        if block.number() == 0 {
            let chain_spec = reth_rpc_eth_api::RpcNodeCore::provider(eth).chain_spec();
            return Ok(build_genesis_output(
                debank_block,
                debank_header,
                chain_spec.genesis(),
                block.state_root(),
                GenesisKind::Canonical,
            ));
        }

        let chain_spec = reth_rpc_eth_api::RpcNodeCore::provider(eth).chain_spec();
        if is_ronin_synthetic_genesis(chain_spec.chain().id(), block.number()) {
            return Ok(build_genesis_output(
                debank_block,
                debank_header,
                chain_spec.genesis(),
                block.state_root(),
                GenesisKind::RoninSynthetic,
            ));
        }

        let receipts: Option<Vec<OpTransactionReceipt>> = eth.block_receipts(block_id).await?;
        let Some(receipts) = receipts else {
            return Err(EthApiError::HeaderNotFound(block_id).into());
        };

        let mut debank_txs: Vec<DebankTransaction> =
            Vec::with_capacity(block.body().transactions().len());
        for (index, tx) in block.body().transactions().iter().enumerate() {
            let receipt = &receipts[index];
            let deposit_nonce = get_deposit_nonce(receipt);
            let l1_fee = get_l1_fee(receipt);
            debank_txs.push(DebankTransaction::from((receipt, tx, deposit_nonce, l1_fee)));
        }

        let parent_block = eth.recovered_block(block.parent_hash().into()).await?;
        let Some(parent_block) = parent_block else {
            return Err(EthApiError::HeaderNotFound(block_id).into());
        };

        let mut block_file =
            BlockFile { block: debank_block, transactions: debank_txs, ..Default::default() };

        if parent_block.state_root() == block.state_root() {
            let state_diff = BlockStorageDiff {
                hash: block.state_root(),
                parent_hash: parent_block.state_root(),
                ..Default::default()
            };
            let validation_hash = block_file.validation().validation_hash;
            return Ok(DebankOutPut {
                block_file,
                header: debank_header,
                state_diff: alloy_rlp::encode(state_diff).into(),
                validation_hash,
            });
        }

        let (mut trace_results, mut state_diff, change_addresses) =
            trace_all_block(eth, block_id).await?;

        for (trace, error_trace, event, error_event) in trace_results.drain(..) {
            block_file.traces.extend(trace);
            block_file.error_traces.extend(error_trace);
            block_file.events.extend(event);
            block_file.error_events.extend(error_event);
        }
        state_diff.hash = block.state_root();
        state_diff.parent_hash = parent_block.state_root();
        block_file.storage_contracts = change_addresses;
        let validation_hash = block_file.validation().validation_hash;

        Ok(DebankOutPut {
            block_file,
            header: debank_header,
            state_diff: alloy_rlp::encode(state_diff).into(),
            validation_hash,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GenesisKind {
    Canonical,
    RoninSynthetic,
}

fn is_ronin_synthetic_genesis(chain_id: u64, block_number: u64) -> bool {
    chain_id == RONIN_CHAIN_ID && block_number == RONIN_MIGRATION_BLOCK
}

fn build_genesis_output(
    mut debank_block: DebankBlock,
    mut header: Header,
    genesis: &Genesis,
    state_root: B256,
    kind: GenesisKind,
) -> DebankOutPut {
    let mut state_diff: BlockStorageDiff = genesis.into();
    state_diff.hash = state_root;

    let (transactions, traces, storage_contracts) = match kind {
        GenesisKind::Canonical => {
            let (transactions, traces) = build_genesis_txs_and_traces(genesis);
            (transactions, traces, get_storage_contracts_from_genesis(genesis))
        }
        GenesisKind::RoninSynthetic => {
            state_diff.parent_hash = RONIN_SYNTHETIC_GENESIS_PARENT_STATE;
            debank_block.parent_id = RONIN_SYNTHETIC_GENESIS_PARENT_HASH;
            header.inner.parent_hash = RONIN_SYNTHETIC_GENESIS_PARENT_HASH;
            let (transactions, traces) = build_ronin_migration_txs_and_traces(genesis);
            (transactions, traces, get_storage_contracts_from_genesis(genesis))
        }
    };

    let block_file = BlockFile {
        block: debank_block,
        transactions,
        traces,
        storage_contracts,
        ..Default::default()
    };
    let validation_hash = block_file.validation().validation_hash;
    DebankOutPut {
        block_file,
        header,
        state_diff: alloy_rlp::encode(state_diff).into(),
        validation_hash,
    }
}

fn build_rpc_header<B>(block: &RecoveredBlock<B>) -> Header
where
    B: reth_primitives_traits::Block,
{
    Header {
        inner: alloy_consensus::Header {
            parent_hash: block.header().parent_hash(),
            ommers_hash: block.header().ommers_hash(),
            beneficiary: block.header().beneficiary(),
            state_root: block.header().state_root(),
            transactions_root: block.header().transactions_root(),
            receipts_root: block.header().receipts_root(),
            logs_bloom: block.header().logs_bloom(),
            difficulty: block.header().difficulty(),
            number: block.header().number(),
            gas_limit: block.header().gas_limit(),
            gas_used: block.header().gas_used(),
            timestamp: block.header().timestamp(),
            extra_data: block.header().extra_data().clone(),
            mix_hash: block.header().mix_hash().unwrap_or_default(),
            nonce: block.header().nonce().unwrap_or_default(),
            base_fee_per_gas: block.header().base_fee_per_gas(),
            withdrawals_root: block.header().withdrawals_root(),
            blob_gas_used: block.header().blob_gas_used(),
            excess_blob_gas: block.header().excess_blob_gas(),
            parent_beacon_block_root: block.header().parent_beacon_block_root(),
            requests_hash: block.header().requests_hash(),
            block_access_list_hash: block.header().block_access_list_hash(),
            slot_number: None,
        },
        hash: block.hash(),
        total_difficulty: None,
        size: None,
    }
}

type TraceEntry = (
    Vec<crate::debank::DebankTrace>,
    Vec<crate::debank::DebankTrace>,
    Vec<crate::debank::DebankEvent>,
    Vec<crate::debank::DebankEvent>,
);

async fn trace_all_block<Eth>(
    eth: &Eth,
    block_id: BlockId,
) -> Result<(Vec<TraceEntry>, BlockStorageDiff, Vec<alloy_primitives::Address>), Eth::Error>
where
    Eth: TraceExt + RpcNodeCore + 'static,
    Eth::Error: FromEthApiError,
    <Eth as RpcNodeCore>::Provider: ChangeSetReader + StorageChangeSetReader,
    reth_evm::BlockEnvFor<Eth::Evm>: Block,
{
    use reth_rpc_eth_types::cache::db::StateProviderTraitObjWrapper;

    let block = eth.recovered_block(block_id);
    let ((evm_env, _), block) = futures_util::try_join!(eth.evm_env_at(block_id), block)?;

    let Some(block) = block else {
        return Err(EthApiError::EvmCustom(format!("cannot find block {block_id}")).into());
    };

    let parent_hash = block.parent_hash();
    let block_hash = block.hash();
    let block_number = block.number();
    let post_state = eth.state_at_block_id(block_hash.into()).await?;
    let exec_state = eth.state_at_block_id(parent_hash.into()).await?;

    eth.spawn_tracing(move |this| {
        let mut db: StateCacheDb = State::builder()
            .with_database(StateProviderDatabase::new(StateProviderTraitObjWrapper(exec_state)))
            .with_bundle_update()
            .build();

        this.apply_pre_execution_changes(&block, &mut db)?;

        let log_index_cell = std::cell::RefCell::new(0usize);

        let mut trace_cfg = TracingInspectorConfig::default_parity()
            .set_steps(true)
            .set_record_logs(true)
            .set_exclude_precompile_calls(false);
        trace_cfg.record_opcodes_filter = Some(OpcodeFilter::new().enabled(OpCode::SSTORE));

        let results: Vec<TraceEntry> = this
            .evm_config()
            .evm_factory()
            .create_tracer(&mut db, evm_env, TracingInspector::new(trace_cfg))
            .try_trace_many(block.transactions_recovered(), |mut ctx| {
                let traces = build_debank_traces(
                    *ctx.tx.tx_hash(),
                    ctx.take_inspector().into_traces(),
                    &log_index_cell,
                );
                Ok::<_, Eth::Error>(traces)
            })
            .commit_last_tx()
            .collect::<Result<_, _>>()?;

        db.merge_transitions(BundleRetention::PlainState);
        let bundle = db.take_bundle();
        let change_addresses = get_storage_contracts_from_bundle(&bundle);
        let account_changesets = this
            .provider()
            .account_block_changeset(block_number)
            .map_err(Eth::Error::from_eth_err)?;
        let storage_changesets =
            this.provider().storage_changeset(block_number).map_err(Eth::Error::from_eth_err)?;
        let storage_diff = get_storage_diffs_from_changesets(
            account_changesets,
            storage_changesets,
            StateProviderDatabase::new(StateProviderTraitObjWrapper(post_state)),
        )
        .map_err(|err| {
            Eth::Error::from_eth_err(EthApiError::EvmCustom(format!(
                "failed to build state diff from changesets: {err}"
            )))
        })?;
        Ok((results, storage_diff, change_addresses))
    })
    .await
}

#[cfg(test)]
mod genesis_tests {
    use super::*;
    use alloy_primitives::{Address, U256, address};
    use std::collections::HashSet;

    fn migration_fixture() -> Genesis {
        serde_json::from_value(serde_json::json!({
            "config": { "chainId": 2020 },
            "gasLimit": "0x3938700", "difficulty": "0x0",
            "alloc": {
                "0000000000000000000000000000000000000011": {"balance":"0x1","code":"0x6011"},
                "0000000000000000000000000000000000000022": {"balance":"0x0","code":"0x6022"},
                "0000000000000000000000000000000000000033": {"balance":"0x0","code":"0x6033"},
                "ed4a9f48a62fb6fdcfb45bb00c9f61d1a436e58c": {"balance":"0x9","code":"0x60ed"},
                "13b0d85ccb8bf860b6b79af3029fca081ae9bef2": {"balance":"0x7","code":"0x6013"},
                "914d7fec6aac8cd542e72bca78b30650d45643d7": {"balance":"0x0","code":"0x6091","storage":{"0x01":"0x02"}}
            }
        }))
        .unwrap()
    }

    fn output(genesis: &Genesis, kind: GenesisKind) -> DebankOutPut {
        build_genesis_output(
            DebankBlock { height: RONIN_MIGRATION_BLOCK, ..Default::default() },
            Header::default(),
            genesis,
            B256::repeat_byte(0x42),
            kind,
        )
    }

    #[test]
    fn ronin_migration_emits_records_without_repeating_original_genesis_kinds() {
        let result = output(&migration_fixture(), GenesisKind::RoninSynthetic);
        let file = result.block_file;
        assert_eq!(file.transactions.len(), 5);
        assert_eq!(file.traces.len(), 5);
        assert_eq!(file.transactions[0].to, address!("0000000000000000000000000000000000000011"));
        assert_eq!(file.transactions[0].value, U256::from(1));
        assert_eq!(file.traces[0].call_create_type, "call");
        assert_eq!(file.traces.last().unwrap().call_create_type, "create");
        assert_eq!(
            file.transactions.last().unwrap().to,
            address!("ed4a9f48a62fb6fdcfb45bb00c9f61d1a436e58c")
        );
        assert!(file.transactions.iter().all(|tx| tx.to != Address::repeat_byte(0xee)));
        assert_eq!(
            file.storage_contracts,
            vec![address!("914d7fec6aac8cd542e72bca78b30650d45643d7")]
        );
        assert_eq!(result.validation_hash, file.validation().validation_hash);
    }

    #[test]
    fn ronin_migration_ids_and_trace_links_are_stable_bytes32() {
        let first = output(&migration_fixture(), GenesisKind::RoninSynthetic);
        let second = output(&migration_fixture(), GenesisKind::RoninSynthetic);
        assert_eq!(
            serde_json::to_value(&first.block_file).unwrap(),
            serde_json::to_value(&second.block_file).unwrap()
        );
        let txs = &first.block_file.transactions;
        assert!(!txs.is_empty());
        let mut ids = HashSet::new();
        let mut trace_ids = HashSet::new();
        for (index, (tx, trace)) in txs.iter().zip(&first.block_file.traces).enumerate() {
            assert!(tx.id.parse::<B256>().is_ok());
            assert!(ids.insert(&tx.id));
            assert!(trace_ids.insert(&trace.id));
            assert_eq!(tx.transaction_index, index as u64);
            assert_eq!(trace.tx_id, tx.id);
            assert_eq!(tx.from, Address::ZERO);
            assert_eq!(tx.gas_limit, 0);
            assert_eq!(tx.gas_price, 0);
            assert_eq!(tx.gas_used, 0);
            assert!(tx.status);
            assert!(trace.parent_trace_id.is_empty());
            if trace.call_create_type == "create" {
                assert_eq!(trace.output, tx.input);
                assert_eq!(trace.input, tx.input);
            }
        }
        let tx = txs
            .iter()
            .find(|tx| tx.to == address!("914d7fec6aac8cd542e72bca78b30650d45643d7"))
            .unwrap();
        assert_eq!(tx.id, "0x020000000000000003500b9c914d7fec6aac8cd542e72bca78b30650d45643d7");
        let trace = first.block_file.traces.iter().find(|trace| trace.tx_id == tx.id).unwrap();
        assert_eq!(trace.id, "f6609a36881a7832e4c53f78d9dd30e5");
    }

    #[test]
    fn migration_keeps_existing_state_diff_and_header_linkage() {
        let genesis = migration_fixture();
        let result = output(&genesis, GenesisKind::RoninSynthetic);
        let mut expected_diff: BlockStorageDiff = (&genesis).into();
        expected_diff.hash = B256::repeat_byte(0x42);
        expected_diff.parent_hash = RONIN_SYNTHETIC_GENESIS_PARENT_STATE;
        assert_eq!(result.state_diff.as_ref(), alloy_rlp::encode(expected_diff));
        assert_eq!(result.header.inner.parent_hash, RONIN_SYNTHETIC_GENESIS_PARENT_HASH);
        assert_eq!(result.block_file.block.parent_id, RONIN_SYNTHETIC_GENESIS_PARENT_HASH);
        assert!(is_ronin_synthetic_genesis(2020, 55_577_500));
        assert!(!is_ronin_synthetic_genesis(2020, 55_577_499));
        assert!(!is_ronin_synthetic_genesis(2020, 55_577_501));
        assert!(!is_ronin_synthetic_genesis(1, 55_577_500));
    }

    #[test]
    fn canonical_genesis_retains_its_existing_records() {
        let result = output(&migration_fixture(), GenesisKind::Canonical);
        assert_eq!(result.block_file.transactions.len(), 10);
        assert_eq!(result.block_file.traces.len(), 10);
        assert_eq!(result.block_file.transactions.last().unwrap().to, Address::repeat_byte(0xee));
        assert!(result.block_file.transactions[0].id.starts_with("0xgenesis01"));
    }

    #[test]
    fn migration_uses_checksum_order_and_balance_before_code() {
        // EIP-55 ASCII order puts ...a1 before ...a0, unlike raw address order.
        let genesis: Genesis = serde_json::from_value(serde_json::json!({
            "config": {"chainId": 2020}, "gasLimit": "0x0", "difficulty": "0x0",
            "alloc": {
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0": {"balance":"0x1","code":"0x6000"},
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1": {"balance":"0x2","code":"0x6001"}
            }
        }))
        .unwrap();
        let file = output(&genesis, GenesisKind::RoninSynthetic).block_file;
        assert_eq!(file.transactions.len(), 4);
        assert_eq!(file.transactions[0].to, address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1"));
        assert_eq!(file.transactions[1].to, file.transactions[0].to);
        assert_eq!(file.transactions[2].to, address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0"));
        assert_eq!(file.transactions[3].to, file.transactions[2].to);
        assert_eq!(
            file.traces.iter().map(|trace| trace.call_create_type.as_str()).collect::<Vec<_>>(),
            vec!["call", "create", "call", "create"]
        );
    }
}
