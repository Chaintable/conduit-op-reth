use crate::debank::{
    BlockFile, BlockStorageDiff, DebankBlock, DebankOutPut, DebankTransaction, build_debank_traces,
    build_genesis_txs_and_traces, get_storage_contracts_from_bundle,
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
const RONIN_SYNTHETIC_GENESIS_BLOCK: u64 = 55_577_500;
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
    chain_id == RONIN_CHAIN_ID && block_number == RONIN_SYNTHETIC_GENESIS_BLOCK
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
            (Vec::new(), Vec::new(), Vec::new())
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
