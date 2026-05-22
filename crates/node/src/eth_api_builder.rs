//! Local copy of upstream `OpEthApiBuilder` that swaps in [`ConduitOpReceiptConverter`].
//!
//! This file is intentionally a near-verbatim copy of
//! `optimism/rust/op-reth/crates/rpc/src/eth/mod.rs` (the `OpEthApiBuilder` block).
//! The only deviation from upstream is replacing `OpReceiptConverter` with
//! [`ConduitOpReceiptConverter`] so the resulting `RpcConvert` reports
//! `deposit_nonce` and `l1_fee` via the trait getters used by the trace RPC
//! extension. Keep changes here minimal and re-sync from upstream when the
//! op-reth pinned tag is bumped.

use crate::receipt::ConduitOpReceiptConverter;
use alloy_primitives::U256;
use eyre::WrapErr;
use op_alloy_network::Optimism;
use op_alloy_rpc_types_engine::OpFlashblockPayloadBase;
use reqwest::Url;
use reth_chainspec::{EthereumHardforks, Hardforks};
use reth_evm::ConfigureEvm;
use reth_node_api::{FullNodeComponents, FullNodeTypes, HeaderTy, NodePrimitives, NodeTypes};
use reth_node_builder::rpc::{EthApiBuilder, EthApiCtx};
use reth_optimism_flashblocks::{
    FlashBlockCompleteSequence, FlashBlockConsensusClient, FlashBlockService,
    FlashblockCachedReceipt, FlashblocksListeners, WsFlashBlockStream,
};
use reth_optimism_rpc::{
    SequencerClient,
    eth::{OpEthApi, transaction::OpTxInfoMapper},
};
use reth_rpc_convert::RpcConverter;
use reth_rpc_eth_api::{
    FullEthApiServer, RpcConvert, RpcTypes, helpers::pending_block::BuildPendingEnv,
};
use std::marker::PhantomData;
use tokio::sync::watch;
use tracing::info;

/// RPC converter type produced by [`ConduitOpEthApiBuilder`]: mirrors upstream
/// `OpRpcConvert` but with [`ConduitOpReceiptConverter`] in the receipt slot.
pub type ConduitOpRpcConvert<N, NetworkT> = RpcConverter<
    NetworkT,
    <N as FullNodeComponents>::Evm,
    ConduitOpReceiptConverter<<N as FullNodeTypes>::Provider>,
    (),
    OpTxInfoMapper<<N as FullNodeTypes>::Provider>,
>;

/// Builds [`OpEthApi`] for Optimism using the conduit receipt converter.
#[derive(Debug)]
pub struct ConduitOpEthApiBuilder<NetworkT = Optimism> {
    sequencer_url: Option<String>,
    sequencer_headers: Vec<String>,
    min_suggested_priority_fee: u64,
    flashblocks_url: Option<Url>,
    flashblock_consensus: bool,
    _nt: PhantomData<NetworkT>,
}

impl<NetworkT> Default for ConduitOpEthApiBuilder<NetworkT> {
    fn default() -> Self {
        Self {
            sequencer_url: None,
            sequencer_headers: Vec::new(),
            min_suggested_priority_fee: 1_000_000,
            flashblocks_url: None,
            flashblock_consensus: false,
            _nt: PhantomData,
        }
    }
}

impl<NetworkT> ConduitOpEthApiBuilder<NetworkT> {
    /// Creates a new [`ConduitOpEthApiBuilder`] with default settings.
    pub const fn new() -> Self {
        Self {
            sequencer_url: None,
            sequencer_headers: Vec::new(),
            min_suggested_priority_fee: 1_000_000,
            flashblocks_url: None,
            flashblock_consensus: false,
            _nt: PhantomData,
        }
    }

    /// With a sequencer URL.
    pub fn with_sequencer(mut self, sequencer_url: Option<String>) -> Self {
        self.sequencer_url = sequencer_url;
        self
    }

    /// With sequencer client headers.
    pub fn with_sequencer_headers(mut self, sequencer_headers: Vec<String>) -> Self {
        self.sequencer_headers = sequencer_headers;
        self
    }

    /// With minimum suggested priority fee.
    pub const fn with_min_suggested_priority_fee(mut self, min: u64) -> Self {
        self.min_suggested_priority_fee = min;
        self
    }

    /// With a flashblocks websocket URL.
    pub fn with_flashblocks(mut self, flashblocks_url: Option<Url>) -> Self {
        self.flashblocks_url = flashblocks_url;
        self
    }

    /// With flashblock consensus client enabled.
    pub const fn with_flashblock_consensus(mut self, flashblock_consensus: bool) -> Self {
        self.flashblock_consensus = flashblock_consensus;
        self
    }
}

impl<N, NetworkT> EthApiBuilder<N> for ConduitOpEthApiBuilder<NetworkT>
where
    N: FullNodeComponents<
            Evm: ConfigureEvm<
                NextBlockEnvCtx: BuildPendingEnv<HeaderTy<N::Types>>
                                     + From<OpFlashblockPayloadBase>
                                     + Unpin,
            >,
            Types: NodeTypes<
                ChainSpec: Hardforks + EthereumHardforks,
                Payload: reth_node_api::PayloadTypes<
                    ExecutionData: for<'a> TryFrom<
                        &'a FlashBlockCompleteSequence,
                        Error: std::fmt::Display,
                    >,
                >,
            >,
        >,
    NetworkT: RpcTypes,
    ConduitOpRpcConvert<N, NetworkT>: RpcConvert<Network = NetworkT>,
    <<N::Types as NodeTypes>::Primitives as NodePrimitives>::Receipt: FlashblockCachedReceipt,
    OpEthApi<N, ConduitOpRpcConvert<N, NetworkT>>:
        FullEthApiServer<Provider = N::Provider, Pool = N::Pool>,
{
    type EthApi = OpEthApi<N, ConduitOpRpcConvert<N, NetworkT>>;

    async fn build_eth_api(self, ctx: EthApiCtx<'_, N>) -> eyre::Result<Self::EthApi> {
        let Self {
            sequencer_url,
            sequencer_headers,
            min_suggested_priority_fee,
            flashblocks_url,
            flashblock_consensus,
            ..
        } = self;
        let rpc_converter =
            RpcConverter::new(ConduitOpReceiptConverter::new(ctx.components.provider().clone()))
                .with_mapper(OpTxInfoMapper::new(ctx.components.provider().clone()));

        let sequencer_client = if let Some(url) = sequencer_url {
            Some(
                SequencerClient::new_with_headers(&url, sequencer_headers)
                    .await
                    .wrap_err_with(|| format!("Failed to init sequencer client with: {url}"))?,
            )
        } else {
            None
        };

        let flashblocks = if let Some(ws_url) = flashblocks_url {
            info!(target: "reth:cli", %ws_url, "Launching flashblocks service");

            let (tx, pending_rx) = watch::channel(None);
            let stream = WsFlashBlockStream::new(ws_url);
            let service = FlashBlockService::new(
                stream,
                ctx.components.evm_config().clone(),
                ctx.components.provider().clone(),
                ctx.components.task_executor().clone(),
                flashblock_consensus,
            );

            let flashblocks_sequence = service.block_sequence_broadcaster().clone();
            let received_flashblocks = service.flashblocks_broadcaster().clone();
            let in_progress_rx = service.subscribe_in_progress();
            ctx.components.task_executor().spawn_task(Box::pin(service.run(tx)));

            if flashblock_consensus {
                info!(target: "reth::cli", "Launching FlashBlockConsensusClient");
                let flashblock_client = FlashBlockConsensusClient::new(
                    ctx.engine_handle.clone(),
                    flashblocks_sequence.subscribe(),
                )?;
                ctx.components.task_executor().spawn_task(Box::pin(flashblock_client.run()));
            }

            Some(FlashblocksListeners::new(
                pending_rx,
                flashblocks_sequence,
                in_progress_rx,
                received_flashblocks,
            ))
        } else {
            None
        };

        let eth_api = ctx.eth_api_builder().with_rpc_converter(rpc_converter).build_inner();

        Ok(OpEthApi::new(
            eth_api,
            sequencer_client,
            U256::from(min_suggested_priority_fee),
            flashblocks,
        ))
    }
}
