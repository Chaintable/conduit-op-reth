use crate::{
    chainspec::ConduitOpChainSpec, eth_api_builder::ConduitOpEthApiBuilder,
    evm::ConduitOpExecutorBuilder,
};
use reth_engine_local::LocalPayloadAttributesBuilder;
use reth_node_api::{FullNodeComponents, PayloadAttributesBuilder, PayloadTypes};
use reth_node_builder::{
    DebugNode, Node, NodeAdapter, NodeComponentsBuilder, NodeTypes,
    components::{BasicPayloadServiceBuilder, ComponentsBuilder},
    node::FullNodeTypes,
    rpc::{BasicEngineValidatorBuilder, RpcAddOns},
};
use reth_optimism_node::{
    OpDAConfig, OpEngineApiBuilder, OpEngineTypes, OpStorage,
    args::RollupArgs,
    node::{
        OpAddOns, OpConsensusBuilder, OpEngineValidatorBuilder, OpFullNodeTypes, OpNetworkBuilder,
        OpNodeTypes, OpPayloadBuilder, OpPoolBuilder,
    },
};
use reth_optimism_payload_builder::config::OpGasLimitConfig;
use reth_optimism_primitives::OpPrimitives;
use reth_primitives_traits::SealedHeader;
use std::sync::Arc;

/// Type configuration for the ConduitOp OP Stack node.
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct ConduitOpNode {
    /// Optimism rollup arguments.
    pub args: RollupArgs,
    /// Data availability configuration for the OP builder.
    ///
    /// Used to throttle the size of the data availability payloads (configured by the batcher via
    /// the `miner_` api).
    ///
    /// By default no throttling is applied.
    pub da_config: OpDAConfig,
    /// Gas limit configuration for the OP builder.
    /// Used to control the gas limit of the blocks produced by the OP builder (configured by the
    /// batcher via the `miner_` api).
    pub gas_limit_config: OpGasLimitConfig,
}

impl ConduitOpNode {
    /// Creates a new instance of the ConduitOp node type.
    pub fn new(args: RollupArgs) -> Self {
        Self {
            args,
            da_config: OpDAConfig::default(),
            gas_limit_config: OpGasLimitConfig::default(),
        }
    }

    /// Configure the data availability configuration for the OP builder.
    pub fn with_da_config(mut self, da_config: OpDAConfig) -> Self {
        self.da_config = da_config;
        self
    }

    /// Configure the gas limit configuration for the OP builder.
    pub fn with_gas_limit_config(mut self, gas_limit_config: OpGasLimitConfig) -> Self {
        self.gas_limit_config = gas_limit_config;
        self
    }
}

impl NodeTypes for ConduitOpNode {
    type Primitives = OpPrimitives;
    type ChainSpec = ConduitOpChainSpec;
    type Storage = OpStorage;
    type Payload = OpEngineTypes;
}

impl<N> Node<N> for ConduitOpNode
where
    N: FullNodeTypes<
        Types: OpFullNodeTypes + OpNodeTypes + NodeTypes<ChainSpec = ConduitOpChainSpec>,
    >,
{
    type ComponentsBuilder = ComponentsBuilder<
        N,
        OpPoolBuilder,
        BasicPayloadServiceBuilder<OpPayloadBuilder>,
        OpNetworkBuilder,
        ConduitOpExecutorBuilder,
        OpConsensusBuilder,
    >;

    type AddOns = OpAddOns<
        NodeAdapter<N, <Self::ComponentsBuilder as NodeComponentsBuilder<N>>::Components>,
        ConduitOpEthApiBuilder,
        OpEngineValidatorBuilder,
        OpEngineApiBuilder<OpEngineValidatorBuilder>,
        BasicEngineValidatorBuilder<OpEngineValidatorBuilder>,
    >;

    fn components_builder(&self) -> Self::ComponentsBuilder {
        let RollupArgs { disable_txpool_gossip, compute_pending_block, discovery_v4, .. } =
            self.args;
        ComponentsBuilder::default()
            .node_types::<N>()
            .pool(
                OpPoolBuilder::default()
                    .with_enable_tx_conditional(self.args.enable_tx_conditional)
                    .with_supervisor(
                        self.args.supervisor_http.clone(),
                        self.args.supervisor_safety_level,
                    ),
            )
            .executor(ConduitOpExecutorBuilder)
            .payload(BasicPayloadServiceBuilder::new(
                OpPayloadBuilder::new(compute_pending_block)
                    .with_da_config(self.da_config.clone())
                    .with_gas_limit_config(self.gas_limit_config.clone()),
            ))
            .network(OpNetworkBuilder::new(disable_txpool_gossip, !discovery_v4))
            .consensus(OpConsensusBuilder::default())
    }

    fn add_ons(&self) -> Self::AddOns {
        // UPSTREAM SYNC: line-for-line copy of `OpAddOnsBuilder::build()` from
        // op-reth `crates/node/src/node.rs` @ tag `op-reth/v1.11.5`.
        // Intentional delta: `OpEthApiBuilder` -> `ConduitOpEthApiBuilder` to inject
        // `ConduitOpReceiptConverter` (provides real `get_deposit_nonce`/`get_l1_fee`).
        // Defaults mirror `OpAddOnsBuilder::default()`: `tokio_runtime = None`,
        // `rpc_middleware = Identity::new()`. When upgrading op-reth, re-diff this
        // function against the new upstream `build()` body. The witness consts below
        // catch constructor signature drift at compile time; new hidden builder setters
        // require manual sync.
        OpAddOns::new(
            RpcAddOns::new(
                ConduitOpEthApiBuilder::default()
                    .with_sequencer(self.args.sequencer.clone())
                    .with_sequencer_headers(self.args.sequencer_headers.clone())
                    .with_min_suggested_priority_fee(self.args.min_suggested_priority_fee)
                    .with_flashblocks(self.args.flashblocks_url.clone())
                    .with_flashblock_consensus(self.args.flashblock_consensus),
                OpEngineValidatorBuilder::default(),
                OpEngineApiBuilder::<OpEngineValidatorBuilder>::default(),
                BasicEngineValidatorBuilder::<OpEngineValidatorBuilder>::default(),
                reth_node_builder::rpc::Identity::new(),
            )
            .with_tokio_runtime(None),
            self.da_config.clone(),
            self.gas_limit_config.clone(),
            self.args.sequencer.clone(),
            self.args.sequencer_headers.clone(),
            self.args.historical_rpc.clone(),
            self.args.enable_tx_conditional,
            self.args.min_suggested_priority_fee,
        )
    }
}

impl<N> DebugNode<N> for ConduitOpNode
where
    N: FullNodeComponents<Types = Self>,
{
    type RpcBlock = alloy_rpc_types_eth::Block<op_alloy_consensus::OpTxEnvelope>;

    fn rpc_to_primitive_block(rpc_block: Self::RpcBlock) -> reth_node_api::BlockTy<Self> {
        rpc_block.into_consensus()
    }

    fn local_payload_attributes_builder(
        chain_spec: &Self::ChainSpec,
    ) -> impl PayloadAttributesBuilder<<Self::Payload as PayloadTypes>::PayloadAttributes> {
        let inner = LocalPayloadAttributesBuilder::new(Arc::new(chain_spec.clone()));
        // This allows us to run --dev mode. Fixed in upstream https://github.com/paradigmxyz/reth/pull/21855/changes
        move |parent: SealedHeader| {
            let mut attrs: op_alloy_rpc_types_engine::OpPayloadAttributes = inner.build(&parent);

            // Encode default OP EIP-1559 params: denominator=50, elasticity=6
            attrs.eip_1559_params = Some(alloy_primitives::B64::from_slice(&[
                0, 0, 0, 50, // denominator
                0, 0, 0, 6, // elasticity
            ]));
            attrs.min_base_fee = Some(0);
            attrs
        }
    }
}

// Compile-time signature pins. If upstream changes the parameter count or order
// of `RpcAddOns::new` or `OpAddOns::new`, these `const` assignments fail to type-check,
// pointing the reviewer at `add_ons()` above. They DO NOT catch new hidden builder
// setters or default-value changes - that still requires manual re-diff per the
// UPSTREAM SYNC comment in `add_ons()`.
#[allow(dead_code, clippy::type_complexity)]
mod upstream_signature_pins {
    use reth_node_api::FullNodeComponents;
    use reth_node_builder::rpc::{EthApiBuilder, Identity, RpcAddOns};
    use reth_optimism_node::{OpDAConfig, node::OpAddOns};
    use reth_optimism_payload_builder::config::OpGasLimitConfig;

    fn pin_rpc_add_ons_new<N, EthB, PVB, EB, EVB>()
    where
        N: FullNodeComponents,
        EthB: EthApiBuilder<N>,
    {
        let _: fn(EthB, PVB, EB, EVB, Identity) -> RpcAddOns<N, EthB, PVB, EB, EVB, Identity> =
            RpcAddOns::<N, EthB, PVB, EB, EVB, Identity>::new;
    }

    fn pin_op_add_ons_new<N, EthB, PVB, EB, EVB>()
    where
        N: FullNodeComponents,
        EthB: EthApiBuilder<N>,
    {
        let _: fn(
            RpcAddOns<N, EthB, PVB, EB, EVB, Identity>,
            OpDAConfig,
            OpGasLimitConfig,
            Option<String>,
            Vec<String>,
            Option<String>,
            bool,
            u64,
        ) -> OpAddOns<N, EthB, PVB, EB, EVB, Identity> =
            OpAddOns::<N, EthB, PVB, EB, EVB, Identity>::new;
    }
}
