//! OP receipt converter wrapper that surfaces deposit nonce and L1 fee.
//!
//! Upstream's [`OpReceiptConverter`] already produces an [`OpTransactionReceipt`] whose
//! `l1_block_info.l1_fee` is populated, but the deposit-specific extra fields
//! (`deposit_nonce`, `deposit_receipt_version`) are dropped during the receipt build
//! step. The trace RPC extension introduced in conduit-reth depends on two
//! `RpcConvert::get_deposit_nonce` / `get_l1_fee` accessors, both of which default
//! to `None`. We override them here by reading directly from the (preserved) inner
//! consensus receipt and the populated `l1_block_info`.

use op_alloy_consensus::{OpReceipt, OpTransaction};
use op_alloy_rpc_types::OpTransactionReceipt;
use reth_chainspec::ChainSpecProvider;
use reth_node_api::NodePrimitives;
use reth_optimism_forks::OpHardforks;
use reth_optimism_rpc::{OpEthApiError, eth::receipt::OpReceiptConverter};
use reth_primitives_traits::SealedBlock;
use reth_rpc_convert::transaction::{ConvertReceiptInput, ReceiptConverter};
use reth_storage_api::BlockReader;
use std::fmt::Debug;

/// Newtype wrapping [`OpReceiptConverter`] to provide deposit-nonce and L1-fee
/// accessors required by the debank-3 trace RPC extension.
#[derive(Debug, Clone)]
pub struct ConduitOpReceiptConverter<Provider> {
    inner: OpReceiptConverter<Provider>,
}

impl<Provider> ConduitOpReceiptConverter<Provider> {
    /// Creates a new [`ConduitOpReceiptConverter`] wrapping a fresh [`OpReceiptConverter`].
    pub const fn new(provider: Provider) -> Self {
        Self { inner: OpReceiptConverter::new(provider) }
    }
}

impl<Provider, N> ReceiptConverter<N> for ConduitOpReceiptConverter<Provider>
where
    N: NodePrimitives<SignedTx: OpTransaction, Receipt = OpReceipt>,
    Provider:
        BlockReader<Block = N::Block> + ChainSpecProvider<ChainSpec: OpHardforks> + Debug + 'static,
    OpReceiptConverter<Provider>:
        ReceiptConverter<N, RpcReceipt = OpTransactionReceipt, Error = OpEthApiError>,
{
    type RpcReceipt = OpTransactionReceipt;
    type Error = OpEthApiError;

    fn convert_receipts(
        &self,
        inputs: Vec<ConvertReceiptInput<'_, N>>,
    ) -> Result<Vec<Self::RpcReceipt>, Self::Error> {
        self.inner.convert_receipts(inputs)
    }

    fn convert_receipts_with_block(
        &self,
        inputs: Vec<ConvertReceiptInput<'_, N>>,
        block: &SealedBlock<N::Block>,
    ) -> Result<Vec<Self::RpcReceipt>, Self::Error> {
        self.inner.convert_receipts_with_block(inputs, block)
    }

    // Upstream `OpReceiptBuilder::build` drops `deposit_nonce` from the receipt-fields
    // surface, but the field is preserved on the underlying consensus receipt at
    // `OpTransactionReceipt.inner.inner.receipt`. Reach in to recover it here.
    fn get_deposit_nonce(&self, receipt: &Self::RpcReceipt) -> Option<u64> {
        deposit_nonce_of(receipt)
    }

    // `l1_block_info.l1_fee` is populated by the receipt builder for every L2 tx whose
    // L1 origin block info is available; expose it directly so trace RPC consumers
    // don't have to walk through the entire receipt structure.
    fn get_l1_fee(&self, receipt: &Self::RpcReceipt) -> Option<u128> {
        l1_fee_of(receipt)
    }
}

/// Extracts `deposit_nonce` from a deposit-type [`OpTransactionReceipt`].
///
/// Pulled out so it can be unit-tested without a Provider; see
/// [`ReceiptConverter::get_deposit_nonce`].
pub(crate) fn deposit_nonce_of(receipt: &OpTransactionReceipt) -> Option<u64> {
    match &receipt.inner.inner.receipt {
        OpReceipt::Deposit(d) => d.deposit_nonce,
        _ => None,
    }
}

/// Extracts the populated `l1_fee` from an [`OpTransactionReceipt`].
pub(crate) fn l1_fee_of(receipt: &OpTransactionReceipt) -> Option<u128> {
    receipt.l1_block_info.l1_fee
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::ReceiptWithBloom;
    use alloy_primitives::{B256, Bloom};
    use alloy_rpc_types_eth::{Log, TransactionReceipt};
    use op_alloy_consensus::OpDepositReceipt;
    use op_alloy_rpc_types::L1BlockInfo;

    fn make_receipt(op_receipt: OpReceipt<Log>, l1_fee: Option<u128>) -> OpTransactionReceipt {
        let receipt_with_bloom =
            ReceiptWithBloom { receipt: op_receipt, logs_bloom: Bloom::default() };
        let inner = TransactionReceipt {
            inner: receipt_with_bloom,
            transaction_hash: B256::ZERO,
            transaction_index: Some(0),
            block_hash: Some(B256::ZERO),
            block_number: Some(0),
            gas_used: 0,
            effective_gas_price: 0,
            blob_gas_used: None,
            blob_gas_price: None,
            from: Default::default(),
            to: None,
            contract_address: None,
        };
        OpTransactionReceipt { inner, l1_block_info: L1BlockInfo { l1_fee, ..Default::default() } }
    }

    #[test]
    fn deposit_nonce_present_for_deposit() {
        let deposit = OpDepositReceipt::<Log> {
            inner: alloy_consensus::Receipt {
                status: alloy_consensus::Eip658Value::Eip658(true),
                cumulative_gas_used: 0,
                logs: vec![],
            },
            deposit_nonce: Some(42),
            deposit_receipt_version: Some(1),
        };
        let receipt = make_receipt(OpReceipt::Deposit(deposit), None);
        assert_eq!(deposit_nonce_of(&receipt), Some(42));
    }

    #[test]
    fn deposit_nonce_none_for_non_deposit() {
        let legacy = alloy_consensus::Receipt::<Log> {
            status: alloy_consensus::Eip658Value::Eip658(true),
            cumulative_gas_used: 0,
            logs: vec![],
        };
        let receipt = make_receipt(OpReceipt::Legacy(legacy), None);
        assert_eq!(deposit_nonce_of(&receipt), None);
    }

    #[test]
    fn l1_fee_round_trips_from_block_info() {
        let legacy = alloy_consensus::Receipt::<Log> {
            status: alloy_consensus::Eip658Value::Eip658(true),
            cumulative_gas_used: 0,
            logs: vec![],
        };
        let receipt = make_receipt(OpReceipt::Legacy(legacy), Some(123_456));
        assert_eq!(l1_fee_of(&receipt), Some(123_456));
    }
}
