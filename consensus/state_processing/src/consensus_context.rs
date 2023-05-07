use crate::common::get_indexed_attestation;
use crate::per_block_processing::errors::{AttestationInvalid, BlockOperationError};
use std::collections::{hash_map::Entry, HashMap};
use std::marker::PhantomData;
use tree_hash::TreeHash;
use types::{
    AbstractExecPayload, Attestation, AttestationData, BeaconState, BeaconStateError, BitList,
    ChainSpec, Epoch, EthSpec, Hash256, IndexedAttestation, SignedBeaconBlock, Slot,
};

#[derive(Debug)]
pub struct ConsensusContext<T: EthSpec> {
    /// Slot to act as an identifier/safeguard
    /// Slot作为一个标识符/保障
    slot: Slot,
    /// Proposer index of the block at `slot`.
    /// 在`slot`的block的proposer index
    proposer_index: Option<u64>,
    /// Block root of the block at `slot`.
    /// 在`slot`的block的block root
    current_block_root: Option<Hash256>,
    /// Cache of indexed attestations constructed during block processing.
    /// 缓存的indexed attestations，在block处理期间构建
    indexed_attestations:
        HashMap<(AttestationData, BitList<T::MaxValidatorsPerCommittee>), IndexedAttestation<T>>,
    _phantom: PhantomData<T>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum ContextError {
    BeaconState(BeaconStateError),
    SlotMismatch { slot: Slot, expected: Slot },
    EpochMismatch { epoch: Epoch, expected: Epoch },
}

impl From<BeaconStateError> for ContextError {
    fn from(e: BeaconStateError) -> Self {
        Self::BeaconState(e)
    }
}

impl<T: EthSpec> ConsensusContext<T> {
    pub fn new(slot: Slot) -> Self {
        Self {
            slot,
            proposer_index: None,
            current_block_root: None,
            indexed_attestations: HashMap::new(),
            _phantom: PhantomData,
        }
    }

    pub fn set_proposer_index(mut self, proposer_index: u64) -> Self {
        self.proposer_index = Some(proposer_index);
        self
    }

    /// Strict method for fetching the proposer index.
    /// 严格的方法用于获取proposer index
    ///
    /// Gets the proposer index for `self.slot` while ensuring that it matches `state.slot()`. This
    /// method should be used in block processing and almost everywhere the proposer index is
    /// required. If the slot check is too restrictive, see `get_proposer_index_from_epoch_state`.
    /// 为`self.slot`获取proposer index，同时确保它与`state.slot()`匹配。这个方法应该在block处理中使用，几乎在需要proposer index的任何地方都可以使用。如果slot检查太严格，请参见`get_proposer_index_from_epoch_state`。
    pub fn get_proposer_index(
        &mut self,
        state: &BeaconState<T>,
        spec: &ChainSpec,
    ) -> Result<u64, ContextError> {
        self.check_slot(state.slot())?;
        self.get_proposer_index_no_checks(state, spec)
    }

    /// More liberal method for fetching the proposer index.
    ///
    /// Fetches the proposer index for `self.slot` but does not require the state to be from an
    /// exactly matching slot (merely a matching epoch). This is useful in batch verification where
    /// we want to extract the proposer index from a single state for every slot in the epoch.
    pub fn get_proposer_index_from_epoch_state(
        &mut self,
        state: &BeaconState<T>,
        spec: &ChainSpec,
    ) -> Result<u64, ContextError> {
        self.check_epoch(state.current_epoch())?;
        self.get_proposer_index_no_checks(state, spec)
    }

    fn get_proposer_index_no_checks(
        &mut self,
        state: &BeaconState<T>,
        spec: &ChainSpec,
    ) -> Result<u64, ContextError> {
        if let Some(proposer_index) = self.proposer_index {
            return Ok(proposer_index);
        }

        let proposer_index = state.get_beacon_proposer_index(self.slot, spec)? as u64;
        self.proposer_index = Some(proposer_index);
        Ok(proposer_index)
    }

    pub fn set_current_block_root(mut self, block_root: Hash256) -> Self {
        self.current_block_root = Some(block_root);
        self
    }

    pub fn get_current_block_root<Payload: AbstractExecPayload<T>>(
        &mut self,
        block: &SignedBeaconBlock<T, Payload>,
    ) -> Result<Hash256, ContextError> {
        self.check_slot(block.slot())?;

        if let Some(current_block_root) = self.current_block_root {
            return Ok(current_block_root);
        }

        let current_block_root = block.message().tree_hash_root();
        self.current_block_root = Some(current_block_root);
        Ok(current_block_root)
    }

    fn check_slot(&self, slot: Slot) -> Result<(), ContextError> {
        if slot == self.slot {
            Ok(())
        } else {
            Err(ContextError::SlotMismatch {
                slot,
                expected: self.slot,
            })
        }
    }

    fn check_epoch(&self, epoch: Epoch) -> Result<(), ContextError> {
        let expected = self.slot.epoch(T::slots_per_epoch());
        if epoch == expected {
            Ok(())
        } else {
            Err(ContextError::EpochMismatch { epoch, expected })
        }
    }

    pub fn get_indexed_attestation(
        &mut self,
        state: &BeaconState<T>,
        attestation: &Attestation<T>,
    ) -> Result<&IndexedAttestation<T>, BlockOperationError<AttestationInvalid>> {
        // 将attestation data以及aggregation bits作为key
        let key = (
            attestation.data.clone(),
            attestation.aggregation_bits.clone(),
        );

        match self.indexed_attestations.entry(key) {
            Entry::Occupied(occupied) => Ok(occupied.into_mut()),
            Entry::Vacant(vacant) => {
                // 获取committee
                let committee =
                    state.get_beacon_committee(attestation.data.slot, attestation.data.index)?;
                // 生成indexed attestation
                let indexed_attestation =
                    get_indexed_attestation(committee.committee, attestation)?;
                Ok(vacant.insert(indexed_attestation))
            }
        }
    }

    pub fn num_cached_indexed_attestations(&self) -> usize {
        self.indexed_attestations.len()
    }
}
