use proto_array::JustifiedBalances;
use std::collections::BTreeSet;
use std::fmt::Debug;
use types::{AbstractExecPayload, BeaconBlockRef, BeaconState, Checkpoint, EthSpec, Hash256, Slot};

/// Approximates the `Store` in "Ethereum 2.0 Phase 0 -- Beacon Chain Fork Choice":
/// 
///
/// https://github.com/ethereum/eth2.0-specs/blob/v0.12.1/specs/phase0/fork-choice.md#store
///
/// ## Detail
///
/// This is only an approximation for two reasons:
/// 这只是一个近似值，原因有两个：
///
/// - This crate stores the actual block DAG in `ProtoArrayForkChoice`.
/// - 这个crate在`ProtoArrayForkChoice`中存储了实际的块DAG。
/// - `time` is represented using `Slot` instead of UNIX epoch `u64`.
/// - `time`是用`Slot`表示的，而不是UNIX epoch `u64`。
///
/// ## Motiviation
///
/// The primary motivation for defining this as a trait to be implemented upstream rather than a
/// concrete struct is to allow this crate to be free from "impure" on-disk database logic,
/// hopefully making auditing easier.
/// 定义为一个上游实现的trait而不是一个具体的struct的主要动机是，使这个crate摆脱“不纯”的磁盘数据库逻辑，希望使审计更容易。
pub trait ForkChoiceStore<T: EthSpec>: Sized {
    type Error: Debug;

    /// Returns the last value passed to `Self::set_current_slot`.
    fn get_current_slot(&self) -> Slot;

    /// Set the value to be returned by `Self::get_current_slot`.
    ///
    /// ## Notes
    ///
    /// This should only ever be called from within `ForkChoice::on_tick`.
    fn set_current_slot(&mut self, slot: Slot);

    /// Called whenever `ForkChoice::on_block` has verified a block, but not yet added it to fork
    /// choice. Allows the implementer to performing caching or other housekeeping duties.
    fn on_verified_block<Payload: AbstractExecPayload<T>>(
        &mut self,
        block: BeaconBlockRef<T, Payload>,
        block_root: Hash256,
        state: &BeaconState<T>,
    ) -> Result<(), Self::Error>;

    /// Returns the `justified_checkpoint`.
    fn justified_checkpoint(&self) -> &Checkpoint;

    /// Returns balances from the `state` identified by `justified_checkpoint.root`.
    fn justified_balances(&self) -> &JustifiedBalances;

    /// Returns the `finalized_checkpoint`.
    fn finalized_checkpoint(&self) -> &Checkpoint;

    /// Returns the `unrealized_justified_checkpoint`.
    fn unrealized_justified_checkpoint(&self) -> &Checkpoint;

    /// Returns the `unrealized_finalized_checkpoint`.
    fn unrealized_finalized_checkpoint(&self) -> &Checkpoint;

    /// Returns the `proposer_boost_root`.
    fn proposer_boost_root(&self) -> Hash256;

    /// Sets `finalized_checkpoint`.
    fn set_finalized_checkpoint(&mut self, checkpoint: Checkpoint);

    /// Sets the `justified_checkpoint`.
    fn set_justified_checkpoint(&mut self, checkpoint: Checkpoint) -> Result<(), Self::Error>;

    /// Sets the `unrealized_justified_checkpoint`.
    fn set_unrealized_justified_checkpoint(&mut self, checkpoint: Checkpoint);

    /// Sets the `unrealized_finalized_checkpoint`.
    fn set_unrealized_finalized_checkpoint(&mut self, checkpoint: Checkpoint);

    /// Sets the proposer boost root.
    fn set_proposer_boost_root(&mut self, proposer_boost_root: Hash256);

    /// Gets the equivocating indices.
    fn equivocating_indices(&self) -> &BTreeSet<u64>;

    /// Adds to the set of equivocating indices.
    fn extend_equivocating_indices(&mut self, indices: impl IntoIterator<Item = u64>);
}
