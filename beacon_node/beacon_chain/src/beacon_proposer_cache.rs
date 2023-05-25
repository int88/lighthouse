//! The `BeaconProposer` cache stores the proposer indices for some epoch.
//! `BeaconProposer` cache为一些epoch缓存proposer indices
//!
//! This cache is keyed by `(epoch, block_root)` where `block_root` is the block root at
//! `end_slot(epoch - 1)`. We make the assertion that the proposer shuffling is identical for all
//! blocks in `epoch` which share the common ancestor of `block_root`.
//! cache用`(epoch, block_root)`作为索引 ，其中`block_root`是在`end_slot(epoch - 1)`的block root
//!
//! The cache is a fairly unintelligent LRU cache that is not pruned after finality. This makes it
//! very simple to reason about, but it might store values that are useless due to finalization. The
//! values it stores are very small, so this should not be an issue.

use crate::{BeaconChain, BeaconChainError, BeaconChainTypes};
use fork_choice::ExecutionStatus;
use lru::LruCache;
use smallvec::SmallVec;
use state_processing::state_advance::partial_state_advance;
use std::cmp::Ordering;
use types::{
    BeaconState, BeaconStateError, ChainSpec, CloneConfig, Epoch, EthSpec, Fork, Hash256, Slot,
    Unsigned,
};

/// The number of sets of proposer indices that should be cached.
const CACHE_SIZE: usize = 16;

/// This value is fairly unimportant, it's used to avoid heap allocations. The result of it being
/// incorrect is non-substantial from a consensus perspective (and probably also from a
/// performance perspective).
const TYPICAL_SLOTS_PER_EPOCH: usize = 32;

/// For some given slot, this contains the proposer index (`index`) and the `fork` that should be
/// used to verify their signature.
/// 对于一些给定的slot，它包含proposer index（`index`）和应该用来验证他们签名的`fork`
pub struct Proposer {
    pub index: usize,
    pub fork: Fork,
}

/// The list of proposers for some given `epoch`, alongside the `fork` that should be used to verify
/// their signatures.
/// 一系列proposers，对于一些给定的`epoch`，并且`fork`应该用来验证他们的签名
pub struct EpochBlockProposers {
    /// The epoch to which the proposers pertain.
    /// 提案人相关的epoch
    epoch: Epoch,
    /// The fork that should be used to verify proposer signatures.
    /// 用于校验proposer signatures的fork
    fork: Fork,
    /// A list of length `T::EthSpec::slots_per_epoch()`, representing the proposers for each slot
    /// in that epoch.
    /// 一个长度为`T::EthSpec::slots_per_epoch()`的列表，表示epoch中每个slot的proposers
    ///
    /// E.g., if `self.epoch == 1`, then `self.proposers[0]` contains the proposer for slot `32`.
    /// 如果`self.epoch == 1`，那么`self.proposers[0]`包含slot `32`的proposer
    proposers: SmallVec<[usize; TYPICAL_SLOTS_PER_EPOCH]>,
}

/// A cache to store the proposers for some epoch.
/// 一个缓存用来存储proposers，对于一些epoch
///
/// See the module-level documentation for more information.
pub struct BeaconProposerCache {
    cache: LruCache<(Epoch, Hash256), EpochBlockProposers>,
}

impl Default for BeaconProposerCache {
    fn default() -> Self {
        Self {
            cache: LruCache::new(CACHE_SIZE),
        }
    }
}

impl BeaconProposerCache {
    /// If it is cached, returns the proposer for the block at `slot` where the block has the
    /// ancestor block root of `shuffling_decision_block` at `end_slot(slot.epoch() - 1)`.
    /// 如果被缓存了，返回在`slot`的block的proposer，其中block有ancestor root，在`shuffling_decision_block`
    /// 在`end_slot(slot.epoch() - 1)`，上一个epocoh的end slot
    pub fn get_slot<T: EthSpec>(
        &mut self,
        shuffling_decision_block: Hash256,
        slot: Slot,
    ) -> Option<Proposer> {
        // 获取slot对应的epoch
        let epoch = slot.epoch(T::slots_per_epoch());
        let key = (epoch, shuffling_decision_block);
        // 从cache中查找
        if let Some(cache) = self.cache.get(&key) {
            // This `if` statement is likely unnecessary, but it feels like good practice.
            // 这个`if`语句可能是不必要的，但是感觉像是好的实践
            if epoch == cache.epoch {
                cache
                    .proposers
                    .get(slot.as_usize() % T::SlotsPerEpoch::to_usize())
                    .map(|&index| Proposer {
                        index,
                        fork: cache.fork,
                    })
            } else {
                None
            }
        } else {
            None
        }
    }

    /// As per `Self::get_slot`, but returns all proposers in all slots for the given `epoch`.
    ///
    /// The nth slot in the returned `SmallVec` will be equal to the nth slot in the given `epoch`.
    /// E.g., if `epoch == 1` then `smallvec[0]` refers to slot 32 (assuming `SLOTS_PER_EPOCH ==
    /// 32`).
    pub fn get_epoch<T: EthSpec>(
        &mut self,
        shuffling_decision_block: Hash256,
        epoch: Epoch,
    ) -> Option<&SmallVec<[usize; TYPICAL_SLOTS_PER_EPOCH]>> {
        let key = (epoch, shuffling_decision_block);
        self.cache.get(&key).map(|cache| &cache.proposers)
    }

    /// Insert the proposers into the cache.
    /// 将proposer插入cache
    ///
    /// See `Self::get` for a description of `shuffling_decision_block`.
    ///
    /// The `fork` value must be valid to verify proposer signatures in `epoch`.
    pub fn insert(
        &mut self,
        epoch: Epoch,
        shuffling_decision_block: Hash256,
        proposers: Vec<usize>,
        fork: Fork,
    ) -> Result<(), BeaconStateError> {
        // epoch和shuffling_decision_block作为key
        let key = (epoch, shuffling_decision_block);
        if !self.cache.contains(&key) {
            self.cache.put(
                key,
                // 插入epoch和fork
                EpochBlockProposers {
                    epoch,
                    fork,
                    proposers: proposers.into(),
                },
            );
        }

        Ok(())
    }
}

/// Compute the proposer duties using the head state without cache.
/// 计算proposer duties，使用head state，不使用cache
pub fn compute_proposer_duties_from_head<T: BeaconChainTypes>(
    current_epoch: Epoch,
    chain: &BeaconChain<T>,
) -> Result<(Vec<usize>, Hash256, ExecutionStatus, Fork), BeaconChainError> {
    // Atomically collect information about the head whilst holding the canonical head `Arc` as
    // short as possible.
    // 原子地收集有关head的信息，同时尽可能短地持有canonical head `Arc`
    let (mut state, head_state_root, head_block_root) = {
        let head = chain.canonical_head.cached_head();
        // Take a copy of the head state.
        // 拷贝head state
        let head_state = head
            .snapshot
            .beacon_state
            .clone_with(CloneConfig::committee_caches_only());
        let head_state_root = head.head_state_root();
        let head_block_root = head.head_block_root();
        (head_state, head_state_root, head_block_root)
    };

    let execution_status = chain
        .canonical_head
        .fork_choice_read_lock()
        .get_block_execution_status(&head_block_root)
        .ok_or(BeaconChainError::HeadMissingFromForkChoice(head_block_root))?;

    // Advance the state into the requested epoch.
    // 移动state到请求的epoch
    ensure_state_is_in_epoch(&mut state, head_state_root, current_epoch, &chain.spec)?;

    // 获取beacon proposer indices
    let indices = state
        .get_beacon_proposer_indices(&chain.spec)
        .map_err(BeaconChainError::from)?;

    let dependent_root = state
        // The only block which decides its own shuffling is the genesis block.
        // 唯一决定自己shuffling的block是genesis block
        .proposer_shuffling_decision_root(chain.genesis_block_root)
        .map_err(BeaconChainError::from)?;

    Ok((indices, dependent_root, execution_status, state.fork()))
}

/// If required, advance `state` to `target_epoch`.
/// 如果需要的话，移动`state`到`target_epoch`
///
/// ## Details
///
/// - Returns an error if `state.current_epoch() > target_epoch`.
/// - 返回error，如果`state.current_epoch() > target_epoch`
/// - No-op if `state.current_epoch() == target_epoch`.
/// - No-op如果`state.current_epoch() == target_epoch`
/// - It must be the case that `state.canonical_root() == state_root`, but this function will not
///     check that.
/// - 必须是`state.canonical_root() == state_root`，但是这个函数不会检查
pub fn ensure_state_is_in_epoch<E: EthSpec>(
    state: &mut BeaconState<E>,
    state_root: Hash256,
    target_epoch: Epoch,
    spec: &ChainSpec,
) -> Result<(), BeaconChainError> {
    match state.current_epoch().cmp(&target_epoch) {
        // Protects against an inconsistent slot clock.
        // 保护不一致的slot clock
        Ordering::Greater => Err(BeaconStateError::SlotOutOfBounds.into()),
        // The state needs to be advanced.
        // state需要移动
        Ordering::Less => {
            let target_slot = target_epoch.start_slot(E::slots_per_epoch());
            partial_state_advance(state, Some(state_root), target_slot, spec)
                .map_err(BeaconChainError::from)
        }
        // The state is suitable, nothing to do.
        // state是合适的，什么都不做
        Ordering::Equal => Ok(()),
    }
}
