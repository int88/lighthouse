use crate::errors::HandleUnavailable;
use crate::{Error, HotColdDB, ItemStore};
use std::borrow::Cow;
use std::marker::PhantomData;
use types::{
    typenum::Unsigned, BeaconState, BeaconStateError, BlindedPayload, EthSpec, Hash256,
    SignedBeaconBlock, Slot,
};

/// Implemented for types that have ancestors (e.g., blocks, states) that may be iterated over.
/// 有ancestors的类型的实现（例如，blocks，states），可以用于遍历
///
/// ## Note
///
/// It is assumed that all ancestors for this object are stored in the database. If this is not the
/// case, the iterator will start returning `None` prior to genesis.
/// 它假设这个对象的所有ancestors存储在database中，如果不是这样的，则iterator会返回`None`，在genesis之前
pub trait AncestorIter<'a, E: EthSpec, Hot: ItemStore<E>, Cold: ItemStore<E>, I: Iterator> {
    /// Returns an iterator over the roots of the ancestors of `self`.
    fn try_iter_ancestor_roots(&self, store: &'a HotColdDB<E, Hot, Cold>) -> Option<I>;
}

impl<'a, E: EthSpec, Hot: ItemStore<E>, Cold: ItemStore<E>>
    AncestorIter<'a, E, Hot, Cold, BlockRootsIterator<'a, E, Hot, Cold>> for SignedBeaconBlock<E>
{
    /// Iterates across all available prior block roots of `self`, starting at the most recent and ending
    /// at genesis.
    fn try_iter_ancestor_roots(
        &self,
        store: &'a HotColdDB<E, Hot, Cold>,
    ) -> Option<BlockRootsIterator<'a, E, Hot, Cold>> {
        let state = store
            .get_state(&self.message().state_root(), Some(self.slot()))
            .ok()??;

        Some(BlockRootsIterator::owned(store, state))
    }
}

impl<'a, E: EthSpec, Hot: ItemStore<E>, Cold: ItemStore<E>>
    AncestorIter<'a, E, Hot, Cold, StateRootsIterator<'a, E, Hot, Cold>> for BeaconState<E>
{
    /// Iterates across all available prior state roots of `self`, starting at the most recent and ending
    /// at genesis.
    fn try_iter_ancestor_roots(
        &self,
        store: &'a HotColdDB<E, Hot, Cold>,
    ) -> Option<StateRootsIterator<'a, E, Hot, Cold>> {
        // The `self.clone()` here is wasteful.
        Some(StateRootsIterator::owned(store, self.clone()))
    }
}

pub struct StateRootsIterator<'a, T: EthSpec, Hot: ItemStore<T>, Cold: ItemStore<T>> {
    inner: RootsIterator<'a, T, Hot, Cold>,
}

impl<'a, T: EthSpec, Hot: ItemStore<T>, Cold: ItemStore<T>> Clone
    for StateRootsIterator<'a, T, Hot, Cold>
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<'a, T: EthSpec, Hot: ItemStore<T>, Cold: ItemStore<T>> StateRootsIterator<'a, T, Hot, Cold> {
    pub fn new(store: &'a HotColdDB<T, Hot, Cold>, beacon_state: &'a BeaconState<T>) -> Self {
        Self {
            inner: RootsIterator::new(store, beacon_state),
        }
    }

    pub fn owned(store: &'a HotColdDB<T, Hot, Cold>, beacon_state: BeaconState<T>) -> Self {
        Self {
            inner: RootsIterator::owned(store, beacon_state),
        }
    }
}

impl<'a, T: EthSpec, Hot: ItemStore<T>, Cold: ItemStore<T>> Iterator
    for StateRootsIterator<'a, T, Hot, Cold>
{
    type Item = Result<(Hash256, Slot), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|result| result.map(|(_, state_root, slot)| (state_root, slot)))
    }
}

/// Iterates backwards through block roots. If any specified slot is unable to be retrieved, the
/// iterator returns `None` indefinitely.
///
/// Uses the `block_roots` field of `BeaconState` as the source of block roots and will
/// perform a lookup on the `Store` for a prior `BeaconState` if `block_roots` has been
/// exhausted.
///
/// Returns `None` for roots prior to genesis or when there is an error reading from `Store`.
pub struct BlockRootsIterator<'a, T: EthSpec, Hot: ItemStore<T>, Cold: ItemStore<T>> {
    inner: RootsIterator<'a, T, Hot, Cold>,
}

impl<'a, T: EthSpec, Hot: ItemStore<T>, Cold: ItemStore<T>> Clone
    for BlockRootsIterator<'a, T, Hot, Cold>
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<'a, T: EthSpec, Hot: ItemStore<T>, Cold: ItemStore<T>> BlockRootsIterator<'a, T, Hot, Cold> {
    /// Create a new iterator over all block roots in the given `beacon_state` and prior states.
    pub fn new(store: &'a HotColdDB<T, Hot, Cold>, beacon_state: &'a BeaconState<T>) -> Self {
        Self {
            inner: RootsIterator::new(store, beacon_state),
        }
    }

    /// Create a new iterator over all block roots in the given `beacon_state` and prior states.
    pub fn owned(store: &'a HotColdDB<T, Hot, Cold>, beacon_state: BeaconState<T>) -> Self {
        Self {
            inner: RootsIterator::owned(store, beacon_state),
        }
    }

    pub fn from_block(
        store: &'a HotColdDB<T, Hot, Cold>,
        block_hash: Hash256,
    ) -> Result<Self, Error> {
        Ok(Self {
            inner: RootsIterator::from_block(store, block_hash)?,
        })
    }
}

impl<'a, T: EthSpec, Hot: ItemStore<T>, Cold: ItemStore<T>> Iterator
    for BlockRootsIterator<'a, T, Hot, Cold>
{
    type Item = Result<(Hash256, Slot), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|result| result.map(|(block_root, _, slot)| (block_root, slot)))
    }
}

/// Iterator over state and block roots that backtracks using the vectors from a `BeaconState`.
/// Iterator追踪遍历state以及block roots，使用来自`BeaconState`的vectors
pub struct RootsIterator<'a, T: EthSpec, Hot: ItemStore<T>, Cold: ItemStore<T>> {
    store: &'a HotColdDB<T, Hot, Cold>,
    beacon_state: Cow<'a, BeaconState<T>>,
    slot: Slot,
}

impl<'a, T: EthSpec, Hot: ItemStore<T>, Cold: ItemStore<T>> Clone
    for RootsIterator<'a, T, Hot, Cold>
{
    fn clone(&self) -> Self {
        Self {
            store: self.store,
            beacon_state: self.beacon_state.clone(),
            slot: self.slot,
        }
    }
}

impl<'a, T: EthSpec, Hot: ItemStore<T>, Cold: ItemStore<T>> RootsIterator<'a, T, Hot, Cold> {
    pub fn new(store: &'a HotColdDB<T, Hot, Cold>, beacon_state: &'a BeaconState<T>) -> Self {
        Self {
            store,
            // 设置beacon state的slot
            slot: beacon_state.slot(),
            beacon_state: Cow::Borrowed(beacon_state),
        }
    }

    pub fn owned(store: &'a HotColdDB<T, Hot, Cold>, beacon_state: BeaconState<T>) -> Self {
        Self {
            store,
            slot: beacon_state.slot(),
            beacon_state: Cow::Owned(beacon_state),
        }
    }

    pub fn from_block(
        store: &'a HotColdDB<T, Hot, Cold>,
        block_hash: Hash256,
    ) -> Result<Self, Error> {
        let block = store
            .get_blinded_block(&block_hash)?
            .ok_or_else(|| BeaconStateError::MissingBeaconBlock(block_hash.into()))?;
        let state = store
            .get_state(&block.state_root(), Some(block.slot()))?
            .ok_or_else(|| BeaconStateError::MissingBeaconState(block.state_root().into()))?;
        Ok(Self::owned(store, state))
    }

    fn do_next(&mut self) -> Result<Option<(Hash256, Hash256, Slot)>, Error> {
        // 如果slot为0或者self.slot大于beacon的slot，则直接返回
        if self.slot == 0 || self.slot > self.beacon_state.slot() {
            return Ok(None);
        }

        // 每迭代一次，slot减一
        self.slot -= 1;

        match (
            // 根据slot获取block和state
            self.beacon_state.get_block_root(self.slot),
            // 从beacon state本身保存的state root中获取block root和state root
            self.beacon_state.get_state_root(self.slot),
        ) {
            (Ok(block_root), Ok(state_root)) => Ok(Some((*block_root, *state_root, self.slot))),
            (Err(BeaconStateError::SlotOutOfBounds), Err(BeaconStateError::SlotOutOfBounds)) => {
                // Read a `BeaconState` from the store that has access to prior historical roots.
                // 从store中读取一个`BeaconState`，已经访问了historical roots之前的东西
                if let Some(beacon_state) =
                    next_historical_root_backtrack_state(self.store, &self.beacon_state)
                        .handle_unavailable()?
                {
                    // 拷贝beacon state
                    self.beacon_state = Cow::Owned(beacon_state);

                    // 获取block root和state root
                    let block_root = *self.beacon_state.get_block_root(self.slot)?;
                    let state_root = *self.beacon_state.get_state_root(self.slot)?;

                    Ok(Some((block_root, state_root, self.slot)))
                } else {
                    // No more states available due to weak subjectivity sync.
                    // 因为weak subjectivity sync，已经没有更多的states可用了
                    Ok(None)
                }
            }
            (Err(e), _) => Err(e.into()),
            (Ok(_), Err(e)) => Err(e.into()),
        }
    }
}

impl<'a, T: EthSpec, Hot: ItemStore<T>, Cold: ItemStore<T>> Iterator
    for RootsIterator<'a, T, Hot, Cold>
{
    /// (block_root, state_root, slot)
    type Item = Result<(Hash256, Hash256, Slot), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.do_next().transpose()
    }
}

/// Block iterator that uses the `parent_root` of each block to backtrack.
/// Block iterator，使用每个block的`parent_root`来回溯
pub struct ParentRootBlockIterator<'a, E: EthSpec, Hot: ItemStore<E>, Cold: ItemStore<E>> {
    store: &'a HotColdDB<E, Hot, Cold>,
    next_block_root: Hash256,
    decode_any_variant: bool,
    _phantom: PhantomData<E>,
}

impl<'a, E: EthSpec, Hot: ItemStore<E>, Cold: ItemStore<E>>
    ParentRootBlockIterator<'a, E, Hot, Cold>
{
    pub fn new(store: &'a HotColdDB<E, Hot, Cold>, start_block_root: Hash256) -> Self {
        Self {
            store,
            // 开始的block的root
            next_block_root: start_block_root,
            // 不解码
            decode_any_variant: false,
            _phantom: PhantomData,
        }
    }

    /// Block iterator that is tolerant of blocks that have the wrong fork for their slot.
    /// Block iterator，能容忍blocks，在它们的slot有着错误的fork
    pub fn fork_tolerant(store: &'a HotColdDB<E, Hot, Cold>, start_block_root: Hash256) -> Self {
        Self {
            store,
            next_block_root: start_block_root,
            decode_any_variant: true,
            _phantom: PhantomData,
        }
    }

    #[allow(clippy::type_complexity)]
    fn do_next(
        &mut self,
    ) -> Result<Option<(Hash256, SignedBeaconBlock<E, BlindedPayload<E>>)>, Error> {
        // Stop once we reach the zero parent, otherwise we'll keep returning the genesis
        // block forever.
        // 停止，一旦我们到达了zero parent，否则我们会继续，直到genesis block
        if self.next_block_root.is_zero() {
            Ok(None)
        } else {
            let block_root = self.next_block_root;
            let block = if self.decode_any_variant {
                self.store.get_block_any_variant(&block_root)
            } else {
                self.store.get_blinded_block(&block_root)
            }?
            .ok_or(Error::BlockNotFound(block_root))?;
            // 将parent block作为next_block_root
            self.next_block_root = block.message().parent_root();
            Ok(Some((block_root, block)))
        }
    }
}

// 实现了Iterator
impl<'a, E: EthSpec, Hot: ItemStore<E>, Cold: ItemStore<E>> Iterator
    for ParentRootBlockIterator<'a, E, Hot, Cold>
{
    // 定义Item
    type Item = Result<(Hash256, SignedBeaconBlock<E, BlindedPayload<E>>), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.do_next().transpose()
    }
}

#[derive(Clone)]
/// Extends `BlockRootsIterator`, returning `SignedBeaconBlock` instances, instead of their roots.
pub struct BlockIterator<'a, T: EthSpec, Hot: ItemStore<T>, Cold: ItemStore<T>> {
    roots: BlockRootsIterator<'a, T, Hot, Cold>,
}

impl<'a, T: EthSpec, Hot: ItemStore<T>, Cold: ItemStore<T>> BlockIterator<'a, T, Hot, Cold> {
    /// Create a new iterator over all blocks in the given `beacon_state` and prior states.
    pub fn new(store: &'a HotColdDB<T, Hot, Cold>, beacon_state: &'a BeaconState<T>) -> Self {
        Self {
            roots: BlockRootsIterator::new(store, beacon_state),
        }
    }

    /// Create a new iterator over all blocks in the given `beacon_state` and prior states.
    pub fn owned(store: &'a HotColdDB<T, Hot, Cold>, beacon_state: BeaconState<T>) -> Self {
        Self {
            roots: BlockRootsIterator::owned(store, beacon_state),
        }
    }

    fn do_next(&mut self) -> Result<Option<SignedBeaconBlock<T, BlindedPayload<T>>>, Error> {
        if let Some(result) = self.roots.next() {
            let (root, _slot) = result?;
            self.roots.inner.store.get_blinded_block(&root)
        } else {
            Ok(None)
        }
    }
}

impl<'a, T: EthSpec, Hot: ItemStore<T>, Cold: ItemStore<T>> Iterator
    for BlockIterator<'a, T, Hot, Cold>
{
    type Item = Result<SignedBeaconBlock<T, BlindedPayload<T>>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.do_next().transpose()
    }
}

/// Fetch the next state to use whilst backtracking in `*RootsIterator`.
/// 获取下一个state使用，同时回溯`*RootsIterator`
///
/// Return `Err(HistoryUnavailable)` in the case where no more backtrack states are available
/// due to weak subjectivity sync.
/// 返回`Err(HistoryUnavailable)`万一没有更多的backtrack state可用，由于weak subjectivity sync
fn next_historical_root_backtrack_state<E: EthSpec, Hot: ItemStore<E>, Cold: ItemStore<E>>(
    store: &HotColdDB<E, Hot, Cold>,
    current_state: &BeaconState<E>,
) -> Result<BeaconState<E>, Error> {
    // For compatibility with the freezer database's restore points, we load a state at
    // a restore point slot (thus avoiding replaying blocks). In the case where we're
    // not frozen, this just means we might not jump back by the maximum amount on
    // our first jump (i.e. at most 1 extra state load).
    // 为了和freezer database的restore points保持一致，我们加载一个在restore point slot的state
    // 因此避免了replaying blocks，万一我们没有frozen，这意味着我们可能不能jump back，通过我们first jump
    // 的maximum amount
    let new_state_slot = slot_of_prev_restore_point::<E>(current_state.slot());

    let (_, historic_state_upper_limit) = store.get_historic_state_limits();

    if new_state_slot >= historic_state_upper_limit {
        let new_state_root = current_state.get_state_root(new_state_slot)?;
        Ok(store
            // 获取之前的restore point的state
            .get_state(new_state_root, Some(new_state_slot))?
            .ok_or_else(|| BeaconStateError::MissingBeaconState((*new_state_root).into()))?)
    } else {
        Err(Error::HistoryUnavailable)
    }
}

/// Compute the slot of the last guaranteed restore point in the freezer database.
/// 计算freezer database中最新的restore point
fn slot_of_prev_restore_point<E: EthSpec>(current_slot: Slot) -> Slot {
    let slots_per_historical_root = E::SlotsPerHistoricalRoot::to_u64();
    (current_slot - 1) / slots_per_historical_root * slots_per_historical_root
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::HotColdDB;
    use crate::StoreConfig as Config;
    use beacon_chain::test_utils::BeaconChainHarness;
    use beacon_chain::types::{ChainSpec, MainnetEthSpec};
    use sloggers::{null::NullLoggerBuilder, Build};

    fn get_state<T: EthSpec>() -> BeaconState<T> {
        let harness = BeaconChainHarness::builder(T::default())
            .default_spec()
            .deterministic_keypairs(1)
            .fresh_ephemeral_store()
            .build();
        harness.advance_slot();
        harness.get_current_state()
    }

    #[test]
    fn block_root_iter() {
        let log = NullLoggerBuilder.build().unwrap();
        let store =
            HotColdDB::open_ephemeral(Config::default(), ChainSpec::minimal(), log).unwrap();
        let slots_per_historical_root = MainnetEthSpec::slots_per_historical_root();

        let mut state_a: BeaconState<MainnetEthSpec> = get_state();
        let mut state_b: BeaconState<MainnetEthSpec> = get_state();

        *state_a.slot_mut() = Slot::from(slots_per_historical_root);
        *state_b.slot_mut() = Slot::from(slots_per_historical_root * 2);

        let mut hashes = (0..).map(Hash256::from_low_u64_be);
        let roots_a = state_a.block_roots_mut();
        for i in 0..roots_a.len() {
            roots_a[i] = hashes.next().unwrap()
        }
        let roots_b = state_b.block_roots_mut();
        for i in 0..roots_b.len() {
            roots_b[i] = hashes.next().unwrap()
        }

        let state_a_root = hashes.next().unwrap();
        state_b.state_roots_mut()[0] = state_a_root;
        store.put_state(&state_a_root, &state_a).unwrap();

        let iter = BlockRootsIterator::new(&store, &state_b);

        assert!(
            iter.clone()
                .any(|result| result.map(|(_root, slot)| slot == 0).unwrap()),
            "iter should contain zero slot"
        );

        let mut collected: Vec<(Hash256, Slot)> = iter.collect::<Result<Vec<_>, _>>().unwrap();
        collected.reverse();

        let expected_len = 2 * MainnetEthSpec::slots_per_historical_root();

        assert_eq!(collected.len(), expected_len);

        for (i, item) in collected.iter().enumerate() {
            assert_eq!(item.0, Hash256::from_low_u64_be(i as u64));
        }
    }

    #[test]
    fn state_root_iter() {
        let log = NullLoggerBuilder.build().unwrap();
        let store =
            HotColdDB::open_ephemeral(Config::default(), ChainSpec::minimal(), log).unwrap();
        let slots_per_historical_root = MainnetEthSpec::slots_per_historical_root();

        let mut state_a: BeaconState<MainnetEthSpec> = get_state();
        let mut state_b: BeaconState<MainnetEthSpec> = get_state();

        // 每个historical root的slots
        *state_a.slot_mut() = Slot::from(slots_per_historical_root);
        *state_b.slot_mut() = Slot::from(slots_per_historical_root * 2);

        let mut hashes = (0..).map(Hash256::from_low_u64_be);

        for slot in 0..slots_per_historical_root {
            // 在state中设置一系列的state root
            state_a
                .set_state_root(Slot::from(slot), hashes.next().unwrap())
                .unwrap_or_else(|_| panic!("should set state_a slot {}", slot));
        }
        for slot in slots_per_historical_root..slots_per_historical_root * 2 {
            state_b
                .set_state_root(Slot::from(slot), hashes.next().unwrap())
                .unwrap_or_else(|_| panic!("should set state_b slot {}", slot));
        }

        // 设置state a的root和state b的root
        let state_a_root = Hash256::from_low_u64_be(slots_per_historical_root as u64);
        let state_b_root = Hash256::from_low_u64_be(slots_per_historical_root as u64 * 2);

        // 将state放入store
        store.put_state(&state_a_root, &state_a).unwrap();
        store.put_state(&state_b_root, &state_b).unwrap();

        // 构建state roots iterator
        let iter = StateRootsIterator::new(&store, &state_b);

        assert!(
            iter.clone()
                .any(|result| result.map(|(_root, slot)| slot == 0).unwrap()),
            // iter应该包含zero slot
            "iter should contain zero slot"
        );

        let mut collected: Vec<(Hash256, Slot)> = iter.collect::<Result<Vec<_>, _>>().unwrap();
        collected.reverse();

        let expected_len = MainnetEthSpec::slots_per_historical_root() * 2;

        assert_eq!(collected.len(), expected_len, "collection length incorrect");

        for (i, item) in collected.iter().enumerate() {
            let (hash, slot) = *item;

            assert_eq!(slot, i as u64, "slot mismatch at {}: {} vs {}", i, slot, i);

            // 保证哈希值相等
            assert_eq!(
                hash,
                Hash256::from_low_u64_be(i as u64),
                "hash mismatch at {}",
                i
            );
        }
    }
}
