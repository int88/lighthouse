use crate::chunked_vector::{
    store_updated_vector, BlockRoots, HistoricalRoots, HistoricalSummaries, RandaoMixes, StateRoots,
};
use crate::config::{
    OnDiskStoreConfig, StoreConfig, DEFAULT_SLOTS_PER_RESTORE_POINT,
    PREV_DEFAULT_SLOTS_PER_RESTORE_POINT,
};
use crate::forwards_iter::{HybridForwardsBlockRootsIterator, HybridForwardsStateRootsIterator};
use crate::impls::beacon_state::{get_full_state, store_full_state};
use crate::iter::{BlockRootsIterator, ParentRootBlockIterator, RootsIterator};
use crate::leveldb_store::BytesKey;
use crate::leveldb_store::LevelDB;
use crate::memory_store::MemoryStore;
use crate::metadata::{
    AnchorInfo, CompactionTimestamp, PruningCheckpoint, SchemaVersion, ANCHOR_INFO_KEY,
    COMPACTION_TIMESTAMP_KEY, CONFIG_KEY, CURRENT_SCHEMA_VERSION, PRUNING_CHECKPOINT_KEY,
    SCHEMA_VERSION_KEY, SPLIT_KEY,
};
use crate::metrics;
use crate::{
    get_key_for_col, DBColumn, DatabaseBlock, Error, ItemStore, KeyValueStoreOp,
    PartialBeaconState, StoreItem, StoreOp,
};
use itertools::process_results;
use leveldb::iterator::LevelDBIterator;
use lru::LruCache;
use parking_lot::{Mutex, RwLock};
use serde_derive::{Deserialize, Serialize};
use slog::{debug, error, info, trace, warn, Logger};
use ssz::{Decode, Encode};
use ssz_derive::{Decode, Encode};
use state_processing::{
    BlockProcessingError, BlockReplayer, SlotProcessingError, StateRootStrategy,
};
use std::cmp::min;
use std::convert::TryInto;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use types::*;

/// On-disk database that stores finalized states efficiently.
/// 磁盘上的数据库，高效地存储finalized states
///
/// Stores vector fields like the `block_roots` and `state_roots` separately, and only stores
/// intermittent "restore point" states pre-finalization.
/// 另外存储vector字段，例如`block_roots`以及`state_roots`
#[derive(Debug)]
pub struct HotColdDB<E: EthSpec, Hot: ItemStore<E>, Cold: ItemStore<E>> {
    /// The slot and state root at the point where the database is split between hot and cold.
    ///
    /// States with slots less than `split.slot` are in the cold DB, while states with slots
    /// greater than or equal are in the hot DB.
    /// slots小于`split.slot`的states存放在cold DB，同时slots大于等于的states则放在hot DB
    pub(crate) split: RwLock<Split>,
    /// The starting slots for the range of blocks & states stored in the database.
    /// 对于一系列存储在数据库中的blocks & states的starting slots
    anchor_info: RwLock<Option<AnchorInfo>>,
    pub(crate) config: StoreConfig,
    /// Cold database containing compact historical data.
    /// Cold database包含压缩后的历史数据
    pub cold_db: Cold,
    /// Hot database containing duplicated but quick-to-access recent data.
    /// Hot database包含了重复但是能快速访问的recent data
    ///
    /// The hot database also contains all blocks.
    pub hot_db: Hot,
    /// LRU cache of deserialized blocks. Updated whenever a block is loaded.
    /// 反序列化blocks的LRU cache，当block加载的时候更新
    block_cache: Mutex<LruCache<Hash256, SignedBeaconBlock<E>>>,
    /// Chain spec.
    pub(crate) spec: ChainSpec,
    /// Logger.
    pub(crate) log: Logger,
    /// Mere vessel for E.
    _phantom: PhantomData<E>,
}

#[derive(Debug, PartialEq)]
pub enum HotColdDBError {
    UnsupportedSchemaVersion {
        target_version: SchemaVersion,
        current_version: SchemaVersion,
    },
    /// Recoverable error indicating that the database freeze point couldn't be updated
    /// due to the finalized block not lying on an epoch boundary (should be infrequent).
    /// 可恢复的错误，表明数据库的freeze point不应该被更新，由于finalized block，不是存在于epoch boundary
    /// （不会很频繁）
    FreezeSlotUnaligned(Slot),
    FreezeSlotError {
        current_split_slot: Slot,
        proposed_split_slot: Slot,
    },
    MissingStateToFreeze(Hash256),
    MissingRestorePointHash(u64),
    MissingRestorePoint(Hash256),
    MissingColdStateSummary(Hash256),
    MissingHotStateSummary(Hash256),
    MissingEpochBoundaryState(Hash256),
    MissingSplitState(Hash256, Slot),
    MissingExecutionPayload(Hash256),
    MissingFullBlockExecutionPayloadPruned(Hash256, Slot),
    MissingAnchorInfo,
    HotStateSummaryError(BeaconStateError),
    RestorePointDecodeError(ssz::DecodeError),
    BlockReplayBeaconError(BeaconStateError),
    BlockReplaySlotError(SlotProcessingError),
    BlockReplayBlockError(BlockProcessingError),
    MissingLowerLimitState(Slot),
    InvalidSlotsPerRestorePoint {
        slots_per_restore_point: u64,
        slots_per_historical_root: u64,
        slots_per_epoch: u64,
    },
    RestorePointBlockHashError(BeaconStateError),
    IterationError {
        unexpected_key: BytesKey,
    },
    AttestationStateIsFinalized {
        split_slot: Slot,
        request_slot: Option<Slot>,
        state_root: Hash256,
    },
}

impl<E: EthSpec> HotColdDB<E, MemoryStore<E>, MemoryStore<E>> {
    pub fn open_ephemeral(
        config: StoreConfig,
        spec: ChainSpec,
        log: Logger,
    ) -> Result<HotColdDB<E, MemoryStore<E>, MemoryStore<E>>, Error> {
        Self::verify_slots_per_restore_point(config.slots_per_restore_point)?;

        // 构建一个memory db
        let db = HotColdDB {
            split: RwLock::new(Split::default()),
            anchor_info: RwLock::new(None),
            cold_db: MemoryStore::open(),
            hot_db: MemoryStore::open(),
            block_cache: Mutex::new(LruCache::new(config.block_cache_size)),
            config,
            spec,
            log,
            _phantom: PhantomData,
        };

        Ok(db)
    }
}

impl<E: EthSpec> HotColdDB<E, LevelDB<E>, LevelDB<E>> {
    /// Open a new or existing database, with the given paths to the hot and cold DBs.
    /// 打开一个新的或者已经存在的数据库，给定的路径作为hot以及cold DBs
    ///
    /// The `slots_per_restore_point` parameter must be a divisor of `SLOTS_PER_HISTORICAL_ROOT`.
    /// 参数`slots_per_restore_point`必须是`SLOTS_PER_HISTORICAL_ROOT`的除数
    ///
    /// The `migrate_schema` function is passed in so that the parent `BeaconChain` can provide
    /// context and access `BeaconChain`-level code without creating a circular dependency.
    /// `migration_schema`函数被传递，这也parent `BeaconChain`可以提供上下文以及访问`BeaconChain`级别的代码
    /// 而不用创建循环依赖
    pub fn open(
        hot_path: &Path,
        cold_path: &Path,
        migrate_schema: impl FnOnce(Arc<Self>, SchemaVersion, SchemaVersion) -> Result<(), Error>,
        config: StoreConfig,
        spec: ChainSpec,
        log: Logger,
    ) -> Result<Arc<Self>, Error> {
        Self::verify_slots_per_restore_point(config.slots_per_restore_point)?;

        // 构建hot cold db
        let mut db = HotColdDB {
            split: RwLock::new(Split::default()),
            anchor_info: RwLock::new(None),
            cold_db: LevelDB::open(cold_path)?,
            hot_db: LevelDB::open(hot_path)?,
            block_cache: Mutex::new(LruCache::new(config.block_cache_size)),
            config,
            spec,
            log,
            _phantom: PhantomData,
        };

        // Allow the slots-per-restore-point value to stay at the previous default if the config
        // uses the new default. Don't error on a failed read because the config itself may need
        // migrating.
        // 允许slots-per-restore-point保持在之前的默认值，如果config使用以新的默认值，不要返回error，在
        // failed state，因为config自身需要migrating
        if let Ok(Some(disk_config)) = db.load_config() {
            if !db.config.slots_per_restore_point_set_explicitly
                && disk_config.slots_per_restore_point == PREV_DEFAULT_SLOTS_PER_RESTORE_POINT
                && db.config.slots_per_restore_point == DEFAULT_SLOTS_PER_RESTORE_POINT
            {
                debug!(
                    db.log,
                    "Ignoring slots-per-restore-point config in favour of on-disk value";
                    "config" => db.config.slots_per_restore_point,
                    "on_disk" => disk_config.slots_per_restore_point,
                );

                // Mutate the in-memory config so that it's compatible.
                // 修改内存中的配置，这样就能兼容
                db.config.slots_per_restore_point = PREV_DEFAULT_SLOTS_PER_RESTORE_POINT;
            }
        }

        // Load the previous split slot from the database (if any). This ensures we can
        // stop and restart correctly. This needs to occur *before* running any migrations
        // because some migrations load states and depend on the split.
        // 从数据库中加载split slot（如果有的话），这确保我们能正确停止和重启，这需要在任何migrations
        // 之前发生，因为migrations加载state依赖split
        if let Some(split) = db.load_split()? {
            *db.split.write() = split;
            *db.anchor_info.write() = db.load_anchor_info()?;

            info!(
                db.log,
                "Hot-Cold DB initialized";
                "split_slot" => split.slot,
                "split_state" => ?split.state_root
            );
        }

        // Ensure that the schema version of the on-disk database matches the software.
        // 确保磁盘数据库中的schema version和软件匹配
        // If the version is mismatched, an automatic migration will be attempted.
        // 如果version不匹配，则会尝试一个自动的migration
        let db = Arc::new(db);
        if let Some(schema_version) = db.load_schema_version()? {
            debug!(
                db.log,
                "Attempting schema migration";
                "from_version" => schema_version.as_u64(),
                "to_version" => CURRENT_SCHEMA_VERSION.as_u64(),
            );
            migrate_schema(db.clone(), schema_version, CURRENT_SCHEMA_VERSION)?;
        } else {
            db.store_schema_version(CURRENT_SCHEMA_VERSION)?;
        }

        // Ensure that any on-disk config is compatible with the supplied config.
        // 确保任何磁盘中的配置和提供的配置匹配
        if let Some(disk_config) = db.load_config()? {
            db.config.check_compatibility(&disk_config)?;
        }
        db.store_config()?;

        // Run a garbage collection pass.
        // 运行gc
        db.remove_garbage()?;

        // If configured, run a foreground compaction pass.
        // 如果配置了，运行一个前台的compaction pass
        if db.config.compact_on_init {
            info!(db.log, "Running foreground compaction");
            db.compact()?;
            info!(db.log, "Foreground compaction complete");
        }

        Ok(db)
    }

    /// Return an iterator over the state roots of all temporary states.
    /// 返回一个iterator，遍历temporary states中的所有state roots
    pub fn iter_temporary_state_roots(&self) -> impl Iterator<Item = Result<Hash256, Error>> + '_ {
        let column = DBColumn::BeaconStateTemporary;
        let start_key =
            BytesKey::from_vec(get_key_for_col(column.into(), Hash256::zero().as_bytes()));

        let keys_iter = self.hot_db.keys_iter();
        keys_iter.seek(&start_key);

        keys_iter
            .take_while(move |key| key.matches_column(column))
            .map(move |bytes_key| {
                bytes_key.remove_column(column).ok_or_else(|| {
                    HotColdDBError::IterationError {
                        unexpected_key: bytes_key,
                    }
                    .into()
                })
            })
    }
}

impl<E: EthSpec, Hot: ItemStore<E>, Cold: ItemStore<E>> HotColdDB<E, Hot, Cold> {
    /// Store a block and update the LRU cache.
    /// 存储一个block并且更新LRU cache
    pub fn put_block(
        &self,
        block_root: &Hash256,
        block: SignedBeaconBlock<E>,
    ) -> Result<(), Error> {
        // Store on disk.
        // 存储到磁盘
        let mut ops = Vec::with_capacity(2);
        let block = self.block_as_kv_store_ops(block_root, block, &mut ops)?;
        self.hot_db.do_atomically(ops)?;
        // Update cache.
        // 更新cache
        self.block_cache.lock().put(*block_root, block);
        Ok(())
    }

    /// Prepare a signed beacon block for storage in the database.
    /// 准备一个signed beacon block，存储到db中
    ///
    /// Return the original block for re-use after storage. It's passed by value so it can be
    /// cracked open and have its payload extracted.
    /// 返回原始的block在存储之后重复使用，它通过值传递，这样它可以裂开并且有它的payload被抽取
    pub fn block_as_kv_store_ops(
        &self,
        key: &Hash256,
        block: SignedBeaconBlock<E>,
        ops: &mut Vec<KeyValueStoreOp>,
    ) -> Result<SignedBeaconBlock<E>, Error> {
        // Split block into blinded block and execution payload.
        // 将block分裂为blinded block和execution payload
        let (blinded_block, payload) = block.into();

        // Store blinded block.
        // 存储blinded block
        self.blinded_block_as_kv_store_ops(key, &blinded_block, ops);

        // Store execution payload if present.
        // 存储execution payload，如果存在的话
        if let Some(ref execution_payload) = payload {
            ops.push(execution_payload.as_kv_store_op(*key));
        }

        // Re-construct block. This should always succeed.
        // 重新构建block，它总是应该成功
        blinded_block
            .try_into_full_block(payload)
            .ok_or(Error::AddPayloadLogicError)
    }

    /// Prepare a signed beacon block for storage in the datbase *without* its payload.
    pub fn blinded_block_as_kv_store_ops(
        &self,
        key: &Hash256,
        blinded_block: &SignedBeaconBlock<E, BlindedPayload<E>>,
        ops: &mut Vec<KeyValueStoreOp>,
    ) {
        let db_key = get_key_for_col(DBColumn::BeaconBlock.into(), key.as_bytes());
        ops.push(KeyValueStoreOp::PutKeyValue(
            db_key,
            blinded_block.as_ssz_bytes(),
        ));
    }

    // 试着获取完整的block
    pub fn try_get_full_block(
        &self,
        block_root: &Hash256,
    ) -> Result<Option<DatabaseBlock<E>>, Error> {
        metrics::inc_counter(&metrics::BEACON_BLOCK_GET_COUNT);

        // Check the cache.
        // 首先检查cache
        if let Some(block) = self.block_cache.lock().get(block_root) {
            metrics::inc_counter(&metrics::BEACON_BLOCK_CACHE_HIT_COUNT);
            return Ok(Some(DatabaseBlock::Full(block.clone())));
        }

        // Load the blinded block.
        // 加载blinded block
        let blinded_block = match self.get_blinded_block(block_root)? {
            Some(block) => block,
            None => return Ok(None),
        };

        // If the block is after the split point then we should have the full execution payload
        // stored in the database. If it isn't but payload pruning is disabled, try to load it
        // on-demand.
        // 如果block在split point之后，那么我们应该有完整的execution payload存储在db中，如果不是，但是payload pruning被关闭
        // 试着按需加载
        //
        // Hold the split lock so that it can't change while loading the payload.
        // 持有split lock，这样它在加载payload的时候不会改变
        let split = self.split.read_recursive();

        let block = if blinded_block.message().execution_payload().is_err()
            || blinded_block.slot() >= split.slot
        {
            // Re-constructing the full block should always succeed here.
            let full_block = self.make_full_block(block_root, blinded_block)?;

            // Add to cache.
            self.block_cache.lock().put(*block_root, full_block.clone());

            DatabaseBlock::Full(full_block)
        } else if !self.config.prune_payloads {
            // If payload pruning is disabled there's a chance we may have the payload of
            // this finalized block. Attempt to load it but don't error in case it's missing.
            let fork_name = blinded_block.fork_name(&self.spec)?;
            if let Some(payload) = self.get_execution_payload(block_root, fork_name)? {
                DatabaseBlock::Full(
                    blinded_block
                        .try_into_full_block(Some(payload))
                        .ok_or(Error::AddPayloadLogicError)?,
                )
            } else {
                DatabaseBlock::Blinded(blinded_block)
            }
        } else {
            DatabaseBlock::Blinded(blinded_block)
        };
        drop(split);

        Ok(Some(block))
    }

    /// Fetch a full block with execution payload from the store.
    /// 从store中获取一个full block，有着execution payload
    pub fn get_full_block(
        &self,
        block_root: &Hash256,
    ) -> Result<Option<SignedBeaconBlock<E>>, Error> {
        match self.try_get_full_block(block_root)? {
            Some(DatabaseBlock::Full(block)) => Ok(Some(block)),
            Some(DatabaseBlock::Blinded(block)) => Err(
                HotColdDBError::MissingFullBlockExecutionPayloadPruned(*block_root, block.slot())
                    .into(),
            ),
            None => Ok(None),
        }
    }

    /// Convert a blinded block into a full block by loading its execution payload if necessary.
    /// 转换一个blinded block为一个full block，通过加载它的execution payload，如果需要的话
    pub fn make_full_block(
        &self,
        block_root: &Hash256,
        blinded_block: SignedBeaconBlock<E, BlindedPayload<E>>,
    ) -> Result<SignedBeaconBlock<E>, Error> {
        if blinded_block.message().execution_payload().is_ok() {
            let fork_name = blinded_block.fork_name(&self.spec)?;
            let execution_payload = self
                .get_execution_payload(block_root, fork_name)?
                .ok_or(HotColdDBError::MissingExecutionPayload(*block_root))?;
            blinded_block.try_into_full_block(Some(execution_payload))
        } else {
            blinded_block.try_into_full_block(None)
        }
        .ok_or(Error::AddPayloadLogicError)
    }

    pub fn get_blinded_block(
        &self,
        block_root: &Hash256,
    ) -> Result<Option<SignedBeaconBlock<E, BlindedPayload<E>>>, Error> {
        self.get_block_with(block_root, |bytes| {
            SignedBeaconBlock::from_ssz_bytes(bytes, &self.spec)
        })
    }

    /// Fetch a block from the store, ignoring which fork variant it *should* be for.
    /// 从store中获取一个block，不管它应该是哪个fork variant
    pub fn get_block_any_variant<Payload: AbstractExecPayload<E>>(
        &self,
        block_root: &Hash256,
    ) -> Result<Option<SignedBeaconBlock<E, Payload>>, Error> {
        self.get_block_with(block_root, SignedBeaconBlock::any_from_ssz_bytes)
    }

    /// Fetch a block from the store using a custom decode function.
    /// 从store获取一个block，使用自定义的decode函数
    ///
    /// This is useful for e.g. ignoring the slot-indicated fork to forcefully load a block as if it
    /// were for a different fork.
    /// 这是有用的，比如对于忽略slot-indicated fork来强制记载一个block，仿佛它来自不同的fork
    pub fn get_block_with<Payload: AbstractExecPayload<E>>(
        &self,
        block_root: &Hash256,
        decoder: impl FnOnce(&[u8]) -> Result<SignedBeaconBlock<E, Payload>, ssz::DecodeError>,
    ) -> Result<Option<SignedBeaconBlock<E, Payload>>, Error> {
        self.hot_db
            .get_bytes(DBColumn::BeaconBlock.into(), block_root.as_bytes())?
            .map(|block_bytes| decoder(&block_bytes))
            .transpose()
            .map_err(|e| e.into())
    }

    /// Load the execution payload for a block from disk.
    /// This method deserializes with the proper fork.
    pub fn get_execution_payload(
        &self,
        block_root: &Hash256,
        fork_name: ForkName,
    ) -> Result<Option<ExecutionPayload<E>>, Error> {
        let column = ExecutionPayload::<E>::db_column().into();
        let key = block_root.as_bytes();

        match self.hot_db.get_bytes(column, key)? {
            Some(bytes) => Ok(Some(ExecutionPayload::from_ssz_bytes(&bytes, fork_name)?)),
            None => Ok(None),
        }
    }

    /// Load the execution payload for a block from disk.
    /// DANGEROUS: this method just guesses the fork.
    pub fn get_execution_payload_dangerous_fork_agnostic(
        &self,
        block_root: &Hash256,
    ) -> Result<Option<ExecutionPayload<E>>, Error> {
        self.get_item(block_root)
    }

    /// Check if the execution payload for a block exists on disk.
    pub fn execution_payload_exists(&self, block_root: &Hash256) -> Result<bool, Error> {
        self.get_item::<ExecutionPayload<E>>(block_root)
            .map(|payload| payload.is_some())
    }

    /// Determine whether a block exists in the database.
    /// 确定block是否在数据库中存在
    pub fn block_exists(&self, block_root: &Hash256) -> Result<bool, Error> {
        self.hot_db
            .key_exists(DBColumn::BeaconBlock.into(), block_root.as_bytes())
    }

    /// Delete a block from the store and the block cache.
    /// 删除一个block，从store以及block cache
    pub fn delete_block(&self, block_root: &Hash256) -> Result<(), Error> {
        self.block_cache.lock().pop(block_root);
        self.hot_db
            .key_delete(DBColumn::BeaconBlock.into(), block_root.as_bytes())?;
        self.hot_db
            .key_delete(DBColumn::ExecPayload.into(), block_root.as_bytes())
    }

    pub fn put_state_summary(
        &self,
        state_root: &Hash256,
        summary: HotStateSummary,
    ) -> Result<(), Error> {
        self.hot_db.put(state_root, &summary).map_err(Into::into)
    }

    /// Store a state in the store.
    /// 将一个state放到store中
    pub fn put_state(&self, state_root: &Hash256, state: &BeaconState<E>) -> Result<(), Error> {
        let mut ops: Vec<KeyValueStoreOp> = Vec::new();
        if state.slot() < self.get_split_slot() {
            // 存储在cold state中
            self.store_cold_state(state_root, state, &mut ops)?;
            self.cold_db.do_atomically(ops)
        } else {
            // 存储在hot state中
            self.store_hot_state(state_root, state, &mut ops)?;
            self.hot_db.do_atomically(ops)
        }
    }

    /// Fetch a state from the store.
    /// 从store中获取一个state
    ///
    /// If `slot` is provided then it will be used as a hint as to which database should
    /// be checked. Importantly, if the slot hint is provided and indicates a slot that lies
    /// in the freezer database, then only the freezer database will be accessed and `Ok(None)`
    /// will be returned if the provided `state_root` doesn't match the state root of the
    /// frozen state at `slot`. Consequently, if a state from a non-canonical chain is desired, it's
    /// best to set `slot` to `None`, or call `load_hot_state` directly.
    /// 如果提供了`slot`，那它会被作为一个hint，用来查找db，重要的是，提供了slot hint并且表明slot存在于
    /// freezer database，那么只有freezer database会被访问并且`Ok(None)`被返回，如果提供的`state_root`
    /// 不匹配在`slot`存储的frozen state的state root，如果需要的是一个non-canonical chain的state
    /// 它最好设置`slot`为`None`，或者直接调用`load_hot_state`
    pub fn get_state(
        &self,
        state_root: &Hash256,
        slot: Option<Slot>,
    ) -> Result<Option<BeaconState<E>>, Error> {
        metrics::inc_counter(&metrics::BEACON_STATE_GET_COUNT);

        if let Some(slot) = slot {
            if slot < self.get_split_slot() {
                // Although we could avoid a DB lookup by shooting straight for the
                // frozen state using `load_cold_state_by_slot`, that would be incorrect
                // in the case where the caller provides a `state_root` that's off the canonical
                // chain. This way we avoid returning a state that doesn't match `state_root`.
                // 尽管我们可以避免一次DB查询，通过使用`load_cold_state_by_slot`查找frozen state
                // 这会不正确，万一caller提供一个`state_root`，不在canonical chain，这种方法我们可以避免
                // 返回一个state，不匹配`state_root`
                self.load_cold_state(state_root)
            } else {
                self.load_hot_state(state_root, StateRootStrategy::Accurate)
            }
        } else {
            // 如果没有指定slot
            // 首先加载hot state
            match self.load_hot_state(state_root, StateRootStrategy::Accurate)? {
                Some(state) => Ok(Some(state)),
                // hot state不存在的话，加载cold state
                None => self.load_cold_state(state_root),
            }
        }
    }

    /// Fetch a state from the store, but don't compute all of the values when replaying blocks
    /// upon that state (e.g., state roots). Additionally, only states from the hot store are
    /// returned.
    /// 从store中获取一个state，不要计算所有的值，当重放blocks到state（例如，state root），另外，只有
    /// 来自hot store的states才被返回
    ///
    /// See `Self::get_state` for information about `slot`.
    /// 查看`Self::get_state`关于`slot`的信息
    ///
    /// ## Warning
    ///
    /// The returned state **is not a valid beacon state**, it can only be used for obtaining
    /// shuffling to process attestations. At least the following components of the state will be
    /// broken/invalid:
    /// 返回的state ***不是一个合法的beacon state*，它只能用于获取shuffling来处理attestations，至少state
    /// 的如下组件会是broken/非法的
    ///
    /// - `state.state_roots`
    /// - `state.block_roots`
    pub fn get_inconsistent_state_for_attestation_verification_only(
        &self,
        state_root: &Hash256,
        slot: Option<Slot>,
    ) -> Result<Option<BeaconState<E>>, Error> {
        metrics::inc_counter(&metrics::BEACON_STATE_GET_COUNT);

        let split_slot = self.get_split_slot();

        if slot.map_or(false, |slot| slot < split_slot) {
            Err(HotColdDBError::AttestationStateIsFinalized {
                split_slot,
                request_slot: slot,
                state_root: *state_root,
            }
            .into())
        } else {
            self.load_hot_state(state_root, StateRootStrategy::Inconsistent)
        }
    }

    /// Delete a state, ensuring it is removed from the LRU cache, as well as from on-disk.
    /// 删除一个state，确保它从LRU cache中移除，以及从on-disk
    ///
    /// It is assumed that all states being deleted reside in the hot DB, even if their slot is less
    /// than the split point. You shouldn't delete states from the finalized portion of the chain
    /// (which are frozen, and won't be deleted), or valid descendents of the finalized checkpoint
    /// (which will be deleted by this function but shouldn't be).
    /// 它假设所有被删除的states存在于hot DB中，即使它们的slot比split point更小，我们不能从chain的finalized部分
    /// 删除states（它们是frozen的，不能被删除），或者finalized checkpoint合法的descendants
    /// （会被这个函数删除，但是不应该）
    pub fn delete_state(&self, state_root: &Hash256, slot: Slot) -> Result<(), Error> {
        // Delete the state summary.
        // 删除state summary
        self.hot_db
            .key_delete(DBColumn::BeaconStateSummary.into(), state_root.as_bytes())?;

        // Delete the full state if it lies on an epoch boundary.
        // 删除full state，如果在epoch boundary
        if slot % E::slots_per_epoch() == 0 {
            self.hot_db
                .key_delete(DBColumn::BeaconState.into(), state_root.as_bytes())?;
        }

        Ok(())
    }

    pub fn forwards_block_roots_iterator(
        &self,
        start_slot: Slot,
        end_state: BeaconState<E>,
        end_block_root: Hash256,
        spec: &ChainSpec,
    ) -> Result<impl Iterator<Item = Result<(Hash256, Slot), Error>> + '_, Error> {
        HybridForwardsBlockRootsIterator::new(
            self,
            start_slot,
            None,
            || (end_state, end_block_root),
            spec,
        )
    }

    pub fn forwards_block_roots_iterator_until(
        &self,
        start_slot: Slot,
        end_slot: Slot,
        get_state: impl FnOnce() -> (BeaconState<E>, Hash256),
        spec: &ChainSpec,
    ) -> Result<HybridForwardsBlockRootsIterator<E, Hot, Cold>, Error> {
        HybridForwardsBlockRootsIterator::new(self, start_slot, Some(end_slot), get_state, spec)
    }

    pub fn forwards_state_roots_iterator(
        &self,
        start_slot: Slot,
        end_state_root: Hash256,
        end_state: BeaconState<E>,
        spec: &ChainSpec,
    ) -> Result<impl Iterator<Item = Result<(Hash256, Slot), Error>> + '_, Error> {
        HybridForwardsStateRootsIterator::new(
            self,
            start_slot,
            None,
            || (end_state, end_state_root),
            spec,
        )
    }

    pub fn forwards_state_roots_iterator_until(
        &self,
        start_slot: Slot,
        end_slot: Slot,
        get_state: impl FnOnce() -> (BeaconState<E>, Hash256),
        spec: &ChainSpec,
    ) -> Result<HybridForwardsStateRootsIterator<E, Hot, Cold>, Error> {
        // 构建一个state root iterator
        HybridForwardsStateRootsIterator::new(self, start_slot, Some(end_slot), get_state, spec)
    }

    /// Load an epoch boundary state by using the hot state summary look-up.
    /// 加载一个epoch boundary state，通过使用hot state summary look-up
    ///
    /// Will fall back to the cold DB if a hot state summary is not found.
    /// 会回到cold DB，如果没有找到一个hot state summary
    pub fn load_epoch_boundary_state(
        &self,
        state_root: &Hash256,
    ) -> Result<Option<BeaconState<E>>, Error> {
        if let Some(HotStateSummary {
            epoch_boundary_state_root,
            ..
        }) = self.load_hot_state_summary(state_root)?
        {
            // NOTE: minor inefficiency here because we load an unnecessary hot state summary
            // 注意：这里有点inefficiency，因为我们加载了一个非必须的hot state summary
            //
            // `StateRootStrategy` should be irrelevant here since we never replay blocks for an epoch
            // boundary state in the hot DB.
            // `StateRootStrategy`应该是无关的，因为我们从不需要replay blocks，对于在hot DB中的一个epoch
            // boundary state
            let state = self
                .load_hot_state(&epoch_boundary_state_root, StateRootStrategy::Accurate)?
                .ok_or(HotColdDBError::MissingEpochBoundaryState(
                    epoch_boundary_state_root,
                ))?;
            Ok(Some(state))
        } else {
            // Try the cold DB
            // 试着从cold DB加载
            match self.load_cold_state_slot(state_root)? {
                Some(state_slot) => {
                    let epoch_boundary_slot =
                        state_slot / E::slots_per_epoch() * E::slots_per_epoch();
                    self.load_cold_state_by_slot(epoch_boundary_slot)
                }
                None => Ok(None),
            }
        }
    }

    pub fn put_item<I: StoreItem>(&self, key: &Hash256, item: &I) -> Result<(), Error> {
        self.hot_db.put(key, item)
    }

    pub fn get_item<I: StoreItem>(&self, key: &Hash256) -> Result<Option<I>, Error> {
        self.hot_db.get(key)
    }

    pub fn item_exists<I: StoreItem>(&self, key: &Hash256) -> Result<bool, Error> {
        self.hot_db.exists::<I>(key)
    }

    /// Convert a batch of `StoreOp` to a batch of `KeyValueStoreOp`.
    /// 转换一批的`StoreOp`到一批的`KeyValueStoreOp`
    pub fn convert_to_kv_batch(
        &self,
        batch: Vec<StoreOp<E>>,
    ) -> Result<Vec<KeyValueStoreOp>, Error> {
        let mut key_value_batch = Vec::with_capacity(batch.len());
        for op in batch {
            match op {
                StoreOp::PutBlock(block_root, block) => {
                    self.block_as_kv_store_ops(
                        &block_root,
                        block.as_ref().clone(),
                        &mut key_value_batch,
                    )?;
                }

                StoreOp::PutState(state_root, state) => {
                    // 放入state
                    self.store_hot_state(&state_root, state, &mut key_value_batch)?;
                }

                StoreOp::PutStateSummary(state_root, summary) => {
                    // 放入state summary
                    key_value_batch.push(summary.as_kv_store_op(state_root));
                }

                StoreOp::PutStateTemporaryFlag(state_root) => {
                    key_value_batch.push(TemporaryFlag.as_kv_store_op(state_root));
                }

                StoreOp::DeleteStateTemporaryFlag(state_root) => {
                    let db_key =
                        get_key_for_col(TemporaryFlag::db_column().into(), state_root.as_bytes());
                    key_value_batch.push(KeyValueStoreOp::DeleteKey(db_key));
                }

                StoreOp::DeleteBlock(block_root) => {
                    let key = get_key_for_col(DBColumn::BeaconBlock.into(), block_root.as_bytes());
                    key_value_batch.push(KeyValueStoreOp::DeleteKey(key));
                }

                StoreOp::DeleteState(state_root, slot) => {
                    let state_summary_key =
                        get_key_for_col(DBColumn::BeaconStateSummary.into(), state_root.as_bytes());
                    key_value_batch.push(KeyValueStoreOp::DeleteKey(state_summary_key));

                    if slot.map_or(true, |slot| slot % E::slots_per_epoch() == 0) {
                        let state_key =
                            get_key_for_col(DBColumn::BeaconState.into(), state_root.as_bytes());
                        key_value_batch.push(KeyValueStoreOp::DeleteKey(state_key));
                    }
                }

                StoreOp::DeleteExecutionPayload(block_root) => {
                    let key = get_key_for_col(DBColumn::ExecPayload.into(), block_root.as_bytes());
                    key_value_batch.push(KeyValueStoreOp::DeleteKey(key));
                }

                StoreOp::KeyValueOp(kv_op) => {
                    key_value_batch.push(kv_op);
                }
            }
        }
        Ok(key_value_batch)
    }

    pub fn do_atomically(&self, batch: Vec<StoreOp<E>>) -> Result<(), Error> {
        // Update the block cache whilst holding a lock, to ensure that the cache updates atomically
        // with the database.
        // 更新block cache，同时维护一个锁，来确保cache在数据库中原子更新
        let mut guard = self.block_cache.lock();

        for op in &batch {
            match op {
                StoreOp::PutBlock(block_root, block) => {
                    guard.put(*block_root, (**block).clone());
                }

                StoreOp::PutState(_, _) => (),

                StoreOp::PutStateSummary(_, _) => (),

                StoreOp::PutStateTemporaryFlag(_) => (),

                StoreOp::DeleteStateTemporaryFlag(_) => (),

                StoreOp::DeleteBlock(block_root) => {
                    guard.pop(block_root);
                }

                StoreOp::DeleteState(_, _) => (),

                StoreOp::DeleteExecutionPayload(_) => (),

                StoreOp::KeyValueOp(_) => (),
            }
        }

        self.hot_db
            .do_atomically(self.convert_to_kv_batch(batch)?)?;
        drop(guard);

        Ok(())
    }

    /// Store a post-finalization state efficiently in the hot database.
    /// 高效地存储post-finalization到hot database
    ///
    /// On an epoch boundary, store a full state. On an intermediate slot, store
    /// just a backpointer to the nearest epoch boundary.
    /// 在每个epoch boundary，存储full state，对一个中间的slot，存储一个backpointer
    /// 到最近的epoch boudary
    pub fn store_hot_state(
        &self,
        state_root: &Hash256,
        state: &BeaconState<E>,
        ops: &mut Vec<KeyValueStoreOp>,
    ) -> Result<(), Error> {
        // On the epoch boundary, store the full state.
        // 在epoch boudary，存储full state
        if state.slot() % E::slots_per_epoch() == 0 {
            trace!(
                self.log,
                // 在epoch boundary，存放完整的full state
                "Storing full state on epoch boundary";
                "slot" => state.slot().as_u64(),
                "state_root" => format!("{:?}", state_root)
            );
            // 存储完整的state
            store_full_state(state_root, state, ops)?;
        }

        // Store a summary of the state.
        // We store one even for the epoch boundary states, as we may need their slots
        // when doing a look up by state root.
        // 存储state的summary，我们甚至存储一个epoch boundary state，因为我们需要它们的slots
        // 当我们用state root进行查找的时候
        let hot_state_summary = HotStateSummary::new(state_root, state)?;
        // 将state root作为key传入
        let op = hot_state_summary.as_kv_store_op(*state_root);
        // 推入到ops
        ops.push(op);

        Ok(())
    }

    /// Load a post-finalization state from the hot database.
    /// 从hot db中加载一个post-finalization state
    ///
    /// Will replay blocks from the nearest epoch boundary.
    /// 会从最近的epoch boudary进行重放
    pub fn load_hot_state(
        &self,
        state_root: &Hash256,
        state_root_strategy: StateRootStrategy,
    ) -> Result<Option<BeaconState<E>>, Error> {
        metrics::inc_counter(&metrics::BEACON_STATE_HOT_GET_COUNT);

        // If the state is marked as temporary, do not return it. It will become visible
        // only once its transaction commits and deletes its temporary flag.
        // 如果state被标记为temporary，不要返回它，它会变为visible，只有它的transaction commits并且
        // 删除它的temporary flag
        if self.load_state_temporary_flag(state_root)?.is_some() {
            return Ok(None);
        }

        if let Some(HotStateSummary {
            slot,
            latest_block_root,
            epoch_boundary_state_root,
        }) = self.load_hot_state_summary(state_root)?
        {
            let boundary_state =
                get_full_state(&self.hot_db, &epoch_boundary_state_root, &self.spec)?.ok_or(
                    HotColdDBError::MissingEpochBoundaryState(epoch_boundary_state_root),
                )?;

            // Optimization to avoid even *thinking* about replaying blocks if we're already
            // on an epoch boundary.
            // 优化，避免了甚至需要思考replaying blocks，如果我们在epoch boundary的话
            let state = if slot % E::slots_per_epoch() == 0 {
                boundary_state
            } else {
                // 加载需要replay的blocks
                let blocks =
                    self.load_blocks_to_replay(boundary_state.slot(), slot, latest_block_root)?;
                // 重放blocks
                self.replay_blocks(
                    boundary_state,
                    blocks,
                    slot,
                    no_state_root_iter(),
                    state_root_strategy,
                )?
            };

            // 返回state
            Ok(Some(state))
        } else {
            // 如果hot state summary都没有，则直接返回None
            Ok(None)
        }
    }

    /// Store a pre-finalization state in the freezer database.
    /// 在freezer database存储在一个pre-finalization state中
    ///
    /// If the state doesn't lie on a restore point boundary then just its summary will be stored.
    /// 如果state不在一个restore point boundary，那么只有它的summary被存储
    pub fn store_cold_state(
        &self,
        state_root: &Hash256,
        state: &BeaconState<E>,
        ops: &mut Vec<KeyValueStoreOp>,
    ) -> Result<(), Error> {
        // 默认都先存储ColdStateSummary
        ops.push(ColdStateSummary { slot: state.slot() }.as_kv_store_op(*state_root));

        if state.slot() % self.config.slots_per_restore_point != 0 {
            // 不是在restore point，则返回
            return Ok(());
        }

        trace!(
            self.log,
            "Creating restore point";
            "slot" => state.slot(),
            "state_root" => format!("{:?}", state_root)
        );

        // 1. Convert to PartialBeaconState and store that in the DB.
        // 2. 转换为PartialBeaconState并且保存在DB中
        let partial_state = PartialBeaconState::from_state_forgetful(state);
        let op = partial_state.as_kv_store_op(*state_root);
        ops.push(op);

        // 2. Store updated vector entries.
        // 2. 存储更新的vector entries
        let db = &self.cold_db;
        store_updated_vector(BlockRoots, db, state, &self.spec, ops)?;
        store_updated_vector(StateRoots, db, state, &self.spec, ops)?;
        store_updated_vector(HistoricalRoots, db, state, &self.spec, ops)?;
        store_updated_vector(RandaoMixes, db, state, &self.spec, ops)?;
        store_updated_vector(HistoricalSummaries, db, state, &self.spec, ops)?;

        // 3. Store restore point.
        // 3. 存储restore point
        let restore_point_index = state.slot().as_u64() / self.config.slots_per_restore_point;
        self.store_restore_point_hash(restore_point_index, *state_root, ops);

        Ok(())
    }

    /// Try to load a pre-finalization state from the freezer database.
    /// 试着从freezer database中加载一个pre-finalization state
    ///
    /// Return `None` if no state with `state_root` lies in the freezer.
    /// 返回`None`，如果没有`state_root`对应的state存在于freezer中
    pub fn load_cold_state(&self, state_root: &Hash256) -> Result<Option<BeaconState<E>>, Error> {
        match self.load_cold_state_slot(state_root)? {
            Some(slot) => self.load_cold_state_by_slot(slot),
            None => Ok(None),
        }
    }

    /// Load a pre-finalization state from the freezer database.
    /// 加载一个pre-finalization state，从freezer database
    ///
    /// Will reconstruct the state if it lies between restore points.
    /// 会重构state，如果它在restore points之间
    pub fn load_cold_state_by_slot(&self, slot: Slot) -> Result<Option<BeaconState<E>>, Error> {
        // Guard against fetching states that do not exist due to gaps in the historic state
        // database, which can occur due to checkpoint sync or re-indexing.
        // See the comments in `get_historic_state_limits` for more information.
        // 防范获取因为historical state database的gaps而不存在的states
        // 可能因为checkpoint sync或者re-indexing
        let (lower_limit, upper_limit) = self.get_historic_state_limits();

        if slot <= lower_limit || slot >= upper_limit {
            if slot % self.config.slots_per_restore_point == 0 {
                // 在restore point边界上
                let restore_point_idx = slot.as_u64() / self.config.slots_per_restore_point;
                // 通过restore_point_idx加载restore point
                self.load_restore_point_by_index(restore_point_idx)
            } else {
                // 加载cold中间state
                self.load_cold_intermediate_state(slot)
            }
            .map(Some)
        } else {
            Ok(None)
        }
    }

    /// Load a restore point state by its `state_root`.
    /// 通过`state_root`加载一个restore point state
    fn load_restore_point(&self, state_root: &Hash256) -> Result<BeaconState<E>, Error> {
        let partial_state_bytes = self
            .cold_db
            .get_bytes(DBColumn::BeaconState.into(), state_root.as_bytes())?
            .ok_or(HotColdDBError::MissingRestorePoint(*state_root))?;
        let mut partial_state: PartialBeaconState<E> =
            PartialBeaconState::from_ssz_bytes(&partial_state_bytes, &self.spec)?;

        // Fill in the fields of the partial state.
        // 加载部分状态的字段
        partial_state.load_block_roots(&self.cold_db, &self.spec)?;
        partial_state.load_state_roots(&self.cold_db, &self.spec)?;
        partial_state.load_historical_roots(&self.cold_db, &self.spec)?;
        partial_state.load_randao_mixes(&self.cold_db, &self.spec)?;
        partial_state.load_historical_summaries(&self.cold_db, &self.spec)?;

        partial_state.try_into()
    }

    /// Load a restore point state by its `restore_point_index`.
    /// 加载一个restore point state，通过它的`restore_point_index`
    fn load_restore_point_by_index(
        &self,
        restore_point_index: u64,
    ) -> Result<BeaconState<E>, Error> {
        // 加载restore point的state
        let state_root = self.load_restore_point_hash(restore_point_index)?;
        self.load_restore_point(&state_root)
    }

    /// Load a frozen state that lies between restore points.
    /// 加载restore points之间的frozen state
    fn load_cold_intermediate_state(&self, slot: Slot) -> Result<BeaconState<E>, Error> {
        // 1. Load the restore points either side of the intermediate state.
        // 1. 加载intermediate state两段的restore points
        let low_restore_point_idx = slot.as_u64() / self.config.slots_per_restore_point;
        let high_restore_point_idx = low_restore_point_idx + 1;

        // Acquire the read lock, so that the split can't change while this is happening.
        // 获取read lock，这样split就不能改变
        let split = self.split.read_recursive();

        let low_restore_point = self.load_restore_point_by_index(low_restore_point_idx)?;
        let high_restore_point = self.get_restore_point(high_restore_point_idx, &split)?;

        // 2. Load the blocks from the high restore point back to the low restore point.
        // 2. 加载blocks从high restore point到low restore point
        let blocks = self.load_blocks_to_replay(
            low_restore_point.slot(),
            slot,
            self.get_high_restore_point_block_root(&high_restore_point, slot)?,
        )?;

        // 3. Replay the blocks on top of the low restore point.
        // 3. 在low restore point之上重放blocks
        // Use a forwards state root iterator to avoid doing any tree hashing.
        // 使用forwards state root的迭代器来避免任何的tree hashing
        // The state root of the high restore point should never be used, so is safely set to 0.
        // high restore point的state root不应该被使用，因此可以安全地置为零
        let state_root_iter = self.forwards_state_roots_iterator_until(
            low_restore_point.slot(),
            slot,
            || (high_restore_point, Hash256::zero()),
            &self.spec,
        )?;

        // 重放blocks
        self.replay_blocks(
            low_restore_point,
            blocks,
            slot,
            Some(state_root_iter),
            StateRootStrategy::Accurate,
        )
    }

    /// Get the restore point with the given index, or if it is out of bounds, the split state.
    /// 获取给定index的restore point，或者如果它超出了范围，split state
    pub(crate) fn get_restore_point(
        &self,
        restore_point_idx: u64,
        split: &Split,
    ) -> Result<BeaconState<E>, Error> {
        if restore_point_idx * self.config.slots_per_restore_point >= split.slot.as_u64() {
            // 超过了split，则直接从get state加载split slot
            self.get_state(&split.state_root, Some(split.slot))?
                .ok_or(HotColdDBError::MissingSplitState(
                    split.state_root,
                    split.slot,
                ))
                .map_err(Into::into)
        } else {
            self.load_restore_point_by_index(restore_point_idx)
        }
    }

    /// Get a suitable block root for backtracking from `high_restore_point` to the state at `slot`.
    /// 获取一个可用的block root，用于从`high_restore_point`回溯到`slot`
    ///
    /// Defaults to the block root for `slot`, which *should* be in range.
    /// 默认block root对于`slot`，它不应该在range内
    fn get_high_restore_point_block_root(
        &self,
        high_restore_point: &BeaconState<E>,
        slot: Slot,
    ) -> Result<Hash256, HotColdDBError> {
        high_restore_point
            // 获取block root
            .get_block_root(slot)
            .or_else(|_| high_restore_point.get_oldest_block_root())
            .map(|x| *x)
            .map_err(HotColdDBError::RestorePointBlockHashError)
    }

    /// Load the blocks between `start_slot` and `end_slot` by backtracking from `end_block_hash`.
    /// 加载`start_slot`和`end_slot`之间的blocks，从`end_block_hash`开始backtracking
    ///
    /// Blocks are returned in slot-ascending order, suitable for replaying on a state with slot
    /// equal to `start_slot`, to reach a state with slot equal to `end_slot`.
    /// Blocks通过slot递增的顺序返回，适用于重放一个state，从`start_slot`到和`end_slot`相等
    pub fn load_blocks_to_replay(
        &self,
        start_slot: Slot,
        end_slot: Slot,
        end_block_hash: Hash256,
        // 返回一系列SignedBeaconBlock
    ) -> Result<Vec<SignedBeaconBlock<E, BlindedPayload<E>>>, Error> {
        let mut blocks = ParentRootBlockIterator::new(self, end_block_hash)
            .map(|result| result.map(|(_, block)| block))
            // Include the block at the end slot (if any), it needs to be
            // replayed in order to construct the canonical state at `end_slot`.
            // 包含在end slot的block（如果有的话），它需要按序重放，为了构建`end_slot`的canonical state
            .filter(|result| {
                result
                    .as_ref()
                    .map_or(true, |block| block.slot() <= end_slot)
            })
            // Include the block at the start slot (if any). Whilst it doesn't need to be
            // applied to the state, it contains a potentially useful state root.
            // 包含在start slot的block（如果有的话），同时它不需要应用到state，它包含一个可能有用的state root
            //
            // Return `true` on an `Err` so that the `collect` fails, unless the error is a
            // `BlockNotFound` error and some blocks are intentionally missing from the DB.
            // This complexity is unfortunately necessary to avoid loading the parent of the
            // oldest known block -- we can't know that we have all the required blocks until we
            // load a block with slot less than the start slot, which is impossible if there are
            // no blocks with slot less than the start slot.
            // 返回`true`在遇到一个`Err`，这样`collect`就会失败，除非error是`BlockNotFound`并且有的blocks是有意在DB中缺失的
            // 这个复杂度是不可避免的必要的，为了避免加载oldest known block的parent，我们没法知道我们有所有需要的blocks，直到
            // 我们加载一个block，它的slot小于start slot，这是不可能的，如果没有blocks有着比start slot更小的slot
            .take_while(|result| match result {
                // block slot大于等于start_slot的时候持续运行
                Ok(block) => block.slot() >= start_slot,
                Err(Error::BlockNotFound(_)) => {
                    self.get_oldest_block_slot() == self.spec.genesis_slot
                }
                Err(_) => true,
            })
            .collect::<Result<Vec<_>, _>>()?;
        blocks.reverse();
        Ok(blocks)
    }

    /// Replay `blocks` on top of `state` until `target_slot` is reached.
    /// 在`state`之上重放`blocks`直到到达`target_slot`
    ///
    /// Will skip slots as necessary. The returned state is not guaranteed
    /// to have any caches built, beyond those immediately required by block processing.
    /// 会按需跳过slots，返回的state不保证有任何的caches被构建，超出了块处理所需的范围
    fn replay_blocks(
        &self,
        state: BeaconState<E>,
        blocks: Vec<SignedBeaconBlock<E, BlindedPayload<E>>>,
        target_slot: Slot,
        state_root_iter: Option<impl Iterator<Item = Result<(Hash256, Slot), Error>>>,
        state_root_strategy: StateRootStrategy,
    ) -> Result<BeaconState<E>, Error> {
        let mut block_replayer = BlockReplayer::new(state, &self.spec)
            .state_root_strategy(state_root_strategy)
            .no_signature_verification()
            .minimal_block_root_verification();

        let have_state_root_iterator = state_root_iter.is_some();
        if let Some(state_root_iter) = state_root_iter {
            block_replayer = block_replayer.state_root_iter(state_root_iter);
        }

        block_replayer
            // 应用blocks
            .apply_blocks(blocks, Some(target_slot))
            .map(|block_replayer| {
                if have_state_root_iterator && block_replayer.state_root_miss() {
                    warn!(
                        self.log,
                        "State root iterator miss";
                        "slot" => target_slot,
                    );
                }

                block_replayer.into_state()
            })
    }

    /// Get a reference to the `ChainSpec` used by the database.
    pub fn get_chain_spec(&self) -> &ChainSpec {
        &self.spec
    }

    /// Get a reference to the `Logger` used by the database.
    pub fn logger(&self) -> &Logger {
        &self.log
    }

    /// Fetch a copy of the current split slot from memory.
    /// 从内存中获取当前的split slot的一个copy
    pub fn get_split_slot(&self) -> Slot {
        self.split.read_recursive().slot
    }

    /// Fetch a copy of the current split slot from memory.
    pub fn get_split_info(&self) -> Split {
        *self.split.read_recursive()
    }

    pub fn set_split(&self, slot: Slot, state_root: Hash256) {
        *self.split.write() = Split { slot, state_root };
    }

    /// Fetch the slot of the most recently stored restore point.
    pub fn get_latest_restore_point_slot(&self) -> Slot {
        (self.get_split_slot() - 1) / self.config.slots_per_restore_point
            * self.config.slots_per_restore_point
    }

    /// Load the database schema version from disk.
    fn load_schema_version(&self) -> Result<Option<SchemaVersion>, Error> {
        self.hot_db.get(&SCHEMA_VERSION_KEY)
    }

    /// Store the database schema version.
    pub fn store_schema_version(&self, schema_version: SchemaVersion) -> Result<(), Error> {
        self.hot_db.put(&SCHEMA_VERSION_KEY, &schema_version)
    }

    /// Store the database schema version atomically with additional operations.
    pub fn store_schema_version_atomically(
        &self,
        schema_version: SchemaVersion,
        mut ops: Vec<KeyValueStoreOp>,
    ) -> Result<(), Error> {
        let column = SchemaVersion::db_column().into();
        let key = SCHEMA_VERSION_KEY.as_bytes();
        let db_key = get_key_for_col(column, key);
        let op = KeyValueStoreOp::PutKeyValue(db_key, schema_version.as_store_bytes());
        ops.push(op);

        self.hot_db.do_atomically(ops)
    }

    /// Initialise the anchor info for checkpoint sync starting from `block`.
    pub fn init_anchor_info(&self, block: BeaconBlockRef<'_, E>) -> Result<KeyValueStoreOp, Error> {
        let anchor_slot = block.slot();
        let slots_per_restore_point = self.config.slots_per_restore_point;

        // Set the `state_upper_limit` to the slot of the *next* restore point.
        // See `get_state_upper_limit` for rationale.
        let next_restore_point_slot = if anchor_slot % slots_per_restore_point == 0 {
            anchor_slot
        } else {
            (anchor_slot / slots_per_restore_point + 1) * slots_per_restore_point
        };
        let anchor_info = AnchorInfo {
            anchor_slot,
            oldest_block_slot: anchor_slot,
            oldest_block_parent: block.parent_root(),
            state_upper_limit: next_restore_point_slot,
            state_lower_limit: self.spec.genesis_slot,
        };
        self.compare_and_set_anchor_info(None, Some(anchor_info))
    }

    /// Get a clone of the store's anchor info.
    /// 获取store的anchor信息的clone
    ///
    /// To do mutations, use `compare_and_set_anchor_info`.
    pub fn get_anchor_info(&self) -> Option<AnchorInfo> {
        self.anchor_info.read_recursive().clone()
    }

    /// Atomically update the anchor info from `prev_value` to `new_value`.
    /// 自动更新achor info，从`prev_value`到`new_value`
    ///
    /// Return a `KeyValueStoreOp` which should be written to disk, possibly atomically with other
    /// values.
    /// 返回一个`KeyValueStoreOp`，它应该被写入磁盘，原子地，和其他值
    ///
    /// Return an `AnchorInfoConcurrentMutation` error if the `prev_value` provided
    /// is not correct.
    pub fn compare_and_set_anchor_info(
        &self,
        prev_value: Option<AnchorInfo>,
        new_value: Option<AnchorInfo>,
    ) -> Result<KeyValueStoreOp, Error> {
        let mut anchor_info = self.anchor_info.write();
        if *anchor_info == prev_value {
            let kv_op = self.store_anchor_info_in_batch(&new_value);
            *anchor_info = new_value;
            Ok(kv_op)
        } else {
            Err(Error::AnchorInfoConcurrentMutation)
        }
    }

    /// As for `compare_and_set_anchor_info`, but also writes the anchor to disk immediately.
    /// 同时立即写入到磁盘
    pub fn compare_and_set_anchor_info_with_write(
        &self,
        prev_value: Option<AnchorInfo>,
        new_value: Option<AnchorInfo>,
    ) -> Result<(), Error> {
        let kv_store_op = self.compare_and_set_anchor_info(prev_value, new_value)?;
        // 写入磁盘
        self.hot_db.do_atomically(vec![kv_store_op])
    }

    /// Load the anchor info from disk, but do not set `self.anchor_info`.
    fn load_anchor_info(&self) -> Result<Option<AnchorInfo>, Error> {
        self.hot_db.get(&ANCHOR_INFO_KEY)
    }

    /// Store the given `anchor_info` to disk.
    ///
    /// The argument is intended to be `self.anchor_info`, but is passed manually to avoid issues
    /// with recursive locking.
    fn store_anchor_info_in_batch(&self, anchor_info: &Option<AnchorInfo>) -> KeyValueStoreOp {
        if let Some(ref anchor_info) = anchor_info {
            anchor_info.as_kv_store_op(ANCHOR_INFO_KEY)
        } else {
            KeyValueStoreOp::DeleteKey(get_key_for_col(
                DBColumn::BeaconMeta.into(),
                ANCHOR_INFO_KEY.as_bytes(),
            ))
        }
    }

    /// If an anchor exists, return its `anchor_slot` field.
    pub fn get_anchor_slot(&self) -> Option<Slot> {
        self.anchor_info
            .read_recursive()
            .as_ref()
            .map(|a| a.anchor_slot)
    }

    /// Return the slot-window describing the available historic states.
    /// 返回slot-window，用来描述可用的historical states
    ///
    /// Returns `(lower_limit, upper_limit)`.
    ///
    /// The lower limit is the maximum slot such that frozen states are available for all
    /// previous slots (<=).
    /// low limit是最大的slot，frozen states可用，对于所有之前的slots（<=）
    ///
    /// The upper limit is the minimum slot such that frozen states are available for all
    /// subsequent slots (>=).
    /// upper limit是最小的slot，frozen states可用，对于所有之后的states
    ///
    /// If `lower_limit >= upper_limit` then all states are available. This will be true
    /// if the database is completely filled in, as we'll return `(split_slot, 0)` in this
    /// instance.
    /// 如果`lower_limit >= upper_limit`，那么所有的states可用，这会为true，当database被完全填充
    /// 因为我们会返回`(split_slot, 0)`，针对这种情况
    pub fn get_historic_state_limits(&self) -> (Slot, Slot) {
        // If checkpoint sync is used then states in the hot DB will always be available, but may
        // become unavailable as finalisation advances due to the lack of a restore point in the
        // database. For this reason we take the minimum of the split slot and the
        // restore-point-aligned `state_upper_limit`, which should be set _ahead_ of the checkpoint
        // slot during initialisation.
        // 如果使用了checkpoint sync，那么hot DB中的states总是可用的，但是会不可用，随着finalisation前进
        // 因为db中缺少一个restore point，因为这个原因，我们拿最小的split slot以及restore-point对齐的
        // `state_upper_limit`，它应该在checkpoint slot之前，在初始化过程中
        //
        // E.g. if we start from a checkpoint at slot 2048+1024=3072 with SPRP=2048, then states
        // with slots 3072-4095 will be available only while they are in the hot database, and this
        // function will return the current split slot as the upper limit. Once slot 4096 is reached
        // a new restore point will be created at that slot, making all states from 4096 onwards
        // permanently available.
        let split_slot = self.get_split_slot();
        self.anchor_info
            .read_recursive()
            .as_ref()
            .map_or((split_slot, self.spec.genesis_slot), |a| {
                (a.state_lower_limit, min(a.state_upper_limit, split_slot))
            })
    }

    /// Return the minimum slot such that blocks are available for all subsequent slots.
    pub fn get_oldest_block_slot(&self) -> Slot {
        self.anchor_info
            .read_recursive()
            .as_ref()
            .map_or(self.spec.genesis_slot, |anchor| anchor.oldest_block_slot)
    }

    /// Return the in-memory configuration used by the database.
    pub fn get_config(&self) -> &StoreConfig {
        &self.config
    }

    /// Load previously-stored config from disk.
    fn load_config(&self) -> Result<Option<OnDiskStoreConfig>, Error> {
        self.hot_db.get(&CONFIG_KEY)
    }

    /// Write the config to disk.
    fn store_config(&self) -> Result<(), Error> {
        self.hot_db.put(&CONFIG_KEY, &self.config.as_disk_config())
    }

    /// Load the split point from disk.
    fn load_split(&self) -> Result<Option<Split>, Error> {
        self.hot_db.get(&SPLIT_KEY)
    }

    /// Stage the split for storage to disk.
    pub fn store_split_in_batch(&self) -> KeyValueStoreOp {
        self.split.read_recursive().as_kv_store_op(SPLIT_KEY)
    }

    /// Load the state root of a restore point.
    /// 加载一个restore point的state root
    fn load_restore_point_hash(&self, restore_point_index: u64) -> Result<Hash256, Error> {
        let key = Self::restore_point_key(restore_point_index);
        self.cold_db
            .get(&key)?
            .map(|r: RestorePointHash| r.state_root)
            .ok_or_else(|| HotColdDBError::MissingRestorePointHash(restore_point_index).into())
    }

    /// Store the state root of a restore point.
    /// 存储一个restore point的state root
    fn store_restore_point_hash(
        &self,
        restore_point_index: u64,
        state_root: Hash256,
        ops: &mut Vec<KeyValueStoreOp>,
    ) {
        let value = &RestorePointHash { state_root };
        let op = value.as_kv_store_op(Self::restore_point_key(restore_point_index));
        ops.push(op);
    }

    /// Convert a `restore_point_index` into a database key.
    fn restore_point_key(restore_point_index: u64) -> Hash256 {
        Hash256::from_low_u64_be(restore_point_index)
    }

    /// Load a frozen state's slot, given its root.
    /// 加载一个frozen state的slot，给定它的root
    pub fn load_cold_state_slot(&self, state_root: &Hash256) -> Result<Option<Slot>, Error> {
        Ok(self
            .cold_db
            .get(state_root)?
            // 获取到的类型为ColdStateSummary
            .map(|s: ColdStateSummary| s.slot))
    }

    /// Load a hot state's summary, given its root.
    /// 加载一个hot state的summary，给定它的root
    pub fn load_hot_state_summary(
        &self,
        state_root: &Hash256,
    ) -> Result<Option<HotStateSummary>, Error> {
        // 从hot db中获取state
        self.hot_db.get(state_root)
    }

    /// Load the temporary flag for a state root, if one exists.
    /// 对于一个state root加载临时的flag，如果它存在的话
    ///
    /// Returns `Some` if the state is temporary, or `None` if the state is permanent or does not
    /// exist -- you should call `load_hot_state_summary` to find out which.
    /// 返回`Some`如果state是临时的，或者`None`，如果state是永久的或者不存在 --- 你需要调用`load_hot_state_summary`
    /// 来发现是哪个
    pub fn load_state_temporary_flag(
        &self,
        state_root: &Hash256,
    ) -> Result<Option<TemporaryFlag>, Error> {
        self.hot_db.get(state_root)
    }

    /// Check that the restore point frequency is valid.
    ///
    /// Specifically, check that it is:
    /// (1) A divisor of the number of slots per historical root, and
    /// (2) Divisible by the number of slots per epoch
    ///
    ///
    /// (1) ensures that we have at least one restore point within range of our state
    /// root history when iterating backwards (and allows for more frequent restore points if
    /// desired).
    ///
    /// (2) ensures that restore points align with hot state summaries, making it
    /// quick to migrate hot to cold.
    fn verify_slots_per_restore_point(slots_per_restore_point: u64) -> Result<(), HotColdDBError> {
        let slots_per_historical_root = E::SlotsPerHistoricalRoot::to_u64();
        let slots_per_epoch = E::slots_per_epoch();
        if slots_per_restore_point > 0
            && slots_per_historical_root % slots_per_restore_point == 0
            && slots_per_restore_point % slots_per_epoch == 0
        {
            Ok(())
        } else {
            Err(HotColdDBError::InvalidSlotsPerRestorePoint {
                slots_per_restore_point,
                slots_per_historical_root,
                slots_per_epoch,
            })
        }
    }

    /// Run a compaction pass to free up space used by deleted states.
    /// 运行一个compaction pass来释放删除的states使用的空间
    pub fn compact(&self) -> Result<(), Error> {
        self.hot_db.compact()?;
        Ok(())
    }

    /// Return `true` if compaction on finalization/pruning is enabled.
    /// 返回`true`，如果在finalization/pruning的压缩已经使能了
    pub fn compact_on_prune(&self) -> bool {
        self.config.compact_on_prune
    }

    /// Load the checkpoint to begin pruning from (the "old finalized checkpoint").
    pub fn load_pruning_checkpoint(&self) -> Result<Option<Checkpoint>, Error> {
        Ok(self
            .hot_db
            .get(&PRUNING_CHECKPOINT_KEY)?
            .map(|pc: PruningCheckpoint| pc.checkpoint))
    }

    /// Store the checkpoint to begin pruning from (the "old finalized checkpoint").
    pub fn store_pruning_checkpoint(&self, checkpoint: Checkpoint) -> Result<(), Error> {
        self.hot_db
            .do_atomically(vec![self.pruning_checkpoint_store_op(checkpoint)])
    }

    /// Create a staged store for the pruning checkpoint.
    pub fn pruning_checkpoint_store_op(&self, checkpoint: Checkpoint) -> KeyValueStoreOp {
        PruningCheckpoint { checkpoint }.as_kv_store_op(PRUNING_CHECKPOINT_KEY)
    }

    /// Load the timestamp of the last compaction as a `Duration` since the UNIX epoch.
    pub fn load_compaction_timestamp(&self) -> Result<Option<Duration>, Error> {
        Ok(self
            .hot_db
            .get(&COMPACTION_TIMESTAMP_KEY)?
            .map(|c: CompactionTimestamp| Duration::from_secs(c.0)))
    }

    /// Store the timestamp of the last compaction as a `Duration` since the UNIX epoch.
    pub fn store_compaction_timestamp(&self, compaction_timestamp: Duration) -> Result<(), Error> {
        self.hot_db.put(
            &COMPACTION_TIMESTAMP_KEY,
            &CompactionTimestamp(compaction_timestamp.as_secs()),
        )
    }

    /// Try to prune all execution payloads, returning early if there is no need to prune.
    pub fn try_prune_execution_payloads(&self, force: bool) -> Result<(), Error> {
        let split = self.get_split_info();

        if split.slot == 0 {
            return Ok(());
        }

        let bellatrix_fork_slot = if let Some(epoch) = self.spec.bellatrix_fork_epoch {
            epoch.start_slot(E::slots_per_epoch())
        } else {
            return Ok(());
        };

        // Load the split state so we can backtrack to find execution payloads.
        let split_state = self.get_state(&split.state_root, Some(split.slot))?.ok_or(
            HotColdDBError::MissingSplitState(split.state_root, split.slot),
        )?;

        // The finalized block may or may not have its execution payload stored, depending on
        // whether it was at a skipped slot. However for a fully pruned database its parent
        // should *always* have been pruned. In case of a long split (no parent found) we
        // continue as if the payloads are pruned, as the node probably has other things to worry
        // about.
        let split_block_root = split_state.get_latest_block_root(split.state_root);

        let already_pruned =
            process_results(split_state.rev_iter_block_roots(&self.spec), |mut iter| {
                iter.find(|(_, block_root)| *block_root != split_block_root)
                    .map_or(Ok(true), |(_, split_parent_root)| {
                        self.execution_payload_exists(&split_parent_root)
                            .map(|exists| !exists)
                    })
            })??;

        if already_pruned && !force {
            info!(self.log, "Execution payloads are pruned");
            return Ok(());
        }

        // Iterate block roots backwards to the Bellatrix fork or the anchor slot, whichever comes
        // first.
        warn!(
            self.log,
            "Pruning finalized payloads";
            "info" => "you may notice degraded I/O performance while this runs"
        );
        let anchor_slot = self.get_anchor_info().map(|info| info.anchor_slot);

        let mut ops = vec![];
        let mut last_pruned_block_root = None;

        for res in std::iter::once(Ok((split_block_root, split.slot)))
            .chain(BlockRootsIterator::new(self, &split_state))
        {
            let (block_root, slot) = match res {
                Ok(tuple) => tuple,
                Err(e) => {
                    warn!(
                        self.log,
                        "Stopping payload pruning early";
                        "error" => ?e,
                    );
                    break;
                }
            };

            if slot < bellatrix_fork_slot {
                info!(
                    self.log,
                    "Payload pruning reached Bellatrix boundary";
                );
                break;
            }

            if Some(block_root) != last_pruned_block_root
                && self.execution_payload_exists(&block_root)?
            {
                debug!(
                    self.log,
                    "Pruning execution payload";
                    "slot" => slot,
                    "block_root" => ?block_root,
                );
                last_pruned_block_root = Some(block_root);
                ops.push(StoreOp::DeleteExecutionPayload(block_root));
            }

            if Some(slot) == anchor_slot {
                info!(
                    self.log,
                    "Payload pruning reached anchor state";
                    "slot" => slot
                );
                break;
            }
        }
        let payloads_pruned = ops.len();
        self.do_atomically(ops)?;
        info!(
            self.log,
            "Execution payload pruning complete";
            "payloads_pruned" => payloads_pruned,
        );
        Ok(())
    }
}

/// Advance the split point of the store, moving new finalized states to the freezer.
/// 提前store的split point，将新的finalized states移动到freezer
pub fn migrate_database<E: EthSpec, Hot: ItemStore<E>, Cold: ItemStore<E>>(
    store: Arc<HotColdDB<E, Hot, Cold>>,
    frozen_head_root: Hash256,
    frozen_head: &BeaconState<E>,
) -> Result<(), Error> {
    debug!(
        store.log,
        "Freezer migration started";
        "slot" => frozen_head.slot()
    );

    // 0. Check that the migration is sensible.
    // 0. 检查migration是有意义的
    // The new frozen head must increase the current split slot, and lie on an epoch
    // boundary (in order for the hot state summary scheme to work).
    // 新的frozen head必须增加当前的split slot，并且在epoch boundary（为了hot state summary scheme能work）
    let current_split_slot = store.split.read_recursive().slot;
    let anchor_slot = store
        .anchor_info
        .read_recursive()
        .as_ref()
        .map(|a| a.anchor_slot);

    if frozen_head.slot() < current_split_slot {
        return Err(HotColdDBError::FreezeSlotError {
            current_split_slot,
            proposed_split_slot: frozen_head.slot(),
        }
        .into());
    }

    if frozen_head.slot() % E::slots_per_epoch() != 0 {
        return Err(HotColdDBError::FreezeSlotUnaligned(frozen_head.slot()).into());
    }

    let mut hot_db_ops: Vec<StoreOp<E>> = Vec::new();

    // 1. Copy all of the states between the head and the split slot, from the hot DB
    // to the cold DB. Delete the execution payloads of these now-finalized blocks.
    // 1. 拷贝head以及split slot之间的states，从hot DB到cold DB，删除execution payloads，对于这些
    // finalized blocks
    let state_root_iter = RootsIterator::new(&store, frozen_head);
    for maybe_tuple in state_root_iter.take_while(|result| match result {
        Ok((_, _, slot)) => {
            slot >= &current_split_slot
                && anchor_slot.map_or(true, |anchor_slot| slot >= &anchor_slot)
        }
        Err(_) => true,
    }) {
        let (block_root, state_root, slot) = maybe_tuple?;

        let mut cold_db_ops: Vec<KeyValueStoreOp> = Vec::new();

        if slot % store.config.slots_per_restore_point == 0 {
            // 获取full states
            let state: BeaconState<E> = get_full_state(&store.hot_db, &state_root, &store.spec)?
                .ok_or(HotColdDBError::MissingStateToFreeze(state_root))?;

            store.store_cold_state(&state_root, &state, &mut cold_db_ops)?;
        }

        // Store a pointer from this state root to its slot, so we can later reconstruct states
        // from their state root alone.
        // 存储一个pointer，从state root到它的slot，这样我们后面可以重构states，从它们的state root
        let cold_state_summary = ColdStateSummary { slot };
        let op = cold_state_summary.as_kv_store_op(state_root);
        cold_db_ops.push(op);

        // There are data dependencies between calls to `store_cold_state()` that prevent us from
        // doing one big call to `store.cold_db.do_atomically()` at end of the loop.
        // 在`store_cold_state()`之间有依赖 ，防止我们做一次大的调用，对于`store.cold_db.do_atomically()`
        // 在loop的结束
        store.cold_db.do_atomically(cold_db_ops)?;

        // Delete the old summary, and the full state if we lie on an epoch boundary.
        // 删除old summary，以及full state，如果我们在epoch boundary
        hot_db_ops.push(StoreOp::DeleteState(state_root, Some(slot)));

        // Delete the execution payload if payload pruning is enabled. At a skipped slot we may
        // delete the payload for the finalized block itself, but that's OK as we only guarantee
        // that payloads are present for slots >= the split slot. The payload fetching code is also
        // forgiving of missing payloads.
        // 删除execution payload，如果使能了payload pruning，在skipped slot，我们可能删除finalized block自己的
        // payload
        if store.config.prune_payloads {
            hot_db_ops.push(StoreOp::DeleteExecutionPayload(block_root));
        }
    }

    // Warning: Critical section.  We have to take care not to put any of the two databases in an
    //          inconsistent state if the OS process dies at any point during the freezeing
    //          procedure.
    //
    // Since it is pretty much impossible to be atomic across more than one database, we trade
    // losing track of states to delete, for consistency.  In other words: We should be safe to die
    // at any point below but it may happen that some states won't be deleted from the hot database
    // and will remain there forever.  Since dying in these particular few lines should be an
    // exceedingly rare event, this should be an acceptable tradeoff.

    // Flush to disk all the states that have just been migrated to the cold store.
    // flush到磁盘，对于所有迁移到cold store的states
    store.cold_db.sync()?;

    {
        let mut split_guard = store.split.write();
        let latest_split_slot = split_guard.slot;

        // Detect a sitation where the split point is (erroneously) changed from more than one
        // place in code.
        // 检测split point在代码中改变超过一次
        if latest_split_slot != current_split_slot {
            error!(
                store.log,
                "Race condition detected: Split point changed while moving states to the freezer";
                "previous split slot" => current_split_slot,
                "current split slot" => latest_split_slot,
            );

            // Assume the freezing procedure will be retried in case this happens.
            // 假设freezing procedure会被重试 ，万一发生的话
            return Err(Error::SplitPointModified(
                current_split_slot,
                latest_split_slot,
            ));
        }

        // Before updating the in-memory split value, we flush it to disk first, so that should the
        // OS process die at this point, we pick up from the right place after a restart.
        // 在更新内存中的split value，我们首先刷到磁盘，因此即使OS进程在这个时候die，在重启之后，我们可以从
        // 正确的地方开始
        let split = Split {
            slot: frozen_head.slot(),
            state_root: frozen_head_root,
        };
        store.hot_db.put_sync(&SPLIT_KEY, &split)?;

        // Split point is now persisted in the hot database on disk.  The in-memory split point
        // hasn't been modified elsewhere since we keep a write lock on it.  It's safe to update
        // the in-memory split point now.
        // split point已经持久化到磁盘的hot db，内存中的split point没有改变，因为我们有一个write lock
        *split_guard = split;
    }

    // Delete the states from the hot database if we got this far.
    // 从hot db中删除states，如果我们到了这么远
    store.do_atomically(hot_db_ops)?;

    debug!(
        store.log,
        "Freezer migration complete";
        "slot" => frozen_head.slot()
    );

    Ok(())
}

/// Struct for storing the split slot and state root in the database.
/// 在数据库中用于存储split slot和state root的结构
#[derive(Debug, Clone, Copy, PartialEq, Default, Encode, Decode, Deserialize, Serialize)]
pub struct Split {
    pub(crate) slot: Slot,
    pub(crate) state_root: Hash256,
}

impl StoreItem for Split {
    fn db_column() -> DBColumn {
        DBColumn::BeaconMeta
    }

    fn as_store_bytes(&self) -> Vec<u8> {
        self.as_ssz_bytes()
    }

    fn from_store_bytes(bytes: &[u8]) -> Result<Self, Error> {
        Ok(Self::from_ssz_bytes(bytes)?)
    }
}

/// Type hint.
fn no_state_root_iter() -> Option<std::iter::Empty<Result<(Hash256, Slot), Error>>> {
    None
}

/// Struct for summarising a state in the hot database.
/// 结构体用于summarising一个state到hot db中
///
/// Allows full reconstruction by replaying blocks.
/// 允许通过重放blocks来reconstruction
#[derive(Debug, Clone, Copy, Default, Encode, Decode)]
pub struct HotStateSummary {
    slot: Slot,
    pub latest_block_root: Hash256,
    epoch_boundary_state_root: Hash256,
}

impl StoreItem for HotStateSummary {
    fn db_column() -> DBColumn {
        DBColumn::BeaconStateSummary
    }

    fn as_store_bytes(&self) -> Vec<u8> {
        self.as_ssz_bytes()
    }

    fn from_store_bytes(bytes: &[u8]) -> Result<Self, Error> {
        Ok(Self::from_ssz_bytes(bytes)?)
    }
}

impl HotStateSummary {
    /// Construct a new summary of the given state.
    /// 对给定的state构建一个新的summary
    pub fn new<E: EthSpec>(state_root: &Hash256, state: &BeaconState<E>) -> Result<Self, Error> {
        // Fill in the state root on the latest block header if necessary (this happens on all
        // slots where there isn't a skip).
        // 填充state root到最新的block header，如果需要的话（这发生在所有的slots，其中没有skip的话）
        let latest_block_root = state.get_latest_block_root(*state_root);
        let epoch_boundary_slot = state.slot() / E::slots_per_epoch() * E::slots_per_epoch();
        let epoch_boundary_state_root = if epoch_boundary_slot == state.slot() {
            *state_root
        } else {
            *state
                .get_state_root(epoch_boundary_slot)
                .map_err(HotColdDBError::HotStateSummaryError)?
        };

        Ok(HotStateSummary {
            slot: state.slot(),
            // 最新的block的root
            latest_block_root,
            // epoch boundary的state root
            epoch_boundary_state_root,
        })
    }
}

/// Struct for summarising a state in the freezer database.
/// 一个结构用于统计一个state，在freezer database中
#[derive(Debug, Clone, Copy, Default, Encode, Decode)]
pub(crate) struct ColdStateSummary {
    pub slot: Slot,
}

impl StoreItem for ColdStateSummary {
    fn db_column() -> DBColumn {
        DBColumn::BeaconStateSummary
    }

    fn as_store_bytes(&self) -> Vec<u8> {
        self.as_ssz_bytes()
    }

    fn from_store_bytes(bytes: &[u8]) -> Result<Self, Error> {
        Ok(Self::from_ssz_bytes(bytes)?)
    }
}

/// Struct for storing the state root of a restore point in the database.
#[derive(Debug, Clone, Copy, Default, Encode, Decode)]
struct RestorePointHash {
    state_root: Hash256,
}

impl StoreItem for RestorePointHash {
    fn db_column() -> DBColumn {
        DBColumn::BeaconRestorePoint
    }

    fn as_store_bytes(&self) -> Vec<u8> {
        self.as_ssz_bytes()
    }

    fn from_store_bytes(bytes: &[u8]) -> Result<Self, Error> {
        Ok(Self::from_ssz_bytes(bytes)?)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct TemporaryFlag;

impl StoreItem for TemporaryFlag {
    fn db_column() -> DBColumn {
        DBColumn::BeaconStateTemporary
    }

    fn as_store_bytes(&self) -> Vec<u8> {
        vec![]
    }

    fn from_store_bytes(_: &[u8]) -> Result<Self, Error> {
        Ok(TemporaryFlag)
    }
}
