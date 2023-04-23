use crate::{DBColumn, Error, StoreItem};
use serde_derive::{Deserialize, Serialize};
use ssz::{Decode, Encode};
use ssz_derive::{Decode, Encode};
use types::{EthSpec, MinimalEthSpec};

pub const PREV_DEFAULT_SLOTS_PER_RESTORE_POINT: u64 = 2048;
// restore point之间的slots数目为8192个
pub const DEFAULT_SLOTS_PER_RESTORE_POINT: u64 = 8192;
pub const DEFAULT_BLOCK_CACHE_SIZE: usize = 5;

/// Database configuration parameters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreConfig {
    /// Number of slots to wait between storing restore points in the freezer database.
    /// 存储在freezer database的restroe points之间的slots的数目
    pub slots_per_restore_point: u64,
    /// Flag indicating whether the `slots_per_restore_point` was set explicitly by the user.
    /// flag表示是否由用户显式设置`slots_per_restore_point`
    pub slots_per_restore_point_set_explicitly: bool,
    /// Maximum number of blocks to store in the in-memory block cache.
    /// 存储在内存中的block cache的blocks的最大值
    pub block_cache_size: usize,
    /// Whether to compact the database on initialization.
    /// 是否在初始化的时候对数据库进行压缩
    pub compact_on_init: bool,
    /// Whether to compact the database during database pruning.
    /// 是否在database pruning的时候对db进行压缩
    pub compact_on_prune: bool,
    /// Whether to prune payloads on initialization and finalization.
    /// 是否在初始化以及finalization的时候移除payloads
    pub prune_payloads: bool,
}

/// Variant of `StoreConfig` that gets written to disk. Contains immutable configuration params.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct OnDiskStoreConfig {
    pub slots_per_restore_point: u64,
}

#[derive(Debug, Clone)]
pub enum StoreConfigError {
    MismatchedSlotsPerRestorePoint { config: u64, on_disk: u64 },
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            // Safe default for tests, shouldn't ever be read by a CLI node.
            // 用于测试，不能被CLI node读到
            slots_per_restore_point: MinimalEthSpec::slots_per_historical_root() as u64,
            slots_per_restore_point_set_explicitly: false,
            block_cache_size: DEFAULT_BLOCK_CACHE_SIZE,
            compact_on_init: false,
            compact_on_prune: true,
            prune_payloads: true,
        }
    }
}

impl StoreConfig {
    pub fn as_disk_config(&self) -> OnDiskStoreConfig {
        OnDiskStoreConfig {
            slots_per_restore_point: self.slots_per_restore_point,
        }
    }

    pub fn check_compatibility(
        &self,
        on_disk_config: &OnDiskStoreConfig,
    ) -> Result<(), StoreConfigError> {
        if self.slots_per_restore_point != on_disk_config.slots_per_restore_point {
            return Err(StoreConfigError::MismatchedSlotsPerRestorePoint {
                config: self.slots_per_restore_point,
                on_disk: on_disk_config.slots_per_restore_point,
            });
        }
        Ok(())
    }
}

impl StoreItem for OnDiskStoreConfig {
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
