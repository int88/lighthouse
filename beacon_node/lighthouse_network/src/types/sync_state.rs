use serde::{Deserialize, Serialize};
use types::Slot;

/// The current state of the node.
/// node当前的状态
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SyncState {
    /// The node is performing a long-range (batch) sync over a finalized chain.
    /// In this state, parent lookups are disabled.
    /// node正在执行一个long-range（批量）的同步，在一个finalized chain之上，这个状态下，parent lookup是禁止的
    SyncingFinalized { start_slot: Slot, target_slot: Slot },
    /// The node is performing a long-range (batch) sync over one or many head chains.
    /// In this state parent lookups are disabled.
    /// node正在执行一个long-range（批量）的同步，在一个或者多个head chains之上
    SyncingHead { start_slot: Slot, target_slot: Slot },
    /// The node is undertaking a backfill sync. This occurs when a user has specified a trusted
    /// state. The node first syncs "forward" by downloading blocks up to the current head as
    /// specified by its peers. Once completed, the node enters this sync state and attempts to
    /// download all required historical blocks to complete its chain.
    /// node正处于一个backfill sync，这会在用户已经指定一个受信的state之后发生，node首先向前同步，通过
    /// 下载blocks，直到当前的head，由它们的peers指定，一旦完成，node进入这个sync state并且试着下载所有需要的
    /// historical blocks来让这个chain完整
    BackFillSyncing { completed: usize, remaining: usize },
    /// The node has completed syncing a finalized chain and is in the process of re-evaluating
    /// which sync state to progress to.
    /// node已经完成了对于finalized chain的同步并且处于重新评估要前往的sync state的过程
    SyncTransition,
    /// The node is up to date with all known peers and is connected to at least one
    /// fully synced peer. In this state, parent lookups are enabled.
    /// node已经和所有已知的peers同步并且至少连接到一个完全同步的peer，在这个状态，可以进行parent lookup
    Synced,
    /// No useful peers are connected. Long-range sync's cannot proceed and we have no useful
    /// peers to download parents for. More peers need to be connected before we can proceed.
    /// 没有有用的peers可以连接，Long-range sync不能进行并且我们没有有用的peers可以下载parents
    /// 需要更多的peers连接，在我们可以继续之前
    Stalled,
}

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
/// The state of the backfill sync.
pub enum BackFillState {
    /// The sync is partially completed and currently paused.
    /// sync已经部分完成并且当前停止了
    Paused,
    /// We are currently backfilling.
    /// 我们当前正在backfilling
    Syncing,
    /// A backfill sync has completed.
    /// 一个backfill sync已经完成
    Completed,
    /// A backfill sync is not required.
    NotRequired,
    /// Too many failed attempts at backfilling. Consider it failed.
    /// 在backfilling过程中有太多失败，可以认为失败
    Failed,
}

impl PartialEq for SyncState {
    fn eq(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (
                SyncState::SyncingFinalized { .. },
                SyncState::SyncingFinalized { .. }
            ) | (SyncState::SyncingHead { .. }, SyncState::SyncingHead { .. })
                | (SyncState::Synced, SyncState::Synced)
                | (SyncState::Stalled, SyncState::Stalled)
                | (SyncState::SyncTransition, SyncState::SyncTransition)
                | (
                    SyncState::BackFillSyncing { .. },
                    SyncState::BackFillSyncing { .. }
                )
        )
    }
}

impl SyncState {
    /// Returns a boolean indicating the node is currently performing a long-range sync.
    pub fn is_syncing(&self) -> bool {
        match self {
            SyncState::SyncingFinalized { .. } => true,
            SyncState::SyncingHead { .. } => true,
            SyncState::SyncTransition => true,
            // Backfill doesn't effect any logic, we consider this state, not syncing.
            SyncState::BackFillSyncing { .. } => false,
            SyncState::Synced => false,
            SyncState::Stalled => false,
        }
    }

    pub fn is_syncing_finalized(&self) -> bool {
        match self {
            SyncState::SyncingFinalized { .. } => true,
            SyncState::SyncingHead { .. } => false,
            SyncState::SyncTransition => false,
            SyncState::BackFillSyncing { .. } => false,
            SyncState::Synced => false,
            SyncState::Stalled => false,
        }
    }

    /// Returns true if the node is synced.
    ///
    /// NOTE: We consider the node synced if it is fetching old historical blocks.
    pub fn is_synced(&self) -> bool {
        matches!(self, SyncState::Synced | SyncState::BackFillSyncing { .. })
    }
}

impl std::fmt::Display for SyncState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyncState::SyncingFinalized { .. } => write!(f, "Syncing Finalized Chain"),
            SyncState::SyncingHead { .. } => write!(f, "Syncing Head Chain"),
            SyncState::Synced { .. } => write!(f, "Synced"),
            SyncState::Stalled { .. } => write!(f, "Stalled"),
            SyncState::SyncTransition => write!(f, "Evaluating known peers"),
            SyncState::BackFillSyncing { .. } => write!(f, "Syncing Historical Blocks"),
        }
    }
}
