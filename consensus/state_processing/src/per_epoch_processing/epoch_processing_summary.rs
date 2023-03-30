use super::{
    altair::{participation_cache::Error as ParticipationCacheError, ParticipationCache},
    base::{validator_statuses::InclusionInfo, TotalBalances, ValidatorStatus},
};
use crate::metrics;
use std::sync::Arc;
use types::{EthSpec, SyncCommittee};

/// Provides a summary of validator participation during the epoch.
/// 提供一个validator参与的summary，在epoch中
#[derive(PartialEq, Debug)]
pub enum EpochProcessingSummary<T: EthSpec> {
    // 分为Base和Altair的EpochProcessingSummary
    Base {
        total_balances: TotalBalances,
        // validator status的vector
        statuses: Vec<ValidatorStatus>,
    },
    Altair {
        participation_cache: ParticipationCache,
        sync_committee: Arc<SyncCommittee<T>>,
    },
}

impl<T: EthSpec> EpochProcessingSummary<T> {
    /// Updates some Prometheus metrics with some values in `self`.
    #[cfg(feature = "metrics")]
    pub fn observe_metrics(&self) -> Result<(), ParticipationCacheError> {
        metrics::set_gauge(
            &metrics::PARTICIPATION_PREV_EPOCH_HEAD_ATTESTING_GWEI_TOTAL,
            self.previous_epoch_head_attesting_balance()? as i64,
        );
        metrics::set_gauge(
            &metrics::PARTICIPATION_PREV_EPOCH_TARGET_ATTESTING_GWEI_TOTAL,
            self.previous_epoch_target_attesting_balance()? as i64,
        );
        metrics::set_gauge(
            &metrics::PARTICIPATION_PREV_EPOCH_SOURCE_ATTESTING_GWEI_TOTAL,
            self.previous_epoch_source_attesting_balance()? as i64,
        );
        metrics::set_gauge(
            &metrics::PARTICIPATION_PREV_EPOCH_ACTIVE_GWEI_TOTAL,
            self.previous_epoch_total_active_balance() as i64,
        );

        Ok(())
    }

    /// Returns the sync committee indices for the current epoch for altair.
    pub fn sync_committee(&self) -> Option<&SyncCommittee<T>> {
        match self {
            EpochProcessingSummary::Altair { sync_committee, .. } => Some(sync_committee),
            EpochProcessingSummary::Base { .. } => None,
        }
    }

    /// Returns the sum of the effective balance of all validators in the current epoch.
    pub fn current_epoch_total_active_balance(&self) -> u64 {
        match self {
            EpochProcessingSummary::Base { total_balances, .. } => total_balances.current_epoch(),
            EpochProcessingSummary::Altair {
                participation_cache,
                ..
            } => participation_cache.current_epoch_total_active_balance(),
        }
    }

    /// Returns the sum of the effective balance of all validators in the current epoch who
    /// included an attestation that matched the target.
    pub fn current_epoch_target_attesting_balance(&self) -> Result<u64, ParticipationCacheError> {
        match self {
            EpochProcessingSummary::Base { total_balances, .. } => {
                Ok(total_balances.current_epoch_target_attesters())
            }
            EpochProcessingSummary::Altair {
                participation_cache,
                ..
            } => participation_cache.current_epoch_target_attesting_balance(),
        }
    }

    /// Returns the sum of the effective balance of all validators in the previous epoch.
    pub fn previous_epoch_total_active_balance(&self) -> u64 {
        match self {
            EpochProcessingSummary::Base { total_balances, .. } => total_balances.previous_epoch(),
            EpochProcessingSummary::Altair {
                participation_cache,
                ..
            } => participation_cache.previous_epoch_total_active_balance(),
        }
    }

    /// Returns `true` if `val_index` was included in the active validator indices in the current
    /// epoch *and* the validator is not slashed.
    /// 返回`true`，如果`val_index`包含在active validator indices，在当前的epoch并且validator没有被slashed
    ///
    /// ## Notes
    ///
    /// Always returns `false` for an unknown `val_index`.
    pub fn is_active_unslashed_in_current_epoch(&self, val_index: usize) -> bool {
        match self {
            EpochProcessingSummary::Base { statuses, .. } => statuses
                .get(val_index)
                .map_or(false, |s| s.is_active_in_current_epoch && !s.is_slashed),
            EpochProcessingSummary::Altair {
                participation_cache,
                ..
            } => participation_cache
                .is_active_unslashed_in_current_epoch(val_index)
                .unwrap_or(false),
        }
    }

    /// Returns `true` if `val_index` had a target-matching attestation included on chain in the
    /// current epoch.
    /// 返回`true`，如果`val_index`有一个target-maching的attestation包含在chain中，在当前的epoch
    ///
    /// ## Differences between Base and Altair
    ///
    /// - Base: active validators return `true`.
    /// - Altair: only active and *unslashed* validators return `true`.
    ///
    /// ## Notes
    ///
    /// Always returns `false` for an unknown `val_index`.
    pub fn is_current_epoch_target_attester(
        &self,
        val_index: usize,
    ) -> Result<bool, ParticipationCacheError> {
        match self {
            EpochProcessingSummary::Base { statuses, .. } => Ok(statuses
                .get(val_index)
                .map_or(false, |s| s.is_current_epoch_target_attester)),
            EpochProcessingSummary::Altair {
                participation_cache,
                ..
            } => participation_cache
                .is_current_epoch_timely_target_attester(val_index)
                .or_else(|e| match e {
                    ParticipationCacheError::InvalidValidatorIndex(_) => Ok(false),
                    e => Err(e),
                }),
        }
    }

    /// Returns the sum of the effective balance of all validators in the previous epoch who
    /// included an attestation that matched the target.
    pub fn previous_epoch_target_attesting_balance(&self) -> Result<u64, ParticipationCacheError> {
        match self {
            EpochProcessingSummary::Base { total_balances, .. } => {
                Ok(total_balances.previous_epoch_target_attesters())
            }
            EpochProcessingSummary::Altair {
                participation_cache,
                ..
            } => participation_cache.previous_epoch_target_attesting_balance(),
        }
    }

    /// Returns the sum of the effective balance of all validators in the previous epoch who
    /// included an attestation that matched the head.
    ///
    /// ## Differences between Base and Altair
    ///
    /// - Base: any attestation can match the head.
    /// - Altair: only "timely" attestations can match the head.
    pub fn previous_epoch_head_attesting_balance(&self) -> Result<u64, ParticipationCacheError> {
        match self {
            EpochProcessingSummary::Base { total_balances, .. } => {
                Ok(total_balances.previous_epoch_head_attesters())
            }
            EpochProcessingSummary::Altair {
                participation_cache,
                ..
            } => participation_cache.previous_epoch_head_attesting_balance(),
        }
    }

    /// Returns the sum of the effective balance of all validators in the previous epoch who
    /// included an attestation that matched the source.
    ///
    /// ## Differences between Base and Altair
    ///
    /// - Base: any attestation can match the source.
    /// - Altair: only "timely" attestations can match the source.
    pub fn previous_epoch_source_attesting_balance(&self) -> Result<u64, ParticipationCacheError> {
        match self {
            EpochProcessingSummary::Base { total_balances, .. } => {
                Ok(total_balances.previous_epoch_attesters())
            }
            EpochProcessingSummary::Altair {
                participation_cache,
                ..
            } => participation_cache.previous_epoch_source_attesting_balance(),
        }
    }

    /// Returns `true` if `val_index` was included in the active validator indices in the previous
    /// epoch *and* the validator is not slashed.
    ///
    /// ## Notes
    ///
    /// Always returns `false` for an unknown `val_index`.
    pub fn is_active_unslashed_in_previous_epoch(&self, val_index: usize) -> bool {
        match self {
            EpochProcessingSummary::Base { statuses, .. } => statuses
                .get(val_index)
                .map_or(false, |s| s.is_active_in_previous_epoch && !s.is_slashed),
            EpochProcessingSummary::Altair {
                participation_cache,
                ..
            } => participation_cache
                .is_active_unslashed_in_previous_epoch(val_index)
                .unwrap_or(false),
        }
    }

    /// Returns `true` if `val_index` had a target-matching attestation included on chain in the
    /// previous epoch.
    /// 返回`true`，如果`val_index`有一个target匹配的attestation，包含在chain中，在之前的epoch
    ///
    /// ## Notes
    ///
    /// Always returns `false` for an unknown `val_index`.
    pub fn is_previous_epoch_target_attester(
        &self,
        val_index: usize,
    ) -> Result<bool, ParticipationCacheError> {
        match self {
            EpochProcessingSummary::Base { statuses, .. } => Ok(statuses
                .get(val_index)
                .map_or(false, |s| s.is_previous_epoch_target_attester)),
            EpochProcessingSummary::Altair {
                participation_cache,
                ..
            } => participation_cache
                .is_previous_epoch_timely_target_attester(val_index)
                .or_else(|e| match e {
                    ParticipationCacheError::InvalidValidatorIndex(_) => Ok(false),
                    e => Err(e),
                }),
        }
    }

    /// Returns `true` if `val_index` had a head-matching attestation included on chain in the
    /// previous epoch.
    ///
    /// ## Differences between Base and Altair
    ///
    /// - Base: any attestation can match the head.
    /// - Altair: only "timely" attestations can match the head.
    ///
    /// ## Notes
    ///
    /// Always returns `false` for an unknown `val_index`.
    pub fn is_previous_epoch_head_attester(
        &self,
        val_index: usize,
    ) -> Result<bool, ParticipationCacheError> {
        match self {
            EpochProcessingSummary::Base { statuses, .. } => Ok(statuses
                .get(val_index)
                .map_or(false, |s| s.is_previous_epoch_head_attester)),
            EpochProcessingSummary::Altair {
                participation_cache,
                ..
            } => participation_cache
                .is_previous_epoch_timely_head_attester(val_index)
                .or_else(|e| match e {
                    ParticipationCacheError::InvalidValidatorIndex(_) => Ok(false),
                    e => Err(e),
                }),
        }
    }

    /// Returns `true` if `val_index` had a source-matching attestation included on chain in the
    /// previous epoch.
    /// 返回`true`，如果`val_index`有一个source matching的attestation包含在chain中，在之前的epoch
    ///
    /// ## Differences between Base and Altair
    ///
    /// - Base: any attestation can match the head.
    /// - Altair: only "timely" attestations can match the source.
    ///
    /// ## Notes
    ///
    /// Always returns `false` for an unknown `val_index`.
    pub fn is_previous_epoch_source_attester(
        &self,
        val_index: usize,
    ) -> Result<bool, ParticipationCacheError> {
        match self {
            EpochProcessingSummary::Base { statuses, .. } => Ok(statuses
                .get(val_index)
                .map_or(false, |s| s.is_previous_epoch_attester)),
            EpochProcessingSummary::Altair {
                participation_cache,
                ..
            } => participation_cache
                .is_previous_epoch_timely_source_attester(val_index)
                .or_else(|e| match e {
                    ParticipationCacheError::InvalidValidatorIndex(_) => Ok(false),
                    e => Err(e),
                }),
        }
    }

    /// Returns information about the inclusion distance for `val_index` for the previous epoch.
    /// 返回inclusion distance的信息，在上一个epoch中，对于`val_index`
    ///
    /// ## Differences between Base and Altair
    ///
    /// - Base: always returns `Some` if the validator had an attestation included on-chain.
    /// - Base: 总是返回`Some`，如果validator有一个attestation包含在chain中
    /// - Altair: always returns `None`.
    /// - Altair: 总是返回None
    ///
    /// ## Notes
    ///
    /// Always returns `false` for an unknown `val_index`.
    pub fn previous_epoch_inclusion_info(&self, val_index: usize) -> Option<InclusionInfo> {
        match self {
            EpochProcessingSummary::Base { statuses, .. } => {
                statuses.get(val_index).and_then(|s| s.inclusion_info)
            }
            // 对于Altair，总是返回None
            EpochProcessingSummary::Altair { .. } => None,
        }
    }
}
