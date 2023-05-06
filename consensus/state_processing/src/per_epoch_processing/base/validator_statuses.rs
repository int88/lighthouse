use crate::common::get_attesting_indices;
use safe_arith::SafeArith;
use types::{BeaconState, BeaconStateError, ChainSpec, Epoch, EthSpec, PendingAttestation};

#[cfg(feature = "arbitrary-fuzz")]
use arbitrary::Arbitrary;

/// Sets the boolean `var` on `self` to be true if it is true on `other`. Otherwise leaves `self`
/// as is.
macro_rules! set_self_if_other_is_true {
    ($self_: ident, $other: ident, $var: ident) => {
        if $other.$var {
            $self_.$var = true;
        }
    };
}

/// The information required to reward a block producer for including an attestation in a block.
#[cfg_attr(feature = "arbitrary-fuzz", derive(Arbitrary))]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct InclusionInfo {
    /// The distance between the attestation slot and the slot that attestation was included in a
    /// block.
    /// 在attestation slot被提出，到attestation被包含到一个block中，之间的distance
    pub delay: u64,
    /// The index of the proposer at the slot where the attestation was included.
    /// 在attestation被包含的slot的proposer的索引
    pub proposer_index: usize,
}

impl Default for InclusionInfo {
    /// Defaults to `delay` at its maximum value and `proposer_index` at zero.
    fn default() -> Self {
        Self {
            delay: u64::max_value(),
            proposer_index: 0,
        }
    }
}

impl InclusionInfo {
    /// Tests if some `other` `InclusionInfo` has a lower inclusion slot than `self`. If so,
    /// replaces `self` with `other`.
    pub fn update(&mut self, other: &Self) {
        if other.delay < self.delay {
            self.delay = other.delay;
            self.proposer_index = other.proposer_index;
        }
    }
}

/// Information required to reward some validator during the current and previous epoch.
#[cfg_attr(feature = "arbitrary-fuzz", derive(Arbitrary))]
#[derive(Debug, Default, Clone, PartialEq)]
pub struct ValidatorStatus {
    /// True if the validator has been slashed, ever.
    pub is_slashed: bool,
    /// True if the validator can withdraw in the current epoch.
    pub is_withdrawable_in_current_epoch: bool,
    /// True if the validator was active in the state's _current_ epoch.
    pub is_active_in_current_epoch: bool,
    /// True if the validator was active in the state's _previous_ epoch.
    pub is_active_in_previous_epoch: bool,
    /// The validator's effective balance in the _current_ epoch.
    /// 在当前的epoch，validator的effective balance
    pub current_epoch_effective_balance: u64,

    /// True if the validator had an attestation included in the _current_ epoch.
    /// true，如果validator有一个attestation包含在当前的epoch
    pub is_current_epoch_attester: bool,
    /// True if the validator's beacon block root attestation for the first slot of the _current_
    /// epoch matches the block root known to the state.
    /// 返回true，如果validator的beacon block root attestation，当前epoch的第一个slot
    pub is_current_epoch_target_attester: bool,
    /// True if the validator had an attestation included in the _previous_ epoch.
    /// 返回true，如果validator有一个attestation包含在之前的epoch
    pub is_previous_epoch_attester: bool,
    /// True if the validator's beacon block root attestation for the first slot of the _previous_
    /// epoch matches the block root known to the state.
    /// True如果validator的beacon block root attestation，对于之前epoch的第一个slot，匹配state中已知的block root
    pub is_previous_epoch_target_attester: bool,
    /// True if the validator's beacon block root attestation in the _previous_ epoch at the
    /// attestation's slot (`attestation_data.slot`) matches the block root known to the state.
    /// 返回true，如果validator的beacon block root attestation，在之前的epoch的attestation的slot，
    /// 匹配state中已知的block root
    pub is_previous_epoch_head_attester: bool,

    /// Information used to reward the block producer of this validators earliest-included
    /// attestation.
    /// 用于奖励这个validator的最早被包含的attestation的block producer的信息
    pub inclusion_info: Option<InclusionInfo>,
}

impl ValidatorStatus {
    /// Accepts some `other` `ValidatorStatus` and updates `self` if required.
    /// 接收一些其他的`ValidatorStatus`并且更新`self`，如果需要的话
    ///
    /// Will never set one of the `bool` fields to `false`, it will only set it to `true` if other
    /// contains a `true` field.
    ///
    /// Note: does not update the winning root info, this is done manually.
    pub fn update(&mut self, other: &Self) {
        // Update all the bool fields, only updating `self` if `other` is true (never setting
        // `self` to false).
        // 更新所有的bool字段，只更新`self`如果`other`为true（从不设置`self`为false）
        set_self_if_other_is_true!(self, other, is_slashed);
        set_self_if_other_is_true!(self, other, is_withdrawable_in_current_epoch);
        set_self_if_other_is_true!(self, other, is_active_in_current_epoch);
        set_self_if_other_is_true!(self, other, is_active_in_previous_epoch);
        set_self_if_other_is_true!(self, other, is_current_epoch_attester);
        set_self_if_other_is_true!(self, other, is_current_epoch_target_attester);
        set_self_if_other_is_true!(self, other, is_previous_epoch_attester);
        set_self_if_other_is_true!(self, other, is_previous_epoch_target_attester);
        set_self_if_other_is_true!(self, other, is_previous_epoch_head_attester);

        if let Some(other_info) = other.inclusion_info {
            if let Some(self_info) = self.inclusion_info.as_mut() {
                self_info.update(&other_info);
            } else {
                self.inclusion_info = other.inclusion_info;
            }
        }
    }
}

/// The total effective balances for different sets of validators during the previous and current
/// epochs.
/// 对于previous和current epochs的不同validators的总effective balances

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "arbitrary-fuzz", derive(Arbitrary))]
pub struct TotalBalances {
    /// The effective balance increment from the spec.
    /// 从spec中的effective balance increment
    effective_balance_increment: u64,
    /// The total effective balance of all active validators during the _current_ epoch.
    /// 在_current_ epoch中，所有active validators的总effective balance
    current_epoch: u64,
    /// The total effective balance of all active validators during the _previous_ epoch.
    /// 在之前的epoch中，所有active validators的总effective balance
    previous_epoch: u64,
    /// The total effective balance of all validators who attested during the _current_ epoch.
    /// 在_current_ epoch中，所有attested的validators的总effective balance
    current_epoch_attesters: u64,
    /// The total effective balance of all validators who attested during the _current_ epoch and
    /// agreed with the state about the beacon block at the first slot of the _current_ epoch.
    /// 所有validators的总effective balance，他们在_current_ epoch中attested，并且在当前epoch的第一个slot
    /// 同意state的beacon block
    current_epoch_target_attesters: u64,
    /// The total effective balance of all validators who attested during the _previous_ epoch.
    /// 所有在_previous_ epoch中attested的validators的总effective balance
    previous_epoch_attesters: u64,
    /// The total effective balance of all validators who attested during the _previous_ epoch and
    /// agreed with the state about the beacon block at the first slot of the _previous_ epoch.
    /// 所有validators的总effective balance，他们在_previous_ epoch中attested，并且在之前epoch的第一个slot
    /// 同意state的beacon block
    previous_epoch_target_attesters: u64,
    /// The total effective balance of all validators who attested during the _previous_ epoch and
    /// agreed with the state about the beacon block at the time of attestation.
    /// 所有validators的总effective balance，他们在_previous_ epoch中attested，并且在attestation的时候
    /// 同意state的beacon block
    previous_epoch_head_attesters: u64,
}

// Generate a safe accessor for a balance in `TotalBalances`, as per spec `get_total_balance`.
// 生成一个安全的accessor，对于`TotalBalances`中的balance，根据spec的`get_total_balance`
macro_rules! balance_accessor {
    ($field_name:ident) => {
        pub fn $field_name(&self) -> u64 {
            std::cmp::max(self.effective_balance_increment, self.$field_name)
        }
    };
}

impl TotalBalances {
    pub fn new(spec: &ChainSpec) -> Self {
        Self {
            effective_balance_increment: spec.effective_balance_increment,
            current_epoch: 0,
            previous_epoch: 0,
            current_epoch_attesters: 0,
            current_epoch_target_attesters: 0,
            previous_epoch_attesters: 0,
            previous_epoch_target_attesters: 0,
            previous_epoch_head_attesters: 0,
        }
    }

    balance_accessor!(current_epoch);
    balance_accessor!(previous_epoch);
    balance_accessor!(current_epoch_attesters);
    balance_accessor!(current_epoch_target_attesters);
    balance_accessor!(previous_epoch_attesters);
    balance_accessor!(previous_epoch_target_attesters);
    balance_accessor!(previous_epoch_head_attesters);
}

/// Summarised information about validator participation in the _previous and _current_ epochs of
/// some `BeaconState`.
/// 总结信息，关于validator在之前以及当前的epoch的参与度，对于一些`BeaconState`
#[cfg_attr(feature = "arbitrary-fuzz", derive(Arbitrary))]
#[derive(Debug, Clone)]
pub struct ValidatorStatuses {
    /// Information about each individual validator from the state's validator registry.
    /// 关于每个validator的信息，来自state的validator registry
    pub statuses: Vec<ValidatorStatus>,
    /// Summed balances for various sets of validators.
    /// 对于各种validators的总balances
    pub total_balances: TotalBalances,
}

impl ValidatorStatuses {
    /// Initializes a new instance, determining:
    /// 初始化一个新的实例，决定：
    ///
    /// - Active validators
    /// - 活跃的validators
    /// - Total balances for the current and previous epochs.
    /// - 当前以及之前的epochs的total balances
    ///
    /// Spec v0.12.1
    pub fn new<T: EthSpec>(
        state: &BeaconState<T>,
        spec: &ChainSpec,
    ) -> Result<Self, BeaconStateError> {
        let mut statuses = Vec::with_capacity(state.validators().len());
        let mut total_balances = TotalBalances::new(spec);

        // 遍历state中的validators
        for (i, validator) in state.validators().iter().enumerate() {
            // 获取这个validator的effective balance
            let effective_balance = state.get_effective_balance(i)?;
            // 构建validator status
            let mut status = ValidatorStatus {
                is_slashed: validator.slashed,
                is_withdrawable_in_current_epoch: validator
                    .is_withdrawable_at(state.current_epoch()),
                current_epoch_effective_balance: effective_balance,
                ..ValidatorStatus::default()
            };

            if validator.is_active_at(state.current_epoch()) {
                // validator在当前是active
                status.is_active_in_current_epoch = true;
                total_balances
                    .current_epoch
                    .safe_add_assign(effective_balance)?;
            }

            if validator.is_active_at(state.previous_epoch()) {
                // validator在之前的epoch是active
                status.is_active_in_previous_epoch = true;
                total_balances
                    .previous_epoch
                    .safe_add_assign(effective_balance)?;
            }

            // 推送status
            statuses.push(status);
        }

        Ok(Self {
            statuses,
            total_balances,
        })
    }

    /// Process some attestations from the given `state` updating the `statuses` and
    /// `total_balances` fields.
    /// 处理一些attestations，从给定的`state`，更新`statuses`以及`total_balances`等字段
    ///
    /// Spec v0.12.1
    pub fn process_attestations<T: EthSpec>(
        &mut self,
        state: &BeaconState<T>,
    ) -> Result<(), BeaconStateError> {
        let base_state = state.as_base()?;
        for a in base_state
            // 之前epoch的attestations
            .previous_epoch_attestations
            .iter()
            .chain(base_state.current_epoch_attestations.iter())
        {
            let committee = state.get_beacon_committee(a.data.slot, a.data.index)?;
            // 获取attesting indices
            let attesting_indices =
                get_attesting_indices::<T>(committee.committee, &a.aggregation_bits)?;

            let mut status = ValidatorStatus::default();

            // Profile this attestation, updating the total balances and generating an
            // `ValidatorStatus` object that applies to all participants in the attestation.
            // 描述这个attestation，更新total balances并且生成一个`ValidatorStatus`对象
            // 应用到这个attestation的所有参与者
            if a.data.target.epoch == state.current_epoch() {
                // 当前的epoch
                status.is_current_epoch_attester = true;

                if target_matches_epoch_start_block(a, state, state.current_epoch())? {
                    status.is_current_epoch_target_attester = true;
                }
            } else if a.data.target.epoch == state.previous_epoch() {
                // 之前的epoch
                status.is_previous_epoch_attester = true;

                // The inclusion delay and proposer index are only required for previous epoch
                // attesters.
                // inclusion delay和proposer index只有在之前epoch的attesters中需要
                status.inclusion_info = Some(InclusionInfo {
                    delay: a.inclusion_delay,
                    proposer_index: a.proposer_index as usize,
                });

                if target_matches_epoch_start_block(a, state, state.previous_epoch())? {
                    status.is_previous_epoch_target_attester = true;

                    if has_common_beacon_block_root(a, state)? {
                        status.is_previous_epoch_head_attester = true;
                    }
                }
            }

            // Loop through the participating validator indices and update the status vec.
            // 遍历参与的validator索引并且更新status
            for validator_index in attesting_indices {
                // 更新validator status
                self.statuses
                    .get_mut(validator_index as usize)
                    .ok_or(BeaconStateError::UnknownValidator(validator_index as usize))?
                    .update(&status);
            }
        }

        // Compute the total balances
        // 计算全部的balances
        for (index, v) in self.statuses.iter().enumerate() {
            // According to the spec, we only count unslashed validators towards the totals.
            // 根据spec，我们只计算unslashed validators的totals
            if !v.is_slashed {
                let validator_balance = state.get_effective_balance(index)?;

                // 添加validator balance
                if v.is_current_epoch_attester {
                    self.total_balances
                        .current_epoch_attesters
                        .safe_add_assign(validator_balance)?;
                }
                if v.is_current_epoch_target_attester {
                    self.total_balances
                        .current_epoch_target_attesters
                        .safe_add_assign(validator_balance)?;
                }
                if v.is_previous_epoch_attester {
                    self.total_balances
                        .previous_epoch_attesters
                        .safe_add_assign(validator_balance)?;
                }
                if v.is_previous_epoch_target_attester {
                    self.total_balances
                        .previous_epoch_target_attesters
                        .safe_add_assign(validator_balance)?;
                }
                if v.is_previous_epoch_head_attester {
                    self.total_balances
                        .previous_epoch_head_attesters
                        .safe_add_assign(validator_balance)?;
                }
            }
        }

        Ok(())
    }
}

/// Returns `true` if the attestation's FFG target is equal to the hash of the `state`'s first
/// beacon block in the given `epoch`.
/// 返回`true`，如果attestation的FFG target等于`state`的第一个beacon block的hash，在给定的`epoch`
///
/// Spec v0.12.1
fn target_matches_epoch_start_block<T: EthSpec>(
    a: &PendingAttestation<T>,
    state: &BeaconState<T>,
    epoch: Epoch,
) -> Result<bool, BeaconStateError> {
    let slot = epoch.start_slot(T::slots_per_epoch());
    let state_boundary_root = *state.get_block_root(slot)?;

    Ok(a.data.target.root == state_boundary_root)
}

/// Returns `true` if a `PendingAttestation` and `BeaconState` share the same beacon block hash for
/// the current slot of the `PendingAttestation`.
///
/// Spec v0.12.1
fn has_common_beacon_block_root<T: EthSpec>(
    a: &PendingAttestation<T>,
    state: &BeaconState<T>,
) -> Result<bool, BeaconStateError> {
    // 获取block root
    let state_block_root = *state.get_block_root(a.data.slot)?;

    // 确保两者的block root相等
    Ok(a.data.beacon_block_root == state_block_root)
}
