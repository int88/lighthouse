//! Provides the `ParticipationCache`, a custom Lighthouse cache which attempts to reduce CPU and
//! memory usage by:
//! 提供`ParticipationCache`，一个自定义的Lighthouse cache，它试图通过：
//!
//! - Caching a map of `validator_index -> participation_flags` for all active validators in the
//!   previous and current epochs.
//! - 缓存一个map，`validator_index -> participation_flags`，对于所有的active validators，在之前和当前的epoch
//! - Caching the total balances of:
//! - 缓存total balances：
//!   - All active validators.
//!   - 所有的active validators
//!   - All active validators matching each of the three "timely" flags.
//!   - 所有的active validators匹配三个"timely" flags中的每一个
//! - Caching the "eligible" validators.
//! - 缓存"合格的" validators
//!
//! Additionally, this cache is returned from the `altair::process_epoch` function and can be used
//! to get useful summaries about the validator participation in an epoch.
//! 另外，这个cache从`altair::process_epoch`函数返回，并且可以用来获取有用的关于validator参与的epoch的总结

use safe_arith::{ArithError, SafeArith};
use types::{
    consts::altair::{
        NUM_FLAG_INDICES, TIMELY_HEAD_FLAG_INDEX, TIMELY_SOURCE_FLAG_INDEX,
        TIMELY_TARGET_FLAG_INDEX,
    },
    BeaconState, BeaconStateError, ChainSpec, Epoch, EthSpec, ParticipationFlags, RelativeEpoch,
};

#[derive(Debug, PartialEq)]
pub enum Error {
    InvalidFlagIndex(usize),
    InvalidValidatorIndex(usize),
}

/// A balance which will never be below the specified `minimum`.
/// 一个balance，永远不会低于指定的`minimum`
///
/// This is an effort to ensure the `EFFECTIVE_BALANCE_INCREMENT` minimum is always respected.
/// 这是一个努力，确保`EFFECTIVE_BALANCE_INCREMENT`的最小值总是被尊重的
#[derive(PartialEq, Debug, Clone, Copy)]
struct Balance {
    raw: u64,
    minimum: u64,
}

impl Balance {
    /// Initialize the balance to `0`, or the given `minimum`.
    /// 初始化balance为`0`，或者给定的`minimum`
    pub fn zero(minimum: u64) -> Self {
        Self { raw: 0, minimum }
    }

    /// Returns the balance with respect to the initialization `minimum`.
    /// 返回balance，关于初始化的`minimum`
    pub fn get(&self) -> u64 {
        std::cmp::max(self.raw, self.minimum)
    }

    /// Add-assign to the balance.
    /// 增加balance
    pub fn safe_add_assign(&mut self, other: u64) -> Result<(), ArithError> {
        self.raw.safe_add_assign(other)
    }
}

/// Caches the participation values for one epoch (either the previous or current).
/// 对于一个epoch，缓存participation的值（之前或者当前）
#[derive(PartialEq, Debug)]
struct SingleEpochParticipationCache {
    /// Maps an active validator index to their participation flags.
    /// 映射一个active validator的index到他们的participation flags
    ///
    /// To reiterate, only active and unslashed validator indices are stored in this map.
    /// 为了重申，只有active和unslashed的validator indices被存储在这个map中
    ///
    /// ## Note
    ///
    /// It would be ideal to maintain a reference to the `BeaconState` here rather than copying the
    /// `ParticipationFlags`, however that would cause us to run into mutable reference limitations
    /// upstream.
    /// 这会更理想，维护一个`BeaconState`的引用在这里，而不是复制`ParticipationFlags`，然而这会导致我们遇到可变引用的限制
    unslashed_participating_indices: Vec<Option<ParticipationFlags>>,
    /// Stores the sum of the balances for all validators in `self.unslashed_participating_indices`
    /// for all flags in `NUM_FLAG_INDICES`.
    /// 存储所有validators的balances的总和，在`self.unslashed_participating_indices`中，对于所有的flags在`NUM_FLAG_INDICES`中
    ///
    /// A flag balance is only incremented if a validator is in that flag set.
    /// 一个flag balance增长只有在一个validator在这个flag set中
    total_flag_balances: [Balance; NUM_FLAG_INDICES],
    /// Stores the sum of all balances of all validators in `self.unslashed_participating_indices`
    /// (regardless of which flags are set).
    /// 存储所有validators的balances的总和，在`self.unslashed_participating_indices`中（不管哪个flag被设置）
    total_active_balance: Balance,
}

impl SingleEpochParticipationCache {
    fn new<T: EthSpec>(state: &BeaconState<T>, spec: &ChainSpec) -> Self {
        let num_validators = state.validators().len();
        let zero_balance = Balance::zero(spec.effective_balance_increment);

        Self {
            unslashed_participating_indices: vec![None; num_validators],
            total_flag_balances: [zero_balance; NUM_FLAG_INDICES],
            total_active_balance: zero_balance,
        }
    }

    /// Returns the total balance of attesters who have `flag_index` set.
    fn total_flag_balance(&self, flag_index: usize) -> Result<u64, Error> {
        self.total_flag_balances
            .get(flag_index)
            .map(Balance::get)
            .ok_or(Error::InvalidFlagIndex(flag_index))
    }

    /// Returns `true` if `val_index` is active, unslashed and has `flag_index` set.
    ///
    /// ## Errors
    ///
    /// May return an error if `flag_index` is out-of-bounds.
    fn has_flag(&self, val_index: usize, flag_index: usize) -> Result<bool, Error> {
        let participation_flags = self
            .unslashed_participating_indices
            .get(val_index)
            .ok_or(Error::InvalidValidatorIndex(val_index))?;
        if let Some(participation_flags) = participation_flags {
            participation_flags
                .has_flag(flag_index)
                .map_err(|_| Error::InvalidFlagIndex(flag_index))
        } else {
            Ok(false)
        }
    }

    /// Process an **active** validator, reading from the `state` with respect to the
    /// `relative_epoch`.
    /// 处理一个**active**的validator，从`state`中读取，关于`relative_epoch`
    ///
    /// ## Errors
    ///
    /// - The provided `state` **must** be Altair. An error will be returned otherwise.
    /// - 提供的`state`必须是Altair，否则会返回一个error
    /// - An error will be returned if the `val_index` validator is inactive at the given
    ///     `relative_epoch`.
    /// - 如果`val_index` validator在给定的`relative_epoch`是inactive的，会返回一个error
    fn process_active_validator<T: EthSpec>(
        &mut self,
        val_index: usize,
        state: &BeaconState<T>,
        current_epoch: Epoch,
        relative_epoch: RelativeEpoch,
    ) -> Result<(), BeaconStateError> {
        // 根据index返回val_balance和validator的值
        let val_balance = state.get_effective_balance(val_index)?;
        let validator = state.get_validator(val_index)?;

        // Sanity check to ensure the validator is active.
        // 确保validator是active的
        let epoch = relative_epoch.into_epoch(current_epoch);
        if !validator.is_active_at(epoch) {
            return Err(BeaconStateError::ValidatorIsInactive { val_index });
        }

        let epoch_participation = match relative_epoch {
            // 根据relative_epoch返回对应的epoch participation
            RelativeEpoch::Current => state.current_epoch_participation(),
            RelativeEpoch::Previous => state.previous_epoch_participation(),
            _ => Err(BeaconStateError::EpochOutOfBounds),
        }?
        .get(val_index)
        .ok_or(BeaconStateError::ParticipationOutOfBounds(val_index))?;

        // All active validators increase the total active balance.
        // 所有的active validators增加total active balance
        self.total_active_balance.safe_add_assign(val_balance)?;

        // Only unslashed validators may proceed.
        // 只有unslashed validators可以继续
        if validator.slashed {
            return Ok(());
        }

        // Add their `ParticipationFlags` to the map.
        // 添加他们的`ParticipationFlags`到map中
        *self
            .unslashed_participating_indices
            .get_mut(val_index)
            .ok_or(BeaconStateError::UnknownValidator(val_index))? = Some(*epoch_participation);

        // Iterate through all the flags and increment the total flag balances for whichever flags
        // are set for `val_index`.
        // 迭代所有的flags，增加total flag balances，对于哪个flags被设置为`val_index`
        for (flag, balance) in self.total_flag_balances.iter_mut().enumerate() {
            if epoch_participation.has_flag(flag)? {
                balance.safe_add_assign(val_balance)?;
            }
        }

        Ok(())
    }
}

/// Maintains a cache to be used during `altair::process_epoch`.
/// 维护一个cache，在`altair::process_epoch`使用
#[derive(PartialEq, Debug)]
pub struct ParticipationCache {
    // 当前epoch的值
    current_epoch: Epoch,
    /// Caches information about active validators pertaining to `self.current_epoch`.
    /// 缓存信息关于active validators，关于`self.current_epoch`
    current_epoch_participation: SingleEpochParticipationCache,
    // 上一个epoch的值
    previous_epoch: Epoch,
    /// Caches information about active validators pertaining to `self.previous_epoch`.
    /// 缓存信息关于active validators，关于`self.previous_epoch`
    previous_epoch_participation: SingleEpochParticipationCache,
    /// Caches the result of the `get_eligible_validator_indices` function.
    /// 缓存结果，对于`get_eligible_validator_indices`函数
    eligible_indices: Vec<usize>,
}

impl ParticipationCache {
    /// Instantiate `Self`, returning a fully initialized cache.
    /// 实例化`Self`，返回一个完全初始化的cache
    ///
    /// ## Errors
    ///
    /// - The provided `state` **must** be an Altair state. An error will be returned otherwise.
    /// - 提供的`state`**必须**是一个Altair state，否则会返回一个error 
    pub fn new<T: EthSpec>(
        state: &BeaconState<T>,
        spec: &ChainSpec,
    ) -> Result<Self, BeaconStateError> {
        let current_epoch = state.current_epoch();
        let previous_epoch = state.previous_epoch();

        // Both the current/previous epoch participations are set to a capacity that is slightly
        // larger than required. The difference will be due slashed-but-active validators.
        // current/previous epoch participations都会设置为一个比要求的稍微大一点的容量，差异会由slashed-but-active validators引起
        let mut current_epoch_participation = SingleEpochParticipationCache::new(state, spec);
        let mut previous_epoch_participation = SingleEpochParticipationCache::new(state, spec);
        // Contains the set of validators which are either:
        // 包含一组validators，他们是：
        //
        // - Active in the previous epoch.
        // - 在上一个epoch是active的
        // - Slashed, but not yet withdrawable.
        // - 已经slashed，但是还没有withdrawable
        //
        // Using the full length of `state.validators` is almost always overkill, but it ensures no
        // reallocations.
        // 使用`state.validators`的完整长度几乎总是过度的，但是它确保没有重新分配
        let mut eligible_indices = Vec::with_capacity(state.validators().len());

        // Iterate through all validators, updating:
        // 遍历所有的validators，更新：
        //
        // 1. Validator participation for current and previous epochs.
        // 1. 对于当前和上一个epoch的validator participation
        // 2. The "eligible indices".
        // 2. 那个"合格的指数"
        //
        // Care is taken to ensure that the ordering of `eligible_indices` is the same as the
        // `get_eligible_validator_indices` function in the spec.
        for (val_index, val) in state.validators().iter().enumerate() {
            if val.is_active_at(current_epoch) {
                // 当前epoch是active的
                current_epoch_participation.process_active_validator(
                    val_index,
                    state,
                    current_epoch,
                    RelativeEpoch::Current,
                )?;
            }

            if val.is_active_at(previous_epoch) {
                // 在之前epoch是active的
                previous_epoch_participation.process_active_validator(
                    val_index,
                    state,
                    current_epoch,
                    RelativeEpoch::Previous,
                )?;
            }

            // Note: a validator might still be "eligible" whilst returning `false` to
            // `Validator::is_active_at`.
            // 注意：一个validator可能仍然是"合格的"，当返回`false`到`Validator::is_active_at`的时候
            if state.is_eligible_validator(previous_epoch, val_index)? {
                eligible_indices.push(val_index)
            }
        }

        Ok(Self {
            current_epoch,
            current_epoch_participation,
            previous_epoch,
            previous_epoch_participation,
            eligible_indices,
        })
    }

    /// Equivalent to the specification `get_eligible_validator_indices` function.
    /// 和spec中的`get_eligible_validator_indices`函数相同
    pub fn eligible_validator_indices(&self) -> &[usize] {
        &self.eligible_indices
    }

    /// Equivalent to the `get_unslashed_participating_indices` function in the specification.
    /// 相当于规范中的`get_unslashed_participating_indices`函数
    pub fn get_unslashed_participating_indices(
        &self,
        flag_index: usize,
        epoch: Epoch,
    ) -> Result<UnslashedParticipatingIndices, BeaconStateError> {
        let participation = if epoch == self.current_epoch {
            // 根据epoch返回对应的epoch participation
            &self.current_epoch_participation
        } else if epoch == self.previous_epoch {
            &self.previous_epoch_participation
        } else {
            return Err(BeaconStateError::EpochOutOfBounds);
        };

        Ok(UnslashedParticipatingIndices {
            participation,
            flag_index,
        })
    }

    /*
     * Balances
     */

    pub fn current_epoch_total_active_balance(&self) -> u64 {
        self.current_epoch_participation.total_active_balance.get()
    }

    pub fn current_epoch_target_attesting_balance(&self) -> Result<u64, Error> {
        self.current_epoch_participation
            .total_flag_balance(TIMELY_TARGET_FLAG_INDEX)
    }

    pub fn previous_epoch_total_active_balance(&self) -> u64 {
        self.previous_epoch_participation.total_active_balance.get()
    }

    pub fn previous_epoch_target_attesting_balance(&self) -> Result<u64, Error> {
        self.previous_epoch_participation
            .total_flag_balance(TIMELY_TARGET_FLAG_INDEX)
    }

    pub fn previous_epoch_source_attesting_balance(&self) -> Result<u64, Error> {
        self.previous_epoch_participation
            .total_flag_balance(TIMELY_SOURCE_FLAG_INDEX)
    }

    pub fn previous_epoch_head_attesting_balance(&self) -> Result<u64, Error> {
        self.previous_epoch_participation
            .total_flag_balance(TIMELY_HEAD_FLAG_INDEX)
    }

    /*
     * Active/Unslashed
     */

    /// Returns `None` for an unknown `val_index`.
    pub fn is_active_unslashed_in_previous_epoch(&self, val_index: usize) -> Option<bool> {
        self.previous_epoch_participation
            .unslashed_participating_indices
            .get(val_index)
            .map(|flags| flags.is_some())
    }

    /// Returns `None` for an unknown `val_index`.
    pub fn is_active_unslashed_in_current_epoch(&self, val_index: usize) -> Option<bool> {
        self.current_epoch_participation
            .unslashed_participating_indices
            .get(val_index)
            .map(|flags| flags.is_some())
    }

    /*
     * Flags
     */

    /// Always returns false for a slashed validator.
    pub fn is_previous_epoch_timely_source_attester(
        &self,
        val_index: usize,
    ) -> Result<bool, Error> {
        self.previous_epoch_participation
            .has_flag(val_index, TIMELY_SOURCE_FLAG_INDEX)
    }

    /// Always returns false for a slashed validator.
    pub fn is_previous_epoch_timely_target_attester(
        &self,
        val_index: usize,
    ) -> Result<bool, Error> {
        self.previous_epoch_participation
            .has_flag(val_index, TIMELY_TARGET_FLAG_INDEX)
    }

    /// Always returns false for a slashed validator.
    pub fn is_previous_epoch_timely_head_attester(&self, val_index: usize) -> Result<bool, Error> {
        self.previous_epoch_participation
            .has_flag(val_index, TIMELY_HEAD_FLAG_INDEX)
    }

    /// Always returns false for a slashed validator.
    pub fn is_current_epoch_timely_source_attester(&self, val_index: usize) -> Result<bool, Error> {
        self.current_epoch_participation
            .has_flag(val_index, TIMELY_SOURCE_FLAG_INDEX)
    }

    /// Always returns false for a slashed validator.
    pub fn is_current_epoch_timely_target_attester(&self, val_index: usize) -> Result<bool, Error> {
        self.current_epoch_participation
            .has_flag(val_index, TIMELY_TARGET_FLAG_INDEX)
    }

    /// Always returns false for a slashed validator.
    pub fn is_current_epoch_timely_head_attester(&self, val_index: usize) -> Result<bool, Error> {
        self.current_epoch_participation
            .has_flag(val_index, TIMELY_HEAD_FLAG_INDEX)
    }
}

/// Imitates the return value of the `get_unslashed_participating_indices` in the
/// specification.
/// 模仿规范中的`get_unslashed_participating_indices`的返回值
///
/// This struct exists to help make the Lighthouse code read more like the specification.
/// 这个结构让Lighthouse的代码更像规范
pub struct UnslashedParticipatingIndices<'a> {
    participation: &'a SingleEpochParticipationCache,
    flag_index: usize,
}

impl<'a> UnslashedParticipatingIndices<'a> {
    /// Returns `Ok(true)` if the given `val_index` is both:
    ///
    /// - An active validator.
    /// - Has `self.flag_index` set.
    pub fn contains(&self, val_index: usize) -> Result<bool, Error> {
        self.participation.has_flag(val_index, self.flag_index)
    }

    /// Returns the sum of all balances of validators which have `self.flag_index` set.
    /// 返回所有的validators的balances的总和，他们有`self.flag_index`被设置
    ///
    /// ## Notes
    ///
    /// Respects the `EFFECTIVE_BALANCE_INCREMENT` minimum.
    /// 尊重`EFFECTIVE_BALANCE_INCREMENT`的最小值
    pub fn total_balance(&self) -> Result<u64, Error> {
        self.participation
            .total_flag_balances
            .get(self.flag_index)
            .ok_or(Error::InvalidFlagIndex(self.flag_index))
            .map(Balance::get)
    }
}
