use integer_sqrt::IntegerSquareRoot;
use smallvec::SmallVec;
use types::{
    consts::altair::{
        NUM_FLAG_INDICES, TIMELY_HEAD_FLAG_INDEX, TIMELY_SOURCE_FLAG_INDEX,
        TIMELY_TARGET_FLAG_INDEX,
    },
    BeaconStateError as Error,
};
use types::{AttestationData, BeaconState, ChainSpec, EthSpec};

/// Get the participation flags for a valid attestation.
/// 对于一个有效的attestation，获取participation flags
///
/// You should have called `verify_attestation_for_block_inclusion` or similar before
/// calling this function, in order to ensure that the attestation's source is correct.
/// 你应该在调用这个函数之前调用`verify_attestation_for_block_inclusion`或者类似的函数，以确保attestation的source是正确的
///
/// This function will return an error if the source of the attestation doesn't match the
/// state's relevant justified checkpoint.
/// 这个函数会返回一个error，如果attestation的source不匹配state的relevant justified checkpoint
pub fn get_attestation_participation_flag_indices<T: EthSpec>(
    state: &BeaconState<T>,
    data: &AttestationData,
    inclusion_delay: u64,
    spec: &ChainSpec,
) -> Result<SmallVec<[usize; NUM_FLAG_INDICES]>, Error> {
    let justified_checkpoint = if data.target.epoch == state.current_epoch() {
        // 如果attestation的target epoch是当前epoch，那么使用当前justified checkpoint
        state.current_justified_checkpoint()
    } else {
        // 否则使用之前的justified checkpoint
        state.previous_justified_checkpoint()
    };

    // Matching roots.
    let is_matching_source = data.source == justified_checkpoint;
    let is_matching_target = is_matching_source
        && data.target.root == *state.get_block_root_at_epoch(data.target.epoch)?;
    // header是否匹配
    let is_matching_head =
        is_matching_target && data.beacon_block_root == *state.get_block_root(data.slot)?;

    if !is_matching_source {
        return Err(Error::IncorrectAttestationSource);
    }

    // Participation flag indices
    let mut participation_flag_indices = SmallVec::new();
    if is_matching_source && inclusion_delay <= T::slots_per_epoch().integer_sqrt() {
        participation_flag_indices.push(TIMELY_SOURCE_FLAG_INDEX);
    }
    if is_matching_target && inclusion_delay <= T::slots_per_epoch() {
        participation_flag_indices.push(TIMELY_TARGET_FLAG_INDEX);
    }
    if is_matching_head && inclusion_delay == spec.min_attestation_inclusion_delay {
        participation_flag_indices.push(TIMELY_HEAD_FLAG_INDEX);
    }
    Ok(participation_flag_indices)
}
