use types::*;

/// Returns validator indices which participated in the attestation, sorted by increasing index.
/// 返回参与attestation的validator indices，按照index递增排序
pub fn get_attesting_indices<T: EthSpec>(
    committee: &[usize],
    bitlist: &BitList<T::MaxValidatorsPerCommittee>,
) -> Result<Vec<u64>, BeaconStateError> {
    if bitlist.len() != committee.len() {
        return Err(BeaconStateError::InvalidBitfield);
    }

    let mut indices = Vec::with_capacity(bitlist.num_set_bits());

    for (i, validator_index) in committee.iter().enumerate() {
        // 遍历bitlist，如果bitlist[i]为true，则将validator_index加入indices
        if let Ok(true) = bitlist.get(i) {
            indices.push(*validator_index as u64)
        }
    }

    indices.sort_unstable();

    Ok(indices)
}

/// Shortcut for getting the attesting indices while fetching the committee from the state's cache.
pub fn get_attesting_indices_from_state<T: EthSpec>(
    state: &BeaconState<T>,
    att: &Attestation<T>,
) -> Result<Vec<u64>, BeaconStateError> {
    let committee = state.get_beacon_committee(att.data.slot, att.data.index)?;
    get_attesting_indices::<T>(committee.committee, &att.aggregation_bits)
}
