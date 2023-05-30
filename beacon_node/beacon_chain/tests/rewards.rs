#![cfg(test)]

use std::collections::HashMap;

use beacon_chain::test_utils::{
    generate_deterministic_keypairs, BeaconChainHarness, EphemeralHarnessType,
};
use beacon_chain::{
    test_utils::{AttestationStrategy, BlockStrategy, RelativeSyncCommittee},
    types::{Epoch, EthSpec, Keypair, MinimalEthSpec},
};
use lazy_static::lazy_static;

pub const VALIDATOR_COUNT: usize = 64;

lazy_static! {
    static ref KEYPAIRS: Vec<Keypair> = generate_deterministic_keypairs(VALIDATOR_COUNT);
}

fn get_harness<E: EthSpec>() -> BeaconChainHarness<EphemeralHarnessType<E>> {
    // 获取默认的spec
    let mut spec = E::default_spec();

    // 我们使用altair，对于所有的tets
    spec.altair_fork_epoch = Some(Epoch::new(0)); // We use altair for all tests

    // 构建harness
    let harness = BeaconChainHarness::builder(E::default())
        .spec(spec)
        .keypairs(KEYPAIRS.to_vec())
        .fresh_ephemeral_store()
        .build();

    harness.advance_slot();

    harness
}

#[tokio::test]
async fn test_sync_committee_rewards() {
    // 产生一个epoch的blocks
    let num_block_produced = MinimalEthSpec::slots_per_epoch();
    // 构建harness
    let harness = get_harness::<MinimalEthSpec>();

    let latest_block_root = harness
        // 扩展chains
        .extend_chain(
            num_block_produced as usize,
            BlockStrategy::OnCanonicalHead,
            AttestationStrategy::AllValidators,
        )
        .await;

    // Create and add sync committee message to op_pool
    // 创建并且添加sync committee message到op_pool
    let sync_contributions = harness.make_sync_contributions(
        &harness.get_current_state(),
        latest_block_root,
        harness.get_current_slot(),
        RelativeSyncCommittee::Current,
    );

    harness
        .process_sync_contributions(sync_contributions)
        .unwrap();

    // Add block
    // 添加block
    let chain = &harness.chain;
    // 获取当前的head state以及head state root
    let (head_state, head_state_root) = harness.get_current_state_and_root();
    // 设置target slot
    let target_slot = harness.get_current_slot() + 1;

    // 在给定的slot添加一个attested block
    let (block_root, mut state) = harness
        .add_attested_block_at_slot(target_slot, head_state, head_state_root, &[])
        .await
        .unwrap();

    // 获取block对应的state
    let block = harness.get_block(block_root).unwrap();
    // 获取blinded block
    let parent_block = chain
        .get_blinded_block(&block.parent_root())
        .unwrap()
        .unwrap();
    // 获取parent state
    let parent_state = chain
        .get_state(&parent_block.state_root(), Some(parent_block.slot()))
        .unwrap()
        .unwrap();

    let reward_payload = chain
        .compute_sync_committee_rewards(block.message(), &mut state)
        .unwrap();

    let rewards = reward_payload
        .iter()
        .map(|reward| (reward.validator_index, reward.reward))
        .collect::<HashMap<_, _>>();

    let proposer_index = state
        // 获取proposer index
        .get_beacon_proposer_index(target_slot, &MinimalEthSpec::default_spec())
        .unwrap();

    let mut mismatches = vec![];

    // 遍历state的validators
    for validator in state.validators() {
        let validator_index = state
            .clone()
            .get_validator_index(&validator.pubkey)
            .unwrap()
            .unwrap();
        // 获取pre state的balance
        let pre_state_balance = parent_state.balances()[validator_index];
        // 获取post state的balance
        let post_state_balance = state.balances()[validator_index];
        let sync_committee_reward = rewards.get(&(validator_index as u64)).unwrap_or(&0);

        if validator_index == proposer_index {
            // 忽略proposer
            continue; // Ignore proposer
        }

        if pre_state_balance as i64 + *sync_committee_reward != post_state_balance as i64 {
            mismatches.push(validator_index.to_string());
        }
    }

    assert_eq!(
        mismatches.len(),
        0,
        "Expect 0 mismatches, but these validators have mismatches on balance: {} ",
        mismatches.join(",")
    );
}
