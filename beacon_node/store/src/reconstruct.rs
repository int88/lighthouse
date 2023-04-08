//! Implementation of historic state reconstruction (given complete block history).
//! 历史state重构的实现（给定完整的block history）
use crate::hot_cold_store::{HotColdDB, HotColdDBError};
use crate::{Error, ItemStore, KeyValueStore};
use itertools::{process_results, Itertools};
use slog::info;
use state_processing::{
    per_block_processing, per_slot_processing, BlockSignatureStrategy, ConsensusContext,
    VerifyBlockRoot,
};
use std::sync::Arc;
use types::{EthSpec, Hash256};

impl<E, Hot, Cold> HotColdDB<E, Hot, Cold>
where
    E: EthSpec,
    Hot: KeyValueStore<E> + ItemStore<E>,
    Cold: KeyValueStore<E> + ItemStore<E>,
{
    pub fn reconstruct_historic_states(self: &Arc<Self>) -> Result<(), Error> {
        // 获取锚点信息
        let mut anchor = if let Some(anchor) = self.get_anchor_info() {
            anchor
        } else {
            // Nothing to do, history is complete.
            // 如果历史是完整的，则什么都不用做
            return Ok(());
        };

        // Check that all historic blocks are known.
        // 检查所有的历史blocks都是已知的
        if anchor.oldest_block_slot != 0 {
            return Err(Error::MissingHistoricBlocks {
                oldest_block_slot: anchor.oldest_block_slot,
            });
        }

        info!(
            self.log,
            "Beginning historic state reconstruction";
            // 开始重构historic state
            "start_slot" => anchor.state_lower_limit,
        );

        let slots_per_restore_point = self.config.slots_per_restore_point;

        // Iterate blocks from the state lower limit to the upper limit.
        // 遍历blocks，从state lower limit到upper limit
        let lower_limit_slot = anchor.state_lower_limit;
        let split = self.get_split_info();
        let upper_limit_state = self.get_restore_point(
            anchor.state_upper_limit.as_u64() / slots_per_restore_point,
            &split,
        )?;
        let upper_limit_slot = upper_limit_state.slot();

        // Use a dummy root, as we never read the block for the upper limit state.
        // 使用一个dummy root，因为我们从不读取upper limit state的block
        let upper_limit_block_root = Hash256::repeat_byte(0xff);

        let block_root_iter = self.forwards_block_roots_iterator(
            lower_limit_slot,
            upper_limit_state,
            upper_limit_block_root,
            &self.spec,
        )?;

        // The state to be advanced.
        // 需要推进的state
        let mut state = self
            .load_cold_state_by_slot(lower_limit_slot)?
            .ok_or(HotColdDBError::MissingLowerLimitState(lower_limit_slot))?;

        state.build_all_caches(&self.spec)?;

        process_results(block_root_iter, |iter| -> Result<(), Error> {
            let mut io_batch = vec![];

            let mut prev_state_root = None;

            for ((prev_block_root, _), (block_root, slot)) in iter.tuple_windows() {
                let is_skipped_slot = prev_block_root == block_root;

                let block = if is_skipped_slot {
                    None
                } else {
                    Some(
                        self.get_blinded_block(&block_root)?
                            .ok_or(Error::BlockNotFound(block_root))?,
                    )
                };

                // Advance state to slot.
                // 移动state到slot
                per_slot_processing(&mut state, prev_state_root.take(), &self.spec)
                    .map_err(HotColdDBError::BlockReplaySlotError)?;

                // Apply block.
                // 应用block
                if let Some(block) = block {
                    let mut ctxt = ConsensusContext::new(block.slot())
                        .set_current_block_root(block_root)
                        .set_proposer_index(block.message().proposer_index());

                    per_block_processing(
                        &mut state,
                        &block,
                        BlockSignatureStrategy::NoVerification,
                        VerifyBlockRoot::True,
                        &mut ctxt,
                        &self.spec,
                    )
                    .map_err(HotColdDBError::BlockReplayBlockError)?;

                    prev_state_root = Some(block.state_root());
                }

                let state_root = prev_state_root
                    .ok_or(())
                    .or_else(|_| state.update_tree_hash_cache())?;

                // Stage state for storage in freezer DB.
                // 存储到freezer DB
                self.store_cold_state(&state_root, &state, &mut io_batch)?;

                // If the slot lies on an epoch boundary, commit the batch and update the anchor.
                // 如果slot存在于epoch boundary，提交batch并且更新anchor
                if slot % slots_per_restore_point == 0 || slot + 1 == upper_limit_slot {
                    info!(
                        self.log,
                        "State reconstruction in progress";
                        "slot" => slot,
                        "remaining" => upper_limit_slot - 1 - slot
                    );

                    self.cold_db.do_atomically(std::mem::take(&mut io_batch))?;

                    // Update anchor.
                    // 更新anchor
                    let old_anchor = Some(anchor.clone());

                    if slot + 1 == upper_limit_slot {
                        // The two limits have met in the middle! We're done!
                        // 如果两个limits相遇，则我们完成了
                        // Perform one last integrity check on the state reached.
                        // 在达到的state再执行最后的完整性检查 
                        let computed_state_root = state.update_tree_hash_cache()?;
                        if computed_state_root != state_root {
                            return Err(Error::StateReconstructionRootMismatch {
                                slot,
                                expected: state_root,
                                computed: computed_state_root,
                            });
                        }

                        // 直接置为空，将新的anchor info
                        self.compare_and_set_anchor_info_with_write(old_anchor, None)?;

                        // 直接返回
                        return Ok(());
                    } else {
                        // The lower limit has been raised, store it.
                        // 增加了lower limit，存储它
                        anchor.state_lower_limit = slot;

                        self.compare_and_set_anchor_info_with_write(
                            old_anchor,
                            Some(anchor.clone()),
                        )?;
                    }
                }
            }

            // Should always reach the `upper_limit_slot` and return early above.
            // 总是应该到达`upper_limit_slot`并且在上面返回
            Err(Error::StateReconstructionDidNotComplete)
        })??;

        // Check that the split point wasn't mutated during the state reconstruction process.
        // It shouldn't have been, due to the serialization of requests through the store migrator,
        // so this is just a paranoid check.
        // 检查split point没有在state reconstruction过程中改变，它不应该，因为请求的序列化，通过store migrator
        let latest_split = self.get_split_info();
        if split != latest_split {
            return Err(Error::SplitPointModified(latest_split.slot, split.slot));
        }

        Ok(())
    }
}
