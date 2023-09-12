// 1. Kafka - Get candidate message
//  a. Store inmemory.
// 2. Kafka - Get decision message.
//  a. Update the store.
// 3. Handle `On Commit` part of the message
//  a. Can there be anything other than publishing to kafka?
//  b. what if the topic doesnt exist?
//  c. Any validation required on what is being published?
//  d. Publish T(k) only if all prioir items are published or if safepoint of T(k) is published?
//  e. If there are multiple messages to be published, should they be done serially?:-
//      i. If to the same topic
//     ii. If to another topic
// 4. After a message was published:-
//  a. Mark that item as processed.
//  b. Prune the store if contiguous items are processed.

use log::error;
use serde_json::Value;
use talos_certifier::{
    model::{Decision, DecisionMessageTrait},
    ports::MessageReciever,
    ChannelMessage,
};
use talos_suffix::{core::SuffixConfig, Suffix, SuffixTrait};

use crate::suffix::{MessengerCandidate, MessengerSuffixTrait, SuffixItemState};

pub async fn talos_messenger<M>(mut certifier_message_receiver: M)
where
    M: MessageReciever<Message = ChannelMessage> + Send + Sync + 'static,
{
    // TODO: GK - Suffix should be passed as a param
    // let suffix_config = SuffixConfig {
    //     capacity: config.suffix_capacity,
    //     prune_start_threshold: config.suffix_prune_threshold,
    //     min_size_after_prune: config.suffix_minimum_size_on_prune,
    // };
    let suffix_config = SuffixConfig {
        capacity: 400_000,
        prune_start_threshold: Some(2_000),
        min_size_after_prune: None,
    };
    let mut suffix: Suffix<MessengerCandidate> = Suffix::with_config(suffix_config);
    loop {
        tokio::select! {
            // 1. Consume message.
        res = certifier_message_receiver.consume_message() => {
            if let Ok(Some(msg)) = res {

                // 2. Add/update to suffix.
                match msg {
                    // 2.1 For CM - Install messages on the version
                    ChannelMessage::Candidate(message) => {
                        let version = message.version;

                        let has_no_commit_actions = message.on_commit.is_none();
                        let _ = suffix.insert(version, message.into());

                        // If no on-commit messages, mark the item as processed.
                        if has_no_commit_actions {
                            suffix.set_item_state(version, SuffixItemState::Complete(crate::suffix::SuffixItemCompleteStateReason::NoCommitActions));
                        }
                    },
                    // 2.2 For DM - Update the decision with outcome + safepoint.
                    ChannelMessage::Decision(decision_version, decision_message) => {
                        let version = decision_message.get_candidate_version();

                        let _ = suffix.update_decision_suffix_item(version, decision_version);
                        suffix.update_item_decision(version, &decision_message);

                        // If abort, mark the item as processed.
                        if decision_message.is_abort() {
                            suffix.set_item_state(version, SuffixItemState::Complete(crate::suffix::SuffixItemCompleteStateReason::Aborted));
                        }
                    },
                }
                // Get items to process
                //
                // Take while safepoint of the on commit is already processed.

                //  How to record if all the `on_commits` are completed?
                //  1. Messenger should filter out the relavant ones it should process.
                //  2. Messenger should keep track of individual on commits that it is relavant
                //  3. When all relavant ones are done, mark the CM as `processed`
                let items_to_process = suffix.get_on_commit_actions_to_process();

                error!("Items to process... {:?}", items_to_process);
            }
        }
        // Next condition - Commit, get processed/published info.
        }
    }
}
