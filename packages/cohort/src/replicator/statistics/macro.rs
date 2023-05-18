// Example usage:-
// ===============
// let mut struct_k = ReplicatorStatisticsItem{ version,   ..Default::default() };
// time_it!(
//     struct_k,
//     suffix_insert_candidate_time,
//     {
//         replicator.process_consumer_message(version, message.into()).await;
//     }
// );

// info!("Struct_k={struct_k:#?}");

// #[macro_export]
// macro_rules! time_it {
//     ( $struct:expr, $field_path:ident,$( $tt:tt)+) => {
//         let timer = std::time::Instant::now();
//         $(
//             $tt
//         )+
//         // info!("Timing: {:?}",  timer.elapsed());
//         $struct.$field_path = Some(timer.elapsed().as_nanos())
//     }
// }

/// Macro to capture the time statistics for some code block executed by the replicator.
#[macro_export]
macro_rules! replicator_update_stats_time {
    ( $tx:ident, $key:ident, $version:ident, $( $tt:tt)+) => {
        let timer = std::time::Instant::now();
        $(
            $tt
        )+

        if $tx.is_some() {
            let _ = $tx.as_ref().unwrap().send(ReplicatorStatisticsChannelMessage::$key($version, timer.elapsed().as_nanos() )).await;
        }
    }
}
