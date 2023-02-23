use criterion::{black_box, criterion_group, criterion_main, Criterion};
use talos_suffix::core::{SuffixConfig, SuffixTrait};
use talos_suffix::Suffix;

#[derive(Clone, Debug)]
#[allow(dead_code)]
struct MockSuffixItemMessage {
    xid: String,
    agent: String,
    cohort: String,
}

fn create_mock_candidate_message(suffixer: u64) -> MockSuffixItemMessage {
    let xid = format!("xid-{}", suffixer);
    let agent = format!("agent-{}", suffixer);
    let cohort = format!("cohort-{}", suffixer);
    MockSuffixItemMessage { xid, agent, cohort }
}
#[derive(Clone, Debug)]
#[allow(dead_code)]
struct MockSuffixItemWithVers {
    vers: u64,
    xid: String,
    agent: String,
    cohort: String,
}

fn create_mock_candidate_message_with_vers(suffixer: u64) -> MockSuffixItemWithVers {
    let xid = format!("xid-{}", suffixer);
    let agent = format!("agent-{}", suffixer);
    let cohort = format!("cohort-{}", suffixer);
    MockSuffixItemWithVers {
        xid,
        agent,
        cohort,
        vers: suffixer,
    }
}

fn bench_suffix_real(c: &mut Criterion) {
    c.bench_function("[SUFFIX REAL] insert 500,000 items", |b| {
        let mut suffix = Suffix::<MockSuffixItemMessage>::with_config(SuffixConfig {
            capacity: 500_000,
            min_size_after_prune: Some(10_000),
            ..SuffixConfig::default()
        });

        b.iter(|| {
            for i in 0..suffix.messages.len().try_into().unwrap() {
                suffix.insert(i, create_mock_candidate_message(i)).unwrap();
            }
        })
    });
    c.bench_function("[SUFFIX REAL] insert an item in a vec of capacity 500,000", |b| {
        let mut suffix = Suffix::with_config(SuffixConfig {
            capacity: 500_000,
            min_size_after_prune: Some(10_000),
            ..SuffixConfig::default()
        });
        b.iter(|| {
            suffix.insert(30, create_mock_candidate_message(30)).unwrap();
        })
    });
    c.bench_function("[SUFFIX REAL] get version=99999 item from 500,000 items", |b| {
        let mut suffix = Suffix::with_config(SuffixConfig {
            capacity: 500_000,
            min_size_after_prune: Some(10_000),
            ..SuffixConfig::default()
        });

        for i in 0..suffix.messages.len().try_into().unwrap() {
            suffix.insert(i, create_mock_candidate_message(i)).unwrap();
        }
        b.iter(|| suffix.get(99999))
    });
}

// fn bench_suffix_prune(c: &mut Criterion) {
//     c.bench_function("[SUFFIX REAL] prune till version=99999 item from 500,000 items", |b| {
//         let mut suffix = Suffix::with_config(SuffixConfig {
//             capacity: 500_000,
//             prune_start_threshold: Some(100_000),
//             min_size_after_prune: Some(10_000),
//             ..SuffixConfig::default()
//         });

//         for i in 99_999..500_000 {
//             suffix.insert(i, create_mock_candidate_message(i)).unwrap();
//         }
//         suffix.update_decision(99999, 500_000 + 99_999).unwrap();
//         // suffix.update_decision(99999, 500_000 + 99_999).unwrap();

//         b.iter(|| {
//             suffix.insert(black_box(999_999), black_box(create_mock_candidate_message(999_999))).unwrap();
//         });
//     });
// }

fn bench_suffix_find_prune_index(c: &mut Criterion) {
    c.bench_function("Test finding the nearest suffix item to prune", |b| {
        let mut suffix = Suffix::with_config(SuffixConfig {
            capacity: 500_000,
            min_size_after_prune: Some(10_000),
            ..SuffixConfig::default()
        });

        (0..500_000).for_each(|i| {
            suffix.insert(i, create_mock_candidate_message_with_vers(i)).unwrap();
        });

        (4000..5000).for_each(|i| {
            suffix.messages[i] = None;
        });
        // b.iter(|| suffix.get(99_999));

        b.iter(|| suffix.find_prune_till_index(black_box(4_800)))
    });
}

// criterion_group!(benches, bench_suffix_find_prune_index);
criterion_group!(benches, bench_suffix_real, bench_suffix_find_prune_index);
criterion_main!(benches);
