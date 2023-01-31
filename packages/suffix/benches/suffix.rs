use criterion::{criterion_group, criterion_main, Criterion};
use suffix::core::SuffixTrait;
use suffix::Suffix;

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

fn bench_suffix_real(c: &mut Criterion) {
    c.bench_function("[SUFFIX REAL] insert 500,000 items", |b| {
        let mut suffix = Suffix::<MockSuffixItemMessage>::new(500_000);

        b.iter(|| {
            for i in 0..suffix.messages.len().try_into().unwrap() {
                suffix.insert(i, create_mock_candidate_message(i)).unwrap();
            }
        })
    });
    c.bench_function("[SUFFIX REAL] insert an item in a vec of capacity 500,000", |b| {
        let mut suffix = Suffix::new(500_000);
        b.iter(|| {
            suffix.insert(30, create_mock_candidate_message(30)).unwrap();
        })
    });
    c.bench_function("[SUFFIX REAL] get version=99999 item from 500,000 items", |b| {
        let mut suffix = Suffix::new(500_000);

        for i in 0..suffix.messages.len().try_into().unwrap() {
            suffix.insert(i, create_mock_candidate_message(i)).unwrap();
        }
        b.iter(|| suffix.get(99999))
    });
    c.bench_function("[SUFFIX REAL] prune till version=99999 item from 500,000 items", |b| {
        let mut suffix = Suffix::new(500_000);

        for i in 99_999..500_000 {
            suffix.insert(i, create_mock_candidate_message(i)).unwrap();
        }
        suffix.update_decision(99999, 500_000 + 99_999).unwrap();
        // suffix.update_decision(99999, 500_000 + 99_999).unwrap();

        b.iter(|| suffix.prune())
    });
}

criterion_group!(benches, bench_suffix_real,);
criterion_main!(benches);
