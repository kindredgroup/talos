use certifier::{certifier::Certifier, certifier_candidate::CertifierCandidate, core::convert_vec_to_hashmap};
use criterion::{criterion_group, criterion_main, Criterion};

fn bench_certifier_rule_1(c: &mut Criterion) {
    c.bench_function("[CERTIFIER - Rule 1] Unconditional Commit", |b| {
        let mut certifier = Certifier::new();
        let test_reads = convert_vec_to_hashmap(vec![("g:1", 0), ("g:2", 0), ("g:3", 2), ("l:2", 4)]);
        let test_writes = convert_vec_to_hashmap(vec![("g:1", 0), ("g:2", 3)]);

        Certifier::update_set(&mut certifier.reads, test_reads);
        Certifier::update_set(&mut certifier.writes, test_writes);

        b.iter(|| {
            let certify_tx = CertifierCandidate {
                vers: 0,
                snapshot: 0,
                readvers: vec![],
                readset: vec![],
                writeset: vec![],
            };
            certifier.certify_transaction(0, certify_tx);
        })
    });
}

fn bench_certifier_rule_2(c: &mut Criterion) {
    c.bench_function("[CERTIFIER - Rule 2] Conditional Abort", |b| {
        let mut certifier = Certifier::new();
        let test_reads = convert_vec_to_hashmap(vec![
            ("a1", 5),
            ("a2", 6),
            ("a3", 10),
            ("b2", 2),
            ("c1", 22),
            ("c2", 12),
            ("c3", 13),
            ("d2", 23),
            ("e1", 9),
            ("e2", 10),
            ("e3", 12),
            ("l2", 14),
            ("g1", 30),
            ("g2", 33),
            ("g3", 23),
            ("l2", 43),
        ]);
        let test_writes = convert_vec_to_hashmap(vec![
            ("k1", 9),
            ("k2", 16),
            ("k3", 10),
            ("l2", 21),
            ("r1", 20),
            ("r2", 12),
            ("e43", 12),
            ("lf2", 14),
            ("gd1", 37),
            ("gds2", 43),
            ("g23", 44),
            ("lsd2", 49),
        ]);

        Certifier::update_set(&mut certifier.reads, test_reads);
        Certifier::update_set(&mut certifier.writes, test_writes);

        b.iter(|| {
            let certify_tx = CertifierCandidate {
                vers: 52,
                snapshot: 6,
                readvers: vec![2, 3],
                readset: vec!["g1".to_owned()],
                writeset: vec!["l1".to_owned()],
            };
            certifier.certify_transaction(9, certify_tx);
        })
    });
}

fn bench_certifier_rule_3(c: &mut Criterion) {
    c.bench_function("[CERTIFIER - Rule 3] Unconditional Abort (anti-dependency)", |b| {
        let mut certifier = Certifier::new();
        let test_reads = convert_vec_to_hashmap(vec![
            ("a1", 5),
            ("a2", 6),
            ("a3", 10),
            ("b2", 2),
            ("c1", 22),
            ("c2", 12),
            ("c3", 13),
            ("d2", 23),
            ("e1", 9),
            ("e2", 10),
            ("e3", 12),
            ("l2", 14),
            ("g1", 30),
            ("g2", 33),
            ("g3", 23),
            ("l2", 43),
        ]);
        let test_writes = convert_vec_to_hashmap(vec![
            ("k1", 9),
            ("k2", 16),
            ("k3", 10),
            ("l2", 21),
            ("r1", 20),
            ("r2", 12),
            ("e43", 32),
            ("lf2", 14),
            ("gd1", 37),
            ("gds2", 43),
            ("g23", 44),
            ("lsd2", 49),
        ]);
        Certifier::update_set(&mut certifier.reads, test_reads);
        Certifier::update_set(&mut certifier.writes, test_writes);

        b.iter(|| {
            let certify_tx = CertifierCandidate {
                vers: 55,
                snapshot: 20,
                readvers: vec![10, 29],
                readset: vec!["r1".to_owned(), "e43".to_owned()],
                writeset: vec!["l1".to_owned()],
            };
            certifier.certify_transaction(18, certify_tx);
        })
    });
}

fn bench_certifier_rule_4(c: &mut Criterion) {
    c.bench_function("[CERTIFIER - Rule 4] Conditional Commit", |b| {
        let mut certifier = Certifier::new();
        let test_reads = convert_vec_to_hashmap(vec![
            ("a1", 5),
            ("a2", 6),
            ("a3", 10),
            ("b2", 2),
            ("c1", 22),
            ("c2", 12),
            ("c3", 13),
            ("d2", 23),
            ("e1", 9),
            ("e2", 10),
            ("e3", 12),
            ("l2", 14),
            ("g1", 30),
            ("g2", 33),
            ("g3", 23),
            ("l2", 43),
        ]);
        let test_writes = convert_vec_to_hashmap(vec![
            ("k1", 9),
            ("k2", 16),
            ("k3", 10),
            ("l2", 21),
            ("r1", 20),
            ("l1", 32),
            ("e43", 19),
            ("lf2", 14),
            ("gd1", 37),
            ("gds2", 43),
            ("g23", 44),
            ("lsd2", 49),
        ]);
        Certifier::update_set(&mut certifier.reads, test_reads);
        Certifier::update_set(&mut certifier.writes, test_writes);

        b.iter(|| {
            let certify_tx = CertifierCandidate {
                vers: 55,
                snapshot: 20,
                readvers: vec![10, 29],
                readset: vec!["r1".to_owned(), "e43".to_owned()],
                writeset: vec!["l1".to_owned()],
            };
            certifier.certify_transaction(18, certify_tx);
        })
    });
}

criterion_group!(
    benches,
    bench_certifier_rule_1,
    bench_certifier_rule_2,
    bench_certifier_rule_3,
    bench_certifier_rule_4
);
criterion_main!(benches);
