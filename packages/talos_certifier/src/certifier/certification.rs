use std::time::Instant;

use ahash::AHashMap;

use super::CertifierCandidate;

#[derive(Debug, PartialEq, Eq)]
pub enum Discord {
    Permissive,
    Assertive,
}

#[derive(Debug, PartialEq, Eq)]
pub enum CertifyOutcome {
    Commited { discord: Discord },
    Aborted { version: Option<u64>, discord: Discord },
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct OutcomeMetric {
    pub certify_time: u64,
    pub safepoint_calc_time: u64,
    pub update_hashmap_time: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    Commited {
        discord: Discord,
        safepoint: u64,
        metrics: OutcomeMetric,
    },
    Aborted {
        version: Option<u64>,
        discord: Discord,
        metrics: OutcomeMetric,
    },
}

impl Outcome {
    pub fn update_metrics(self, incoming_metrics: OutcomeMetric) -> Outcome {
        let outcome = match self {
            Outcome::Commited {
                discord,
                safepoint,
                metrics: _,
            } => Outcome::Commited {
                discord,
                safepoint,
                metrics: incoming_metrics,
            },
            Outcome::Aborted { version, discord, metrics: _ } => Outcome::Aborted {
                version,
                discord,
                metrics: incoming_metrics,
            },
        };

        outcome
    }
    pub fn get_metrics(&self) -> &OutcomeMetric {
        match self {
            Outcome::Commited { discord, safepoint, metrics } => metrics,
            Outcome::Aborted { version, discord, metrics } => metrics,
        }
    }
}

pub type CertifierReadset = AHashMap<String, u64>;
pub type CertifierWriteset = AHashMap<String, u64>;
pub struct Certifier {
    pub reads: CertifierReadset,
    pub writes: CertifierWriteset,
}

/**
 *
 * R1. Commit write-only transactions. I.e., if readset(Tk) = ∅, then commit Tk.
 * R2. Otherwise, if Tk’s snapshot version falls short of the suffix boundary by more than one transaction, then try to abort it. I.e., if snapshot(Tk) < i – 1, then try to abort Tk.
 * R3. Otherwise, if there exists an earlier transaction in the suffix that overlaps with Tk, is not among Tk's read versions, and Tk has an antidependency directed upon it, then abort Tk. I.e., if ∃ Tj : snapshot(Tk) < j < k ∧ j ∉ readvers(Tk) ∧ writeset(Tj) ∩ readset(Tk) ≠ ∅, then abort Tk.
 * R4. Otherwise, try to commit Tk.
 */

impl Certifier {
    pub fn new() -> Certifier {
        Certifier {
            reads: AHashMap::default(),
            writes: AHashMap::default(),
        }
    }

    pub fn get_certifier_base_ver(suffix_head: u64, txn_vers: u64) -> u64 {
        let base = if suffix_head != 0 { suffix_head } else { txn_vers };
        base - 1
    }

    pub fn certify_transaction(&mut self, suffix_head: u64, certify_tx: CertifierCandidate) -> Outcome {
        let time_certify = Instant::now();
        let certification_outcome = self.certify(suffix_head, &certify_tx);
        let time_certify = time_certify.elapsed().as_nanos();

        let time_safepoint = Instant::now();
        let outcome = match certification_outcome {
            CertifyOutcome::Commited { discord } => {
                let safepoint = self.calculate_safe_point(suffix_head, &certify_tx);
                Outcome::Commited {
                    discord,
                    safepoint,
                    metrics: OutcomeMetric::default(),
                }
            }
            CertifyOutcome::Aborted { version, discord } => Outcome::Aborted {
                version,
                discord,
                metrics: OutcomeMetric::default(),
            },
        };
        let time_safepoint = time_safepoint.elapsed().as_nanos();

        let time_update_hashmap = Instant::now();
        Certifier::update_set(&mut self.reads, certify_tx.convert_readset_to_collection());
        Certifier::update_set(&mut self.writes, certify_tx.convert_writeset_to_collection());
        let time_update_hashmap = time_update_hashmap.elapsed().as_nanos();

        let outcome = outcome.update_metrics(OutcomeMetric {
            certify_time: time_certify as u64,
            safepoint_calc_time: time_safepoint as u64,
            update_hashmap_time: time_update_hashmap as u64,
        });
        outcome
    }

    pub(crate) fn certify(&self, suffix_head: u64, certify_tx: &CertifierCandidate) -> CertifyOutcome {
        // Rule R0: Conditional abort transactions with version 0.
        if certify_tx.vers == 0 {
            return CertifyOutcome::Aborted {
                version: None,
                discord: Discord::Permissive,
            };
        }

        // Rule R1: Unconditional commit write-only transactions.
        if certify_tx.readset.is_empty() {
            return CertifyOutcome::Commited { discord: Discord::Assertive };
        }

        // Rule R2: Conditional abort transactions below suffix boundary
        if certify_tx.is_version_above_snapshot(Certifier::get_certifier_base_ver(suffix_head, certify_tx.vers)) {
            return CertifyOutcome::Aborted {
                version: None,
                discord: Discord::Permissive,
            };
        }

        // Rule R3: Unconditional abort on antidependency
        if let Some(&antidependecy_vers) = certify_tx.readset.iter().find_map(|rs| {
            let w = self.writes.get(rs)?;

            if certify_tx.is_version_above_snapshot(*w) && !certify_tx.readvers.contains(w) {
                return Some(w);
            }
            None
        }) {
            return CertifyOutcome::Aborted {
                version: Some(antidependecy_vers),
                discord: Discord::Assertive,
            };
        }

        // Rule R4: Unconditional Commit
        CertifyOutcome::Commited { discord: Discord::Permissive }
    }

    pub fn calculate_safe_point(&self, suffix_head: u64, certify_tx: &CertifierCandidate) -> u64 {
        let mut safepoint = Certifier::get_certifier_base_ver(suffix_head, certify_tx.vers);

        certify_tx.readset.iter().for_each(|r| {
            // Safepoint calculation for read-read
            if let Some(&read) = self.reads.get(r) {
                if read > safepoint {
                    safepoint = read;
                }
            }

            // Safepoint calculation for write-read
            if let Some(&write) = self.writes.get(r) {
                // safepoint for write-read only when there is no antidependecy. Therefore safe to assume safepoint can be calculated for all WR relation.
                if write > safepoint {
                    safepoint = write;
                }

                // if let (false, true) = (
                //     ceritfy_tx.is_version_above_snapshot(write) && !ceritfy_tx.readvers.contains(&write), //anti-dependecy
                //     write > safepoint,
                // ) {
                //     safepoint = write;
                // }
            }
        });

        certify_tx.writeset.iter().for_each(|w| {
            // Safe point calculation for read-write
            if let Some(&read) = self.reads.get(w) {
                if read > safepoint {
                    safepoint = read;
                }
            }
            // Safe point calculation for write-write
            if let Some(&write) = self.writes.get(w) {
                if write > safepoint {
                    safepoint = write;
                }
            }
        });

        safepoint
    }

    /// Inserts if key is not present else updates the read or write Hashmap of the certifier
    pub fn update_set(self_items: &mut AHashMap<String, u64>, add_items: AHashMap<String, u64>) {
        self_items.extend(add_items);
    }

    /// Prune items from the read or write hash map
    ///
    /// Removes if key is found and the version < version in the certifier.
    pub fn prune_set(self_items: &mut AHashMap<String, u64>, remove_items: &AHashMap<String, u64>) {
        self_items.retain(|k, v| match remove_items.get(k) {
            Some(read_value) => read_value > v,
            None => true,
        });
    }
}

impl Default for Certifier {
    fn default() -> Self {
        Self::new()
    }
}
