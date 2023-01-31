use thiserror::Error as ThisError;

type SuffixIndex = usize;
type SuffixVersion = u64;

#[derive(Debug, PartialEq, Eq, ThisError)]
pub enum SuffixError {
    #[error("Suffix error - No item found for version {0}")]
    ItemNotFound(SuffixVersion, Option<SuffixIndex>),
    #[error("Suffix error - calculating index {1} from head {0}")]
    IndexCalculationError(u64, SuffixVersion),
    #[error("Suffix error - converting version=({0}) to index")]
    VersionToIndexConversionError(u64),
    #[error("Suffix error - no prune version present in meta info")]
    PruneVersionNotFound,
}
