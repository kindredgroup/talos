pub mod env;
pub mod otel;
pub mod sync;

pub trait ResetVariantTrait {
    const RESET_VARIANT: Self;
    fn get_reset_variant() -> Self
    where
        Self: Sized,
    {
        Self::RESET_VARIANT
    }
}
