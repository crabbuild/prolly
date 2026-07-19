pub(crate) mod execution;
#[expect(
    dead_code,
    reason = "the ready-only bridge is consumed when ProllyEngine replaces facade-local reads"
)]
pub(crate) mod ready;
pub(crate) mod validation;
