//! Graph index metadata, registration and selection policy.

mod metadata;
mod policy;
mod registry;

pub use metadata::{
    CsrIndexHandle, GraphIndexKey, GraphIndexMetadata, IndexDirection, IndexReference,
};
pub use policy::{IndexDecision, IndexFallbackReason, IndexUsagePolicy};
pub use registry::{GraphIndexRegistry, InMemoryGraphIndexRegistry};
