//! Graph index metadata, registration and selection policy.

mod metadata;
mod persistence;
mod policy;
mod registry;

pub use metadata::{
    CsrIndexHandle, GraphIndexKey, GraphIndexMetadata, IndexDirection, IndexReference,
};
pub use persistence::{
    CsrIndexLoadOptions, CsrIndexStore, CsrIndexWriteOptions, GraphSourceIdentity,
    IndexSourceValidation, PersistedCsrIndexDescriptor, CSR_INDEX_FORMAT_VERSION,
};
pub use policy::{IndexDecision, IndexFallbackReason, IndexUsagePolicy};
pub use registry::{GraphIndexRegistry, InMemoryGraphIndexRegistry};
