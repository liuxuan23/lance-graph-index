//! Graph index metadata, registration and selection policy.

mod direct_adjacency;
mod metadata;
mod persistence;
mod registry;
mod selection;

pub use direct_adjacency::{
    DirectAdjacencyComponentDescriptorRef, DirectAdjacencyIndexBuilder, DirectAdjacencyIndexStore,
    DirectAdjacencyLoadOptions, DirectAdjacencyWriteOptions, MultiTypeDirectAdjacencyIndexBuilder,
    MultiTypeDirectAdjacencyIndexStore, MultiTypeDirectAdjacencyLoadOptions,
    MultiTypeSourceValidation, PersistedDirectAdjacencyDescriptor,
    PersistedMultiTypeDirectAdjacencyDescriptor, DIRECT_ADJACENCY_INDEX_FORMAT_VERSION,
    MULTI_TYPE_DIRECT_ADJACENCY_INDEX_FORMAT_VERSION,
};
pub use metadata::{
    CsrIndexHandle, DirectAdjacencyIndexHandle, DirectAdjacencyMetadata, DirectAdjacencyReference,
    GraphIndexKey, GraphIndexMetadata, IndexDirection, IndexReference,
    MultiTypeDirectAdjacencyIndexHandle, MultiTypeDirectAdjacencyMetadata,
};
pub use persistence::{
    CsrIndexLoadOptions, CsrIndexStore, CsrIndexWriteOptions, GraphSourceIdentity,
    IndexSourceValidation, PersistedCsrIndexDescriptor, CSR_INDEX_FORMAT_VERSION,
};
pub use registry::{GraphIndexRegistry, InMemoryGraphIndexRegistry};
pub use selection::{ExpandExecutionMode, ExpandIndexReference, ExpandPlanDecision};
