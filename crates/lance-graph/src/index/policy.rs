use super::metadata::IndexReference;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexUsagePolicy {
    Disabled,
    Prefer,
    Require,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexFallbackReason {
    PolicyDisabled,
    IndexNotFound,
    UnsupportedDirection,
    MultipleRelationshipTypes,
    RelationshipVariableUsed,
    RelationshipPropertyRequired,
    TargetVariableReused,
    UnsupportedIdType,
    SchemaMismatch,
    StaleIndex,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexDecision {
    Use(IndexReference),
    Fallback(IndexFallbackReason),
}
