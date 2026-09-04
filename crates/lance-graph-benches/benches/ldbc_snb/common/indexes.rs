use super::config::{BenchmarkConfig, BenchmarkMode};
use super::dataset::DatasetFixture;
use arrow_schema::DataType;
use lance_graph::{
    CoveringAdjacencyIndexBuilder, CoveringAdjacencyMetadata, CsrIndexBuilder, CsrIndexHandle,
    CsrIndexStore, DirectAdjacencyIndexBuilder, DirectAdjacencyMetadata, GraphIndexKey,
    GraphIndexMetadata, InMemoryGraphIndexRegistry, IndexDirection,
    MultiTypeCoveringAdjacencyIndexBuilder, MultiTypeCoveringAdjacencyIndexStore,
    MultiTypeDirectAdjacencyIndexBuilder, MultiTypeDirectAdjacencyIndexStore,
};
use std::fs;
use std::path::Path;
use std::sync::Arc;

pub const DIRECT_INDEX_NAME: &str = "ldbc_snb_direct";
pub const COVERING_INDEX_NAME: &str = "ldbc_snb_covering";

pub async fn load_or_build_indexes(
    config: &BenchmarkConfig,
    datasets: &DatasetFixture,
) -> Result<Arc<InMemoryGraphIndexRegistry>, String> {
    let index_root = config.root.join("indexes");
    fs::create_dir_all(&index_root)
        .map_err(|error| format!("failed to create {}: {error}", index_root.display()))?;
    let registry = Arc::new(InMemoryGraphIndexRegistry::new());
    if config.modes.contains(&BenchmarkMode::Csr) {
        if config.rebuild_indexes {
            remove_index_dir(&index_root.join("csr"))?;
        }
        load_or_build_csr(&index_root, datasets, registry.as_ref()).await?;
    }
    if config.modes.contains(&BenchmarkMode::Direct) {
        if config.rebuild_indexes {
            remove_index_dir(&index_root.join("direct"))?;
        }
        load_or_build_direct(&index_root, datasets, registry.as_ref()).await?;
    }
    if config.modes.contains(&BenchmarkMode::Covering) {
        if config.rebuild_indexes {
            remove_index_dir(&index_root.join("covering"))?;
        }
        load_or_build_covering(&index_root, datasets, registry.as_ref()).await?;
    }
    Ok(registry)
}

fn remove_index_dir(path: &Path) -> Result<(), String> {
    if path.exists() {
        fs::remove_dir_all(path)
            .map_err(|error| format!("failed to remove {}: {error}", path.display()))?;
    }
    Ok(())
}

fn key() -> GraphIndexKey {
    GraphIndexKey::new("KNOWS", "Person", "Person", IndexDirection::Outgoing)
}

async fn load_or_build_csr(
    index_root: &Path,
    datasets: &DatasetFixture,
    registry: &InMemoryGraphIndexRegistry,
) -> Result<(), String> {
    let uri = index_root.join("csr").join("generation-1");
    let descriptor = if uri.join("manifest.json").is_file() {
        CsrIndexStore::read_descriptor(path_string(&uri)?)
            .await
            .map_err(|error| format!("failed to read CSR descriptor: {error}"))?
    } else {
        let index = CsrIndexBuilder::new()
            .with_num_vertices(datasets.manifest.person_count)
            .add_edges_from_batch(&datasets.knows_batch)
            .map_err(|error| format!("failed to add KNOWS edges to CSR: {error}"))?
            .try_build()
            .map_err(|error| format!("failed to build CSR: {error}"))?;
        let handle = CsrIndexHandle {
            index: Arc::new(index),
            metadata: GraphIndexMetadata {
                key: key(),
                source_id_field: "person_id".into(),
                target_id_field: "person_id".into(),
                id_data_type: DataType::Int64,
                num_vertices: datasets.manifest.person_count,
                num_edges: datasets.manifest.physical_knows_edges,
                source_uri: Some(datasets.knows.uri().to_string()),
                source_version: Some(datasets.knows.version().version),
                generation: 1,
            },
        };
        CsrIndexStore::write(path_string(&uri)?, &handle, Default::default())
            .await
            .map_err(|error| format!("failed to persist CSR: {error}"))?
    };
    CsrIndexStore::load_into_registry(&descriptor, Default::default(), registry)
        .await
        .map_err(|error| format!("failed to load CSR: {error}"))?;
    Ok(())
}

async fn load_or_build_direct(
    index_root: &Path,
    datasets: &DatasetFixture,
    registry: &InMemoryGraphIndexRegistry,
) -> Result<(), String> {
    let component_uri = index_root.join("direct").join("component-generation-1");
    let bundle_uri = index_root.join("direct").join("bundle-generation-1");
    let descriptor = if bundle_uri.join("bundle-descriptor.json").is_file() {
        MultiTypeDirectAdjacencyIndexStore::read_descriptor(path_string(&bundle_uri)?)
            .await
            .map_err(|error| format!("failed to read Direct bundle descriptor: {error}"))?
    } else {
        let metadata = DirectAdjacencyMetadata {
            key: key(),
            source_id_field: "src_id".into(),
            target_id_field: "person_id".into(),
            adjacency_field: "dst_ids".into(),
            id_data_type: DataType::Int64,
            num_sources: 0,
            num_edges: 0,
            dataset_uri: String::new(),
            dataset_version: 0,
            scalar_index_name: "src_id_btree".into(),
            source_uri: Some(datasets.knows.uri().to_string()),
            source_version: Some(datasets.knows.version().version),
            generation: 1,
        };
        let component = DirectAdjacencyIndexBuilder::new(metadata)
            .map_err(|error| format!("failed to create Direct builder: {error}"))?
            .add_edges_from_batch(&datasets.knows_batch)
            .map_err(|error| format!("failed to add KNOWS edges to Direct index: {error}"))?
            .build_and_persist(path_string(&component_uri)?, Default::default())
            .await
            .map_err(|error| format!("failed to persist Direct index: {error}"))?;
        MultiTypeDirectAdjacencyIndexBuilder::new(DIRECT_INDEX_NAME, 1)
            .map_err(|error| format!("failed to create Direct bundle builder: {error}"))?
            .add_component(component)
            .map_err(|error| format!("failed to add Direct component: {error}"))?
            .build_and_persist(path_string(&bundle_uri)?)
            .await
            .map_err(|error| format!("failed to persist Direct bundle: {error}"))?
    };
    let handle = MultiTypeDirectAdjacencyIndexStore::load(&descriptor, Default::default())
        .await
        .map_err(|error| format!("failed to load Direct bundle: {error}"))?;
    registry
        .register_direct_adjacency_bundle(handle)
        .map_err(|error| format!("failed to register Direct bundle: {error}"))?;
    Ok(())
}

async fn load_or_build_covering(
    index_root: &Path,
    datasets: &DatasetFixture,
    registry: &InMemoryGraphIndexRegistry,
) -> Result<(), String> {
    let component_uri = index_root.join("covering").join("component-generation-1");
    let bundle_uri = index_root.join("covering").join("bundle-generation-1");
    let descriptor = if bundle_uri.join("bundle-descriptor.json").is_file() {
        MultiTypeCoveringAdjacencyIndexStore::read_descriptor(path_string(&bundle_uri)?)
            .await
            .map_err(|error| format!("failed to read Covering bundle descriptor: {error}"))?
    } else {
        let metadata =
            CoveringAdjacencyMetadata::new(key(), "person_id", "person_id", DataType::Int64, 1)
                .with_source_identity(
                    datasets.knows.uri().to_string(),
                    Some(datasets.knows.version().version),
                );
        let component = CoveringAdjacencyIndexBuilder::new(metadata)
            .map_err(|error| format!("failed to create Covering builder: {error}"))?
            .build_sorted_batch_and_persist(
                &datasets.knows_batch,
                path_string(&component_uri)?,
                Default::default(),
            )
            .await
            .map_err(|error| format!("failed to persist Covering index: {error}"))?;
        MultiTypeCoveringAdjacencyIndexBuilder::new(COVERING_INDEX_NAME, 1)
            .map_err(|error| format!("failed to create Covering bundle builder: {error}"))?
            .add_component(component)
            .map_err(|error| format!("failed to add Covering component: {error}"))?
            .build_and_persist(path_string(&bundle_uri)?)
            .await
            .map_err(|error| format!("failed to persist Covering bundle: {error}"))?
    };
    let handle = MultiTypeCoveringAdjacencyIndexStore::load(&descriptor, Default::default())
        .await
        .map_err(|error| format!("failed to load Covering bundle: {error}"))?;
    registry
        .register_covering_adjacency_bundle(handle)
        .map_err(|error| format!("failed to register Covering bundle: {error}"))?;
    Ok(())
}

fn path_string(path: &Path) -> Result<&str, String> {
    path.to_str()
        .ok_or_else(|| format!("path is not valid UTF-8: {}", path.display()))
}
