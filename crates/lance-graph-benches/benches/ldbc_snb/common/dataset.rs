use arrow_array::RecordBatch;
use arrow_select::concat::concat_batches;
use futures::TryStreamExt;
use lance::dataset::Dataset;
use lance_index::scalar::{BuiltinIndexType, ScalarIndexParams};
use lance_index::{DatasetIndexExt, IndexType};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use std::sync::Arc;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DatasetManifest {
    pub dataset: String,
    pub scale_factor: String,
    pub person_count: u64,
    pub logical_knows_edges: u64,
    pub physical_knows_edges: u64,
    pub id_mapping: String,
    pub knows_direction: String,
    pub person_dataset_uri: String,
    pub person_dataset_version: u64,
    pub knows_dataset_uri: String,
    pub knows_dataset_version: u64,
}

pub struct DatasetFixture {
    pub manifest: DatasetManifest,
    pub person: Arc<Dataset>,
    pub knows: Arc<Dataset>,
    pub knows_batch: RecordBatch,
}

pub async fn open_datasets(root: &Path) -> Result<DatasetFixture, String> {
    let manifest_path = root.join("manifest.json");
    let bytes = fs::read(&manifest_path)
        .map_err(|error| format!("failed to read {}: {error}", manifest_path.display()))?;
    let manifest: DatasetManifest = serde_json::from_slice(&bytes)
        .map_err(|error| format!("failed to parse {}: {error}", manifest_path.display()))?;
    if manifest.id_mapping != "person_dense_0_based" {
        return Err(format!(
            "unsupported ID mapping {}; expected person_dense_0_based",
            manifest.id_mapping
        ));
    }
    if manifest.knows_direction != "bidirectional_materialized_as_outgoing" {
        return Err(format!(
            "unsupported KNOWS direction {}; expected bidirectional_materialized_as_outgoing",
            manifest.knows_direction
        ));
    }

    let mut person = Dataset::open(&manifest.person_dataset_uri)
        .await
        .map_err(|error| format!("failed to open Person dataset: {error}"))?;
    if person.version().version < manifest.person_dataset_version {
        return Err(format!(
            "Person dataset version is older than the manifest: manifest={}, current={}",
            manifest.person_dataset_version,
            person.version().version
        ));
    }
    ensure_person_index(&mut person).await?;
    let person = Arc::new(
        Dataset::open(&manifest.person_dataset_uri)
            .await
            .map_err(|error| format!("failed to reopen Person dataset: {error}"))?,
    );
    let knows = Arc::new(
        Dataset::open(&manifest.knows_dataset_uri)
            .await
            .map_err(|error| format!("failed to open KNOWS dataset: {error}"))?,
    );
    if knows.version().version != manifest.knows_dataset_version {
        return Err(format!(
            "KNOWS dataset version changed: manifest={}, current={}",
            manifest.knows_dataset_version,
            knows.version().version
        ));
    }
    let knows_batch = collect_knows(&knows).await?;
    if knows_batch.num_rows() as u64 != manifest.physical_knows_edges {
        return Err(format!(
            "KNOWS row count mismatch: manifest={}, current={}",
            manifest.physical_knows_edges,
            knows_batch.num_rows()
        ));
    }
    Ok(DatasetFixture {
        manifest,
        person,
        knows,
        knows_batch,
    })
}

async fn ensure_person_index(dataset: &mut Dataset) -> Result<(), String> {
    let indices = dataset
        .load_indices()
        .await
        .map_err(|error| format!("failed to list Person indexes: {error}"))?;
    if indices.iter().any(|index| index.name == "person_id_btree") {
        return Ok(());
    }
    dataset
        .create_index(
            &["person_id"],
            IndexType::BTree,
            Some("person_id_btree".into()),
            &ScalarIndexParams::for_builtin(BuiltinIndexType::BTree),
            false,
        )
        .await
        .map_err(|error| format!("failed to create Person.person_id BTree: {error}"))?;
    Ok(())
}

async fn collect_knows(dataset: &Dataset) -> Result<RecordBatch, String> {
    let mut scan = dataset.scan();
    scan.project(&["src_id", "dst_id"])
        .map_err(|error| format!("failed to project KNOWS IDs: {error}"))?;
    let batches = scan
        .try_into_stream()
        .await
        .map_err(|error| format!("failed to scan KNOWS: {error}"))?
        .try_collect::<Vec<_>>()
        .await
        .map_err(|error| format!("failed to collect KNOWS: {error}"))?;
    let schema = batches
        .first()
        .map(RecordBatch::schema)
        .ok_or_else(|| "KNOWS dataset is empty".to_string())?;
    concat_batches(&schema, &batches)
        .map_err(|error| format!("failed to concatenate KNOWS batches: {error}"))
}
