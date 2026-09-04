use super::config::{BenchmarkConfig, BenchmarkMode};
use super::dataset::{open_datasets, DatasetFixture};
use super::indexes::{load_or_build_indexes, COVERING_INDEX_NAME, DIRECT_INDEX_NAME};
use arrow_array::RecordBatch;
use datafusion::datasource::{DefaultTableSource, TableProvider};
use datafusion::execution::context::SessionContext;
use lance::datafusion::LanceTableProvider;
use lance_graph::{
    CypherQuery, ExpandExecutionMode, GraphConfig, GraphSourceCatalog, InMemoryCatalog,
    InMemoryGraphIndexRegistry,
};
use std::sync::Arc;

pub struct LdbcSnbFixture {
    pub catalog: Arc<dyn GraphSourceCatalog>,
    pub join_context: SessionContext,
    pub indexed_context: SessionContext,
    pub indexes: Arc<InMemoryGraphIndexRegistry>,
    pub graph_config: GraphConfig,
    pub datasets: DatasetFixture,
}

impl LdbcSnbFixture {
    pub async fn open(config: &BenchmarkConfig) -> Result<Self, String> {
        let datasets = open_datasets(&config.root).await?;
        let indexes = load_or_build_indexes(config, &datasets).await?;
        let person_table: Arc<dyn TableProvider> = Arc::new(LanceTableProvider::new(
            datasets.person.clone(),
            false,
            false,
        ));
        let knows_table: Arc<dyn TableProvider> = Arc::new(LanceTableProvider::new(
            datasets.knows.clone(),
            false,
            false,
        ));
        let join_context = SessionContext::new();
        join_context
            .register_table("person", person_table.clone())
            .map_err(|error| format!("failed to register Person in Join context: {error}"))?;
        join_context
            .register_table("knows", knows_table.clone())
            .map_err(|error| format!("failed to register KNOWS in Join context: {error}"))?;
        let indexed_context = SessionContext::new();
        indexed_context
            .register_table("person", person_table.clone())
            .map_err(|error| format!("failed to register Person in indexed context: {error}"))?;
        indexed_context
            .register_table("knows", knows_table.clone())
            .map_err(|error| format!("failed to register KNOWS in indexed context: {error}"))?;
        let catalog = Arc::new(
            InMemoryCatalog::new()
                .with_node_source("Person", Arc::new(DefaultTableSource::new(person_table)))
                .with_relationship_source("KNOWS", Arc::new(DefaultTableSource::new(knows_table))),
        );
        let graph_config = GraphConfig::builder()
            .with_node_label("Person", "person_id")
            .with_relationship("KNOWS", "src_id", "dst_id")
            .build()
            .map_err(|error| format!("failed to build LDBC graph config: {error}"))?;
        Ok(Self {
            catalog,
            join_context,
            indexed_context,
            indexes,
            graph_config,
            datasets,
        })
    }

    pub async fn execute(
        &self,
        query: &CypherQuery,
        mode: BenchmarkMode,
    ) -> Result<RecordBatch, String> {
        match mode {
            BenchmarkMode::Join => {
                query
                    .execute_with_catalog_and_context(
                        self.catalog.clone(),
                        self.join_context.clone(),
                    )
                    .await
            }
            _ => {
                query
                    .execute_with_catalog_context_and_indexes(
                        self.catalog.clone(),
                        self.indexed_context.clone(),
                        self.indexes.clone(),
                        expand_mode(mode)?,
                    )
                    .await
            }
        }
        .map_err(|error| format!("{} execution failed: {error}", mode.name()))
    }

    pub async fn explain(
        &self,
        query: &CypherQuery,
        mode: BenchmarkMode,
    ) -> Result<String, String> {
        let context = if mode == BenchmarkMode::Join {
            self.join_context.clone()
        } else {
            self.indexed_context.clone()
        };
        query
            .explain_with_catalog_context_and_indexes(
                self.catalog.clone(),
                context,
                self.indexes.clone(),
                expand_mode(mode)?,
            )
            .await
            .map_err(|error| format!("{} explain failed: {error}", mode.name()))
    }
}

fn expand_mode(mode: BenchmarkMode) -> Result<ExpandExecutionMode, String> {
    match mode {
        BenchmarkMode::Join => Ok(ExpandExecutionMode::Join),
        BenchmarkMode::Csr => Ok(ExpandExecutionMode::Csr),
        BenchmarkMode::Direct => ExpandExecutionMode::direct_adjacency(DIRECT_INDEX_NAME)
            .map_err(|error| error.to_string()),
        BenchmarkMode::Covering => ExpandExecutionMode::covering_adjacency(COVERING_INDEX_NAME)
            .map_err(|error| error.to_string()),
    }
}
