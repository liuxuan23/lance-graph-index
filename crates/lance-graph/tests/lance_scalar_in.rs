// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;

use arrow_array::{
    builder::{Int64Builder, ListBuilder},
    Array, Int64Array, ListArray, RecordBatch, RecordBatchIterator, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{col, lit};
use datafusion::physical_plan::displayable;
use lance::dataset::{Dataset, WriteParams};
use lance::index::DatasetIndexInternalExt;
use lance_index::metrics::NoOpMetricsCollector;
use lance_index::scalar::{BuiltinIndexType, SargableQuery, ScalarIndexParams, SearchResult};
use lance_index::{DatasetIndexExt, IndexType};

#[tokio::test]
async fn in_list_uses_lance_scalar_index() {
    let temp = tempfile::tempdir().unwrap();
    let uri = temp.path().join("person.lance");
    let schema = Arc::new(Schema::new(vec![
        Field::new("person_id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(0..1_000_000)),
            Arc::new(StringArray::from_iter_values(
                (0..1_000_000).map(|id| format!("person_{id}")),
            )),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    let mut dataset = Dataset::write(reader, uri.to_str().unwrap(), Some(WriteParams::default()))
        .await
        .unwrap();
    let params = ScalarIndexParams::for_builtin(BuiltinIndexType::BTree);
    dataset
        .create_index(
            &["person_id"],
            IndexType::BTree,
            Some("person_id_btree".into()),
            &params,
            false,
        )
        .await
        .unwrap();

    let mut scanner = dataset.scan();
    scanner.project(&["person_id", "name"]).unwrap();
    scanner
        .filter_expr(col("person_id").in_list(vec![lit(43_i64), lit(44_i64), lit(45_i64)], false));
    let plan = scanner.create_plan().await.unwrap();
    let plan_text = displayable(plan.as_ref()).indent(true).to_string();
    assert!(
        plan_text.contains("ScalarIndexQuery"),
        "expected scalar index plan, got:\n{plan_text}"
    );

    let result = scanner.try_into_batch().await.unwrap();
    assert_eq!(result.num_rows(), 3);
    let ids = result
        .column_by_name("person_id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.values(), &[43, 44, 45]);

    let mut scanner = dataset.scan();
    scanner.project(&["person_id", "name"]).unwrap();
    scanner.filter_expr(
        col("person_id")
            .in_list(vec![lit(43_i64), lit(43_i64), lit(44_i64)], false)
            .and(col("name").eq(lit("person_44"))),
    );
    let plan = scanner.create_plan().await.unwrap();
    let plan_text = displayable(plan.as_ref()).indent(true).to_string();
    assert!(
        plan_text.contains("ScalarIndexQuery"),
        "combined target predicate lost scalar index path:\n{plan_text}"
    );
    let result = scanner.try_into_batch().await.unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(
        result
            .column_by_name("person_id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        44
    );
}

#[tokio::test]
async fn nested_adjacency_round_trips_through_scalar_index_and_take_rows() {
    let temp = tempfile::tempdir().unwrap();
    let uri = temp.path().join("adjacency.lance");
    let item = Arc::new(Field::new("item", DataType::Int64, true));
    let schema = Arc::new(Schema::new(vec![
        Field::new("src_id", DataType::Int64, false),
        Field::new("dst_ids", DataType::List(item), false),
    ]));

    let batch = |sources: Vec<i64>, neighbors: Vec<Vec<i64>>| {
        let mut lists = ListBuilder::new(Int64Builder::new());
        for values in neighbors {
            lists.values().append_slice(&values);
            lists.append(true);
        }
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(sources)),
                Arc::new(lists.finish()),
            ],
        )
        .unwrap()
    };
    let batches = vec![
        Ok(batch(vec![1, 2], vec![vec![7, 7, 9], vec![]])),
        Ok(batch(vec![3, 4], vec![vec![11], vec![13, 17]])),
    ];
    let reader = RecordBatchIterator::new(batches, schema);
    let mut dataset = Dataset::write(reader, uri.to_str().unwrap(), Some(WriteParams::default()))
        .await
        .unwrap();
    dataset
        .create_index(
            &["src_id"],
            IndexType::BTree,
            Some("src_id_btree".into()),
            &ScalarIndexParams::for_builtin(BuiltinIndexType::BTree),
            false,
        )
        .await
        .unwrap();

    let indices = dataset.load_indices().await.unwrap();
    let index = indices
        .iter()
        .find(|index| index.name == "src_id_btree")
        .unwrap();
    let scalar_index = dataset
        .open_scalar_index("src_id", &index.uuid.to_string(), &NoOpMetricsCollector)
        .await
        .unwrap();
    let result = scalar_index
        .search(
            &SargableQuery::IsIn(vec![
                ScalarValue::Int64(Some(1)),
                ScalarValue::Int64(Some(2)),
                ScalarValue::Int64(Some(4)),
            ]),
            &NoOpMetricsCollector,
        )
        .await
        .unwrap();
    let row_ids = match result {
        SearchResult::Exact(row_ids) | SearchResult::AtMost(row_ids) => row_ids,
        SearchResult::AtLeast(_) => panic!("BTree lookup returned an incomplete result"),
    }
    .row_ids()
    .unwrap()
    .map(u64::from)
    .collect::<Vec<_>>();

    let fetched = dataset
        .take_rows(&row_ids, dataset.schema().clone())
        .await
        .unwrap();
    let sources = fetched
        .column_by_name("src_id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let lists = fetched
        .column_by_name("dst_ids")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let mut rows = (0..fetched.num_rows())
        .map(|row| {
            let values = lists.value(row);
            let values = values.as_any().downcast_ref::<Int64Array>().unwrap();
            (sources.value(row), values.values().to_vec())
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(|(source, _)| *source);
    assert_eq!(
        rows,
        vec![(1, vec![7, 7, 9]), (2, vec![]), (4, vec![13, 17])]
    );
}
