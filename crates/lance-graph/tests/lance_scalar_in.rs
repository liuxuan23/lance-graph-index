// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::logical_expr::{col, lit};
use datafusion::physical_plan::displayable;
use lance::dataset::{Dataset, WriteParams};
use lance_index::scalar::{BuiltinIndexType, ScalarIndexParams};
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
