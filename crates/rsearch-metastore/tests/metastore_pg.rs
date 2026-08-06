//! Integration tests against a real Postgres. Ignored by default; run with:
//!   RSEARCH_TEST_DATABASE_URL=postgres://rsearch:rsearch@localhost:5433/rsearch \
//!     cargo test -p rsearch-metastore -- --ignored

use rsearch_common::config::MetastoreConfig;
use rsearch_metastore::{Metastore, MetastoreError, SplitState};

async fn metastore() -> Option<Metastore> {
    let url = std::env::var("RSEARCH_TEST_DATABASE_URL").ok()?;
    let cfg = MetastoreConfig {
        database_url: url,
        max_connections: 4,
    };
    Some(Metastore::connect(&cfg).await.unwrap())
}

fn unique(prefix: &str) -> String {
    format!("{prefix}-{}", uuid::Uuid::new_v4().simple())
}

#[tokio::test]
#[ignore = "requires Postgres (set RSEARCH_TEST_DATABASE_URL)"]
async fn stream_crud_and_mapping() {
    let Some(ms) = metastore().await else { return };
    let name = unique("stream");

    let stream = ms.ensure_stream(&name).await.unwrap();
    assert_eq!(stream.name, name);
    // ensure is idempotent and returns the same row.
    let again = ms.ensure_stream(&name).await.unwrap();
    assert_eq!(stream.id, again.id);

    let mapping = serde_json::json!({"properties": {"service": {"type": "keyword"}}});
    ms.update_stream_mapping(&name, &mapping).await.unwrap();
    assert_eq!(ms.get_stream(&name).await.unwrap().mapping, mapping);

    ms.set_stream_retention(&name, Some(24)).await.unwrap();
    assert_eq!(ms.get_stream(&name).await.unwrap().retention_hours, Some(24));

    ms.delete_stream(&name).await.unwrap();
    assert!(matches!(
        ms.get_stream(&name).await,
        Err(MetastoreError::StreamNotFound(_))
    ));
}

#[tokio::test]
#[ignore = "requires Postgres (set RSEARCH_TEST_DATABASE_URL)"]
async fn split_state_machine() {
    let Some(ms) = metastore().await else { return };
    let stream = ms.ensure_stream(&unique("splits")).await.unwrap();
    let split_id = unique("split");

    ms.stage_split(
        &split_id,
        stream.id,
        &format!("streams/{}/{}.split", stream.name, split_id),
        1000,
        4096,
        1_753_300_000_000,
        1_753_300_060_000,
        512,
        Some("node-a"),
    )
    .await
    .unwrap();

    // Staged splits are invisible to queries.
    assert!(
        ms.splits_for_query(stream.id, None, None)
            .await
            .unwrap()
            .is_empty()
    );

    ms.publish_split(&split_id).await.unwrap();
    // Double publish is a state conflict.
    assert!(matches!(
        ms.publish_split(&split_id).await,
        Err(MetastoreError::SplitStateConflict(_))
    ));

    // Time-range pruning: overlapping window finds it, disjoint misses it.
    let hits = ms
        .splits_for_query(stream.id, Some(1_753_300_030_000), None)
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].split_id, split_id);
    assert_eq!(hits[0].state(), SplitState::Published);
    assert!(
        ms.splits_for_query(stream.id, Some(1_753_300_060_001), None)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        ms.splits_for_query(stream.id, None, Some(1_753_299_999_999))
            .await
            .unwrap()
            .is_empty()
    );

    let marked = ms
        .mark_splits_for_delete(&[split_id.clone()])
        .await
        .unwrap();
    assert_eq!(marked, 1);
    // Idempotent.
    assert_eq!(
        ms.mark_splits_for_delete(&[split_id.clone()]).await.unwrap(),
        0
    );
    assert!(
        ms.splits_for_query(stream.id, None, None)
            .await
            .unwrap()
            .is_empty()
    );

    ms.delete_split_row(&split_id).await.unwrap();
    assert!(ms.get_split(&split_id).await.unwrap().is_none());
    ms.delete_stream(&stream.name).await.unwrap();
}

#[tokio::test]
#[ignore = "requires Postgres (set RSEARCH_TEST_DATABASE_URL)"]
async fn node_heartbeats() {
    let Some(ms) = metastore().await else { return };
    let node_id = unique("node");
    let roles = vec!["ingest".to_string(), "search".to_string()];

    ms.heartbeat(&node_id, &roles, Some("10.0.0.1:9200"))
        .await
        .unwrap();
    let live = ms.live_nodes(30.0).await.unwrap();
    let me = live.iter().find(|n| n.id == node_id).unwrap();
    assert_eq!(me.roles, roles);
    assert!(me.heartbeat_age_secs < 5.0);

    // A node that "last heartbeated an hour ago" is not live.
    sqlx::query("UPDATE nodes SET last_heartbeat = now() - interval '1 hour' WHERE id = $1")
        .bind(&node_id)
        .execute(ms.pool())
        .await
        .unwrap();
    assert!(!ms.live_nodes(30.0).await.unwrap().iter().any(|n| n.id == node_id));

    let expired = ms.expire_dead_nodes(60.0).await.unwrap();
    assert!(expired >= 1);
}

#[tokio::test]
#[ignore = "requires Postgres (set RSEARCH_TEST_DATABASE_URL)"]
async fn draining_flag_tracks_start_time() {
    let Some(ms) = metastore().await else { return };
    let node_id = unique("drain");
    ms.heartbeat(&node_id, &["data".to_string()], Some("10.0.0.2:9200"))
        .await
        .unwrap();

    let me = |nodes: Vec<rsearch_metastore::NodeRecord>| {
        nodes.into_iter().find(|n| n.id == node_id).unwrap()
    };
    assert_eq!(me(ms.list_nodes().await.unwrap()).draining_since_secs, None);

    assert!(ms.set_node_draining(&node_id, true).await.unwrap());
    let node = me(ms.list_nodes().await.unwrap());
    assert!(node.draining);
    let age = node.draining_since_secs.expect("stamped on drain");
    assert!(age < 5.0);

    // Backdate the drain start; a repeated drain request must NOT reset it.
    sqlx::query("UPDATE nodes SET draining_since = now() - interval '2 hours' WHERE id = $1")
        .bind(&node_id)
        .execute(ms.pool())
        .await
        .unwrap();
    assert!(ms.set_node_draining(&node_id, true).await.unwrap());
    let age = me(ms.list_nodes().await.unwrap())
        .draining_since_secs
        .unwrap();
    assert!(age > 7000.0, "repeated drain reset draining_since (age {age})");

    // Undrain clears the timestamp.
    assert!(ms.set_node_draining(&node_id, false).await.unwrap());
    let node = me(ms.list_nodes().await.unwrap());
    assert!(!node.draining);
    assert_eq!(node.draining_since_secs, None);

    // Unknown node is reported, not silently ignored.
    assert!(!ms.set_node_draining(&unique("ghost"), true).await.unwrap());

    sqlx::query("DELETE FROM nodes WHERE id = $1")
        .bind(&node_id)
        .execute(ms.pool())
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires Postgres (set RSEARCH_TEST_DATABASE_URL)"]
async fn replication_targets_draining_last_resort() {
    let Some(ms) = metastore().await else { return };
    let normal = unique("rt-normal");
    let draining = unique("rt-draining");
    for id in [&normal, &draining] {
        ms.heartbeat(id, &["data".to_string()], Some("10.0.0.3:9200"))
            .await
            .unwrap();
    }
    ms.set_node_draining(&draining, true).await.unwrap();

    let ours = |nodes: Vec<rsearch_metastore::NodeRecord>| -> Vec<String> {
        nodes
            .into_iter()
            .map(|n| n.id)
            .filter(|id| id == &normal || id == &draining)
            .collect()
    };

    // Default: draining nodes are never write/repair targets.
    let targets = ours(ms.replication_targets(30.0, &[], 100, false).await.unwrap());
    assert!(targets.contains(&normal));
    assert!(!targets.contains(&draining));

    // Last resort: draining nodes are eligible, ranked after the rest.
    let targets = ours(ms.replication_targets(30.0, &[], 100, true).await.unwrap());
    assert!(targets.contains(&draining));

    for id in [&normal, &draining] {
        sqlx::query("DELETE FROM nodes WHERE id = $1")
            .bind(id)
            .execute(ms.pool())
            .await
            .unwrap();
    }
}
