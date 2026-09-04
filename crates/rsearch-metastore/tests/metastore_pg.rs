//! Integration tests against a real Postgres. Ignored by default; run with:
//!   RSEARCH_TEST_DATABASE_URL=postgres://rsearch:rsearch@localhost:5433/rsearch \
//!     cargo test -p rsearch-metastore -- --ignored

use rsearch_common::config::MetastoreConfig;
use rsearch_metastore::{Metastore, MetastoreError, NewSplit, SplitState, StreamMode};

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
async fn stream_mode_is_fixed_once_data_exists() {
    let Some(ms) = metastore().await else { return };
    let name = unique("mode");

    // Auto-created streams are log mode; an empty stream may switch.
    let stream = ms.ensure_stream(&name).await.unwrap();
    assert_eq!(stream.mode(), StreamMode::Log);
    ms.set_stream_mode(&name, StreamMode::Document).await.unwrap();
    assert!(ms.get_stream(&name).await.unwrap().is_document_mode());
    // ensure_stream_with_mode never flips an existing stream.
    let again = ms.ensure_stream_with_mode(&name, StreamMode::Log).await.unwrap();
    assert_eq!(again.mode(), StreamMode::Document);

    // Once a split exists the mode is fixed.
    let split_id = unique("split");
    ms.stage_split(&NewSplit {
        split_id: &split_id,
        stream_id: stream.id,
        storage_key: "k",
        doc_count: 1,
        size_bytes: 1,
        time_start_millis: 0,
        time_end_millis: 0,
        footer_len: 0,
        created_by: None,
        seq_min: None,
        seq_max: None,
        tombstone_seq_applied: 0,
        schema_version: 0,
        dynamic_fields: None,
    })
    .await
    .unwrap();
    assert!(matches!(
        ms.set_stream_mode(&name, StreamMode::Log).await,
        Err(MetastoreError::StreamModeFixed(_))
    ));
    assert!(matches!(
        ms.set_stream_mode(&unique("missing"), StreamMode::Log).await,
        Err(MetastoreError::StreamNotFound(_))
    ));
    ms.delete_stream(&name).await.unwrap();
}

#[tokio::test]
#[ignore = "requires Postgres (set RSEARCH_TEST_DATABASE_URL)"]
async fn split_state_machine() {
    let Some(ms) = metastore().await else { return };
    let stream = ms.ensure_stream(&unique("splits")).await.unwrap();
    let split_id = unique("split");

    ms.stage_split(&NewSplit {
        split_id: &split_id,
        stream_id: stream.id,
        storage_key: &format!("streams/{}/{}.split", stream.name, split_id),
        doc_count: 1000,
        size_bytes: 4096,
        time_start_millis: 1_753_300_000_000,
        time_end_millis: 1_753_300_060_000,
        footer_len: 512,
        created_by: Some("node-a"),
        seq_min: Some(10),
        seq_max: Some(20),
        tombstone_seq_applied: 0,
        schema_version: 0,
        dynamic_fields: None,
    })
    .await
    .unwrap();

    // Staged splits are invisible to queries.
    assert!(
        ms.splits_for_query(stream.id, None, None, 10_000)
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
        .splits_for_query(stream.id, Some(1_753_300_030_000), None, 10_000)
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].split_id, split_id);
    assert_eq!(hits[0].state(), SplitState::Published);
    assert!(
        ms.splits_for_query(stream.id, Some(1_753_300_060_001), None, 10_000)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        ms.splits_for_query(stream.id, None, Some(1_753_299_999_999), 10_000)
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
        ms.splits_for_query(stream.id, None, None, 10_000)
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
async fn small_published_splits_windows_per_stream() {
    let Some(ms) = metastore().await else { return };
    let busy = ms.ensure_stream(&unique("merge-busy")).await.unwrap();
    let starved = ms.ensure_stream(&unique("merge-starved")).await.unwrap();

    // The busy stream has more small splits than the window, all newer
    // than the starved stream's; a global LIMIT would return only busy
    // rows (#60). The starved stream's two oldest tie on time_start so
    // the window edge exercises the id tiebreak.
    let mut created: std::collections::HashMap<i64, Vec<String>> =
        std::collections::HashMap::new();
    for (stream, times) in [
        (&busy, vec![2_000_000i64, 2_001_000, 2_002_000, 2_003_000]),
        (&starved, vec![1_000_000, 1_000_000, 1_001_000]),
    ] {
        for start in times {
            let split_id = unique("split");
            ms.stage_split(&NewSplit {
                split_id: &split_id,
                stream_id: stream.id,
                storage_key: &format!("streams/{}/{split_id}.split", stream.name),
                doc_count: 100,
                size_bytes: 4096,
                time_start_millis: start,
                time_end_millis: start + 999,
                footer_len: 512,
                created_by: None,
                seq_min: None,
                seq_max: None,
                tombstone_seq_applied: 0,
                schema_version: 0,
                dynamic_fields: None,
            })
            .await
            .unwrap();
            ms.publish_split(&split_id).await.unwrap();
            created.entry(stream.id).or_default().push(split_id);
        }
    }

    // The shared dev database may hold other streams' splits; assert only
    // on rows belonging to the two streams created here.
    let ours: Vec<_> = ms
        .small_published_splits(1 << 20, 2)
        .await
        .unwrap()
        .into_iter()
        .filter(|s| s.stream_id == busy.id || s.stream_id == starved.id)
        .collect();

    // Each stream contributes exactly its two oldest candidates,
    // time-ordered within the stream, regardless of how many the busier
    // stream holds. The starved stream's timestamp tie resolves to
    // insertion (id) order.
    let busy_ids: Vec<&str> = ours
        .iter()
        .filter(|s| s.stream_id == busy.id)
        .map(|s| s.split_id.as_str())
        .collect();
    let starved_ids: Vec<&str> = ours
        .iter()
        .filter(|s| s.stream_id == starved.id)
        .map(|s| s.split_id.as_str())
        .collect();
    let expect = |stream_id: i64| -> Vec<&str> {
        created[&stream_id][..2].iter().map(String::as_str).collect()
    };
    assert_eq!(busy_ids, expect(busy.id));
    assert_eq!(starved_ids, expect(starved.id));
    assert!(
        ours.windows(2).all(|w| w[0].stream_id != w[1].stream_id
            || w[0].time_start_millis <= w[1].time_start_millis),
        "rows must be time-ordered within each stream"
    );

    for stream in [&busy, &starved] {
        ms.delete_stream(&stream.name).await.unwrap();
    }
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

#[tokio::test]
#[ignore = "requires Postgres (set RSEARCH_TEST_DATABASE_URL)"]
async fn tombstones_upsert_and_page_by_seq() {
    use rsearch_metastore::NewTombstone;
    let Some(ms) = metastore().await else { return };
    let stream = ms.ensure_stream(&unique("tomb")).await.unwrap();
    let t = |doc: &str, before: i64| NewTombstone {
        stream_id: stream.id,
        doc_id: doc.to_string(),
        before_seq: before,
    };

    // Duplicates within one batch collapse to the max before_seq.
    ms.upsert_tombstones(&[t("a", 10), t("b", 20), t("a", 15)])
        .await
        .unwrap();
    let rows = ms.tombstones_since(stream.id, 0, 100).await.unwrap();
    assert_eq!(rows.len(), 2);
    let a = rows.iter().find(|r| r.doc_id == "a").unwrap().clone();
    assert_eq!(a.before_seq, 15);
    let last_seq = rows.iter().map(|r| r.seq).max().unwrap();

    // Nothing new since the last seq.
    assert!(ms.tombstones_since(stream.id, last_seq, 100).await.unwrap().is_empty());

    // Raising an existing row re-sequences it so incremental readers see
    // it again; lowering is ignored (keeps the max).
    ms.upsert_tombstones(&[t("a", 30), t("b", 5)]).await.unwrap();
    let newer = ms.tombstones_since(stream.id, last_seq, 100).await.unwrap();
    assert_eq!(newer.len(), 1);
    assert_eq!((newer[0].doc_id.as_str(), newer[0].before_seq), ("a", 30));
    assert!(newer[0].seq > a.seq);
    let all = ms.tombstones_since(stream.id, 0, 100).await.unwrap();
    assert_eq!(all.iter().find(|r| r.doc_id == "b").unwrap().before_seq, 20);
    assert_eq!(all.len(), 2);

    // Bounds lookup: what a new write must exceed.
    let mut bounds = ms
        .tombstone_bounds(&[(stream.id, "a".into()), (stream.id, "zzz".into())])
        .await
        .unwrap();
    bounds.sort();
    assert_eq!(bounds, vec![(stream.id, "a".to_string(), 30)]);

    // Paging honors the limit in seq order.
    let page = ms.tombstones_since(stream.id, 0, 1).await.unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].doc_id, "b");

    ms.delete_stream(&stream.name).await.unwrap();
    assert!(
        ms.tombstones_since(stream.id, 0, 10).await.unwrap().is_empty(),
        "cascade"
    );

    // ---- purge (same test: purge is global, so it must not run
    // concurrently with the assertions above).
    let stream = ms.ensure_stream(&unique("purge")).await.unwrap();
    let t = |doc: &str, before: i64| NewTombstone {
        stream_id: stream.id,
        doc_id: doc.to_string(),
        before_seq: before,
    };
    ms.upsert_tombstones(&[t("a", 10), t("b", 20)]).await.unwrap();
    let rows = ms.tombstones_since(stream.id, 0, 10).await.unwrap();
    let (seq_a, seq_b) = (rows[0].seq, rows[1].seq);
    fn split<'a>(stream_id: i64, id: &'a str, applied: i64) -> NewSplit<'a> {
        NewSplit {
        split_id: id,
        stream_id,
        storage_key: "k",
        doc_count: 1,
        size_bytes: 1,
        time_start_millis: 0,
        time_end_millis: 0,
        footer_len: 0,
        created_by: None,
        seq_min: Some(1),
        seq_max: Some(2),
        tombstone_seq_applied: applied,
        schema_version: 0,
        dynamic_fields: None,
        }
    }
    let s1 = unique("s1");
    ms.stage_split(&split(stream.id, &s1, seq_a)).await.unwrap();
    ms.publish_split(&s1).await.unwrap();

    async fn remaining(ms: &Metastore, id: i64) -> Vec<i64> {
        ms.tombstones_since(id, 0, 10)
            .await
            .unwrap()
            .into_iter()
            .map(|t| t.seq)
            .collect()
    }
    // Grace not elapsed: nothing purged even though a is applied.
    ms.purge_tombstones(3600.0, 1_000).await.unwrap();
    assert_eq!(remaining(&ms, stream.id).await, vec![seq_a, seq_b]);
    // Grace 0: only a (seq <= floor); b is above the floor.
    ms.purge_tombstones(0.0, 1_000).await.unwrap();
    assert_eq!(remaining(&ms, stream.id).await, vec![seq_b]);
    // Raising the floor releases b.
    ms.mark_tombstones_applied(&s1, seq_b).await.unwrap();
    ms.purge_tombstones(0.0, 1_000).await.unwrap();
    assert!(remaining(&ms, stream.id).await.is_empty());
    ms.delete_stream(&stream.name).await.unwrap();
}

#[tokio::test]
#[ignore = "requires Postgres (set RSEARCH_TEST_DATABASE_URL)"]
async fn stray_object_keys_finds_placements_without_split_rows() {
    let Some(ms) = metastore().await else { return };
    let stream = ms.ensure_stream(&unique("stray")).await.unwrap();

    // Tracked: placement rows plus a split row (any state protects it).
    let tracked_key = format!("streams/test/{}.split", unique("tracked"));
    let split_id = unique("split");
    ms.stage_split(&NewSplit {
        split_id: &split_id,
        stream_id: stream.id,
        storage_key: &tracked_key,
        doc_count: 1,
        size_bytes: 10,
        time_start_millis: 0,
        time_end_millis: 0,
        footer_len: 0,
        created_by: None,
        seq_min: None,
        seq_max: None,
        tombstone_seq_applied: 0,
        schema_version: 0,
        dynamic_fields: None,
    })
    .await
    .unwrap();
    ms.record_object_location(&tracked_key, "stray-node-a", 10).await.unwrap();

    // Stray: placement rows on two nodes, no split row — the crash
    // window between the storage put and the split-row insert (#50).
    let stray_key = format!("streams/test/{}.split", unique("stray"));
    ms.record_object_location(&stray_key, "stray-node-a", 10).await.unwrap();
    ms.record_object_location(&stray_key, "stray-node-b", 10).await.unwrap();

    let strays = ms.stray_object_keys(0.0, 10_000).await.unwrap();
    assert!(strays.contains(&stray_key));
    assert!(!strays.contains(&tracked_key));
    // Distinct keys: two placement rows surface the key once.
    assert_eq!(strays.iter().filter(|k| *k == &stray_key).count(), 1);

    // The age floor keeps fresh put->stage windows out of the sweep.
    let fresh = ms.stray_object_keys(3600.0, 10_000).await.unwrap();
    assert!(!fresh.contains(&stray_key));

    ms.remove_object_locations(&stray_key).await.unwrap();
    ms.remove_object_locations(&tracked_key).await.unwrap();
    ms.delete_stream(&stream.name).await.unwrap();
}

#[tokio::test]
#[ignore = "requires Postgres (set RSEARCH_TEST_DATABASE_URL)"]
async fn splits_below_schema_version_lists_stale_layouts_newest_first() {
    let Some(ms) = metastore().await else { return };
    let stream = ms.ensure_stream(&unique("schema-upgrade")).await.unwrap();

    // Two legacy splits (versions 0 and 1) and one current; only the
    // legacy ones are upgrade candidates, newest data first (#66).
    let mut ids = Vec::new();
    for (version, end) in [(0, 1_000_999i64), (2, 3_000_999), (1, 2_000_999)] {
        let split_id = unique("split");
        ms.stage_split(&NewSplit {
            split_id: &split_id,
            stream_id: stream.id,
            storage_key: &format!("streams/{}/{split_id}.split", stream.name),
            doc_count: 10,
            size_bytes: 1024,
            time_start_millis: end - 999,
            time_end_millis: end,
            footer_len: 128,
            created_by: None,
            seq_min: None,
            seq_max: None,
            tombstone_seq_applied: 0,
            schema_version: version,
            dynamic_fields: None,
        })
        .await
        .unwrap();
        ms.publish_split(&split_id).await.unwrap();
        ids.push((version, split_id));
    }

    // The shared dev database may hold other streams' splits.
    let ours: Vec<_> = ms
        .splits_below_schema_version(2, 10_000)
        .await
        .unwrap()
        .into_iter()
        .filter(|s| s.stream_id == stream.id)
        .collect();
    let expected: Vec<&str> = ids
        .iter()
        .filter(|(v, _)| *v < 2)
        .map(|(_, id)| id.as_str())
        .collect();
    assert_eq!(ours.len(), 2);
    assert_eq!(ours[0].split_id, expected[1], "version-1 split has the newer data");
    assert_eq!(ours[1].split_id, expected[0]);
    assert!(ours.iter().all(|s| s.schema_version < 2));

    // The column round-trips through every split read path.
    let current = ms
        .splits_for_query(stream.id, None, None, 100)
        .await
        .unwrap()
        .into_iter()
        .find(|s| s.time_end_millis == 3_000_999)
        .unwrap();
    assert_eq!(current.schema_version, 2);

    ms.delete_stream(&stream.name).await.unwrap();
}
