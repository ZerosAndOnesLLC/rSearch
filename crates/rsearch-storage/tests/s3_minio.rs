//! Integration tests against a real S3-compatible endpoint (MinIO).
//! Ignored by default; run with a MinIO container up:
//!   docker run -d -p 9000:9000 -e MINIO_ROOT_USER=minioadmin \
//!     -e MINIO_ROOT_PASSWORD=minioadmin minio/minio server /data
//!   RSEARCH_TEST_S3_ENDPOINT=http://127.0.0.1:9000 \
//!     cargo test -p rsearch-storage --test s3_minio -- --ignored

use bytes::Bytes;
use rsearch_common::config::StorageConfig;
use rsearch_storage::{S3Storage, Storage, StorageError};

fn test_config(bucket: &str) -> Option<StorageConfig> {
    let endpoint = std::env::var("RSEARCH_TEST_S3_ENDPOINT").ok()?;
    // Credentials via our own config (the self-hosted path) — no AWS env
    // vars or credential chain involved.
    Some(StorageConfig {
        backend: "s3".to_string(),
        bucket: bucket.to_string(),
        endpoint,
        force_path_style: true,
        access_key_id: "minioadmin".to_string(),
        secret_access_key: "minioadmin".to_string(),
        ..StorageConfig::default()
    })
}

async fn setup(bucket: &str) -> Option<S3Storage> {
    let cfg = test_config(bucket)?;
    // Create the bucket directly with the SDK (setup only; the code under
    // test goes through the Storage trait).
    let http_client = aws_smithy_http_client::Builder::new()
        .tls_provider(aws_smithy_http_client::tls::Provider::Rustls(
            aws_smithy_http_client::tls::rustls_provider::CryptoMode::AwsLcFips,
        ))
        .build_https();
    let shared = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .http_client(http_client)
        .endpoint_url(cfg.endpoint.clone())
        .credentials_provider(aws_sdk_s3::config::Credentials::new(
            "minioadmin",
            "minioadmin",
            None,
            None,
            "test",
        ))
        .region(aws_config::Region::new("us-east-1"))
        .load()
        .await;
    let client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::config::Builder::from(&shared)
            .force_path_style(true)
            .build(),
    );
    let _ = client.create_bucket().bucket(bucket).send().await;
    Some(S3Storage::from_config(&cfg).await.unwrap())
}

#[tokio::test]
#[ignore = "requires MinIO (set RSEARCH_TEST_S3_ENDPOINT)"]
async fn put_get_range_size_delete_roundtrip() {
    let Some(s) = setup("rsearch-test-roundtrip").await else {
        return;
    };
    s.put("splits/a/one.bin", Bytes::from_static(b"0123456789"))
        .await
        .unwrap();
    assert_eq!(
        s.get("splits/a/one.bin").await.unwrap().as_ref(),
        b"0123456789"
    );
    assert_eq!(
        s.get_range("splits/a/one.bin", 3..7).await.unwrap().as_ref(),
        b"3456"
    );
    assert_eq!(s.size("splits/a/one.bin").await.unwrap(), 10);
    assert!(s.exists("splits/a/one.bin").await.unwrap());

    s.delete("splits/a/one.bin").await.unwrap();
    assert!(!s.exists("splits/a/one.bin").await.unwrap());
    assert!(matches!(
        s.get("splits/a/one.bin").await,
        Err(StorageError::NotFound(_))
    ));
}

#[tokio::test]
#[ignore = "requires MinIO (set RSEARCH_TEST_S3_ENDPOINT)"]
async fn list_and_put_file() {
    let Some(s) = setup("rsearch-test-list").await else {
        return;
    };
    // Idempotence: clear leftovers from previous runs.
    for key in s.list("").await.unwrap() {
        s.delete(&key).await.unwrap();
    }
    for key in ["p/1", "p/2", "p/sub/3", "q/1"] {
        s.put(key, Bytes::from_static(b"x")).await.unwrap();
    }
    assert_eq!(s.list("p/").await.unwrap(), vec!["p/1", "p/2", "p/sub/3"]);

    let tmp = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(tmp.path(), b"from-file").unwrap();
    s.put_file("p/file.bin", tmp.path()).await.unwrap();
    assert_eq!(s.get("p/file.bin").await.unwrap().as_ref(), b"from-file");
}
