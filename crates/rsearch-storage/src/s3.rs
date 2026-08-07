use std::ops::Range;
use std::path::Path;

use async_trait::async_trait;
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;

use rsearch_common::config::StorageConfig;

use crate::error::{StorageError, StorageResult};
use crate::storage::Storage;

/// S3 / S3-compatible (MinIO) backend. All TLS uses the aws-lc-rs FIPS
/// provider; AWS FIPS endpoints are enabled via config for GovCloud.
pub struct S3Storage {
    client: Client,
    bucket: String,
}

impl S3Storage {
    /// Build a client from config: FIPS TLS, optional custom endpoint /
    /// path-style / static credentials. Requires storage.bucket to be set.
    pub async fn from_config(cfg: &StorageConfig) -> StorageResult<Self> {
        if cfg.bucket.is_empty() {
            return Err(StorageError::Backend {
                key: String::new(),
                message: "storage.bucket must be set for the s3 backend".to_string(),
            });
        }

        let http_client = aws_smithy_http_client::Builder::new()
            .tls_provider(aws_smithy_http_client::tls::Provider::Rustls(
                aws_smithy_http_client::tls::rustls_provider::CryptoMode::AwsLcFips,
            ))
            .build_https();

        let mut loader =
            aws_config::defaults(aws_config::BehaviorVersion::latest()).http_client(http_client);
        if !cfg.endpoint.is_empty() {
            loader = loader.endpoint_url(cfg.endpoint.clone());
        }
        // Self-hosted / air-gapped: static credentials from our own config
        // short-circuit the AWS chain so IMDS is never probed off-AWS.
        if !cfg.access_key_id.is_empty() {
            loader = loader.credentials_provider(aws_sdk_s3::config::Credentials::new(
                cfg.access_key_id.clone(),
                cfg.secret_access_key.clone(),
                None,
                None,
                "rsearch-config",
            ));
        }
        let region = if cfg.region.is_empty() {
            "us-east-1".to_string()
        } else {
            cfg.region.clone()
        };
        loader = loader.region(aws_config::Region::new(region));
        let shared = loader.load().await;

        let mut builder =
            aws_sdk_s3::config::Builder::from(&shared).force_path_style(cfg.force_path_style);
        if cfg.use_fips_endpoint {
            builder = builder.use_fips(true);
        }
        Ok(Self {
            client: Client::from_conf(builder.build()),
            bucket: cfg.bucket.clone(),
        })
    }

    fn backend_err(key: &str, err: impl std::fmt::Debug) -> StorageError {
        StorageError::Backend {
            key: key.to_string(),
            message: format!("{err:?}"),
        }
    }
}

#[async_trait]
impl Storage for S3Storage {
    async fn put(&self, key: &str, data: Bytes) -> StorageResult<()> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(data))
            .send()
            .await
            .map_err(|e| Self::backend_err(key, e))?;
        Ok(())
    }

    async fn put_file(&self, key: &str, local: &Path) -> StorageResult<()> {
        let body = ByteStream::from_path(local)
            .await
            .map_err(|e| Self::backend_err(key, e))?;
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(body)
            .send()
            .await
            .map_err(|e| Self::backend_err(key, e))?;
        Ok(())
    }

    async fn get(&self, key: &str) -> StorageResult<Bytes> {
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| match &e {
                aws_sdk_s3::error::SdkError::ServiceError(se) if se.err().is_no_such_key() => {
                    StorageError::NotFound(key.to_string())
                }
                _ => Self::backend_err(key, e),
            })?;
        let data = resp
            .body
            .collect()
            .await
            .map_err(|e| Self::backend_err(key, e))?;
        Ok(data.into_bytes())
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> StorageResult<Bytes> {
        if range.is_empty() {
            return Ok(Bytes::new());
        }
        let http_range = format!("bytes={}-{}", range.start, range.end - 1);
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .range(http_range)
            .send()
            .await
            .map_err(|e| match &e {
                aws_sdk_s3::error::SdkError::ServiceError(se) if se.err().is_no_such_key() => {
                    StorageError::NotFound(key.to_string())
                }
                _ => Self::backend_err(key, e),
            })?;
        let data = resp
            .body
            .collect()
            .await
            .map_err(|e| Self::backend_err(key, e))?;
        Ok(data.into_bytes())
    }

    async fn size(&self, key: &str) -> StorageResult<u64> {
        let resp = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| match &e {
                aws_sdk_s3::error::SdkError::ServiceError(se) if se.err().is_not_found() => {
                    StorageError::NotFound(key.to_string())
                }
                _ => Self::backend_err(key, e),
            })?;
        u64::try_from(resp.content_length().unwrap_or(0)).map_err(|e| Self::backend_err(key, e))
    }

    async fn delete(&self, key: &str) -> StorageResult<()> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| Self::backend_err(key, e))?;
        Ok(())
    }

    async fn list(&self, prefix: &str) -> StorageResult<Vec<String>> {
        let mut keys = Vec::new();
        let mut pages = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(prefix)
            .into_paginator()
            .send();
        while let Some(page) = pages.next().await {
            let page = page.map_err(|e| Self::backend_err(prefix, e))?;
            for obj in page.contents() {
                if let Some(key) = obj.key() {
                    keys.push(key.to_string());
                }
            }
        }
        keys.sort();
        Ok(keys)
    }
}
