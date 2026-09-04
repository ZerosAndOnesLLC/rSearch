//! Scroll contexts (issue #72): server-side paging state shared across
//! search nodes.

use crate::error::MetastoreResult;
use crate::metastore::Metastore;

/// A stored scroll context.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ScrollRecord {
    /// Opaque scroll id handed to the client.
    pub id: String,
    /// Stream the scroll runs against.
    pub stream: String,
    /// The `_search` body it was opened with (aggregations removed).
    pub request: serde_json::Value,
    /// `sort` values of the last hit served; None until a page had hits.
    pub cursor: Option<serde_json::Value>,
    /// `hits.total` of the first page.
    pub total: serde_json::Value,
}

impl Metastore {
    /// Register a scroll opened by the first page of a search.
    pub async fn create_scroll(
        &self,
        id: &str,
        stream: &str,
        request: &serde_json::Value,
        cursor: Option<&serde_json::Value>,
        total: &serde_json::Value,
        keep_alive_secs: f64,
    ) -> MetastoreResult<()> {
        sqlx::query(
            "INSERT INTO scroll_contexts (id, stream, request, cursor, total, expires_at)
             VALUES ($1, $2, $3, $4, $5, now() + make_interval(secs => $6))",
        )
        .bind(id)
        .bind(stream)
        .bind(request)
        .bind(cursor)
        .bind(total)
        .bind(keep_alive_secs)
        .execute(self.pool())
        .await?;
        Ok(())
    }

    /// A live (unexpired) scroll context by id.
    pub async fn get_scroll(&self, id: &str) -> MetastoreResult<Option<ScrollRecord>> {
        Ok(sqlx::query_as::<_, ScrollRecord>(
            "SELECT id, stream, request, cursor, total FROM scroll_contexts
             WHERE id = $1 AND expires_at > now()",
        )
        .bind(id)
        .fetch_optional(self.pool())
        .await?)
    }

    /// Advance a scroll past the page just served and renew its expiry.
    /// A page without hits leaves the cursor where it was.
    pub async fn advance_scroll(
        &self,
        id: &str,
        cursor: Option<&serde_json::Value>,
        keep_alive_secs: f64,
    ) -> MetastoreResult<()> {
        sqlx::query(
            "UPDATE scroll_contexts
             SET cursor = COALESCE($2, cursor),
                 expires_at = now() + make_interval(secs => $3)
             WHERE id = $1",
        )
        .bind(id)
        .bind(cursor)
        .bind(keep_alive_secs)
        .execute(self.pool())
        .await?;
        Ok(())
    }

    /// Advance the cursor without touching the expiry (a continuation
    /// that passed no `scroll` keep-alive).
    pub async fn advance_scroll_cursor(
        &self,
        id: &str,
        cursor: Option<&serde_json::Value>,
    ) -> MetastoreResult<()> {
        sqlx::query("UPDATE scroll_contexts SET cursor = COALESCE($2, cursor) WHERE id = $1")
            .bind(id)
            .bind(cursor)
            .execute(self.pool())
            .await?;
        Ok(())
    }

    /// Free scroll contexts by id; returns how many existed.
    pub async fn delete_scrolls(&self, ids: &[String]) -> MetastoreResult<u64> {
        let result = sqlx::query("DELETE FROM scroll_contexts WHERE id = ANY($1)")
            .bind(ids)
            .execute(self.pool())
            .await?;
        Ok(result.rows_affected())
    }

    /// Free every scroll context (`DELETE /_search/scroll/_all`).
    pub async fn delete_all_scrolls(&self) -> MetastoreResult<u64> {
        let result = sqlx::query("DELETE FROM scroll_contexts")
            .execute(self.pool())
            .await?;
        Ok(result.rows_affected())
    }

    /// Drop expired scroll contexts (control job).
    pub async fn purge_expired_scrolls(&self) -> MetastoreResult<u64> {
        let result = sqlx::query("DELETE FROM scroll_contexts WHERE expires_at <= now()")
            .execute(self.pool())
            .await?;
        Ok(result.rows_affected())
    }
}
