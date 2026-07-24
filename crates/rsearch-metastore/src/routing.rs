//! Routing rule storage: conditions on incoming documents that route
//! (move) or copy them to other streams.

use crate::error::{MetastoreError, MetastoreResult};
use crate::metastore::Metastore;

#[derive(Debug, Clone, sqlx::FromRow, serde::Serialize)]
pub struct RoutingRuleRecord {
    pub id: i64,
    pub name: String,
    pub field: String,
    /// eq | contains | exists
    pub op: String,
    pub value: String,
    pub target_stream: String,
    pub copy: bool,
}

const RULE_COLUMNS: &str = "id, name, field, op, value, target_stream, copy";

impl Metastore {
    pub async fn list_routing_rules(&self) -> MetastoreResult<Vec<RoutingRuleRecord>> {
        let query = format!("SELECT {RULE_COLUMNS} FROM routing_rules ORDER BY id");
        Ok(sqlx::query_as::<_, RoutingRuleRecord>(sqlx::AssertSqlSafe(query))
            .fetch_all(self.pool())
            .await?)
    }

    pub async fn create_routing_rule(
        &self,
        name: &str,
        field: &str,
        op: &str,
        value: &str,
        target_stream: &str,
        copy: bool,
    ) -> MetastoreResult<RoutingRuleRecord> {
        if !matches!(op, "eq" | "contains" | "exists") {
            return Err(MetastoreError::Database(sqlx::Error::Configuration(
                format!("invalid op '{op}' (expected eq, contains, or exists)").into(),
            )));
        }
        let query = format!(
            "INSERT INTO routing_rules (name, field, op, value, target_stream, copy)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (name) DO UPDATE SET field = EXCLUDED.field, op = EXCLUDED.op,
                 value = EXCLUDED.value, target_stream = EXCLUDED.target_stream,
                 copy = EXCLUDED.copy
             RETURNING {RULE_COLUMNS}"
        );
        Ok(sqlx::query_as::<_, RoutingRuleRecord>(sqlx::AssertSqlSafe(query))
            .bind(name)
            .bind(field)
            .bind(op)
            .bind(value)
            .bind(target_stream)
            .bind(copy)
            .fetch_one(self.pool())
            .await?)
    }

    pub async fn delete_routing_rule(&self, name: &str) -> MetastoreResult<bool> {
        let result = sqlx::query("DELETE FROM routing_rules WHERE name = $1")
            .bind(name)
            .execute(self.pool())
            .await?;
        Ok(result.rows_affected() > 0)
    }
}
