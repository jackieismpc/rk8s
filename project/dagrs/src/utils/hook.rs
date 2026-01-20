use crate::node::Node;
use crate::utils::env::EnvVar;
use crate::utils::output::Output;
use async_trait::async_trait;
use std::sync::Arc;

/// Execution hook trait for monitoring node execution
#[async_trait]
pub trait ExecutionHook: Send + Sync {
    async fn before_node_run(&self, node: &dyn Node, env: &Arc<EnvVar>);
    async fn after_node_run(&self, node: &dyn Node, output: &Output, env: &Arc<EnvVar>);
    async fn on_error(&self, error: &dyn std::error::Error, env: &Arc<EnvVar>);
}
