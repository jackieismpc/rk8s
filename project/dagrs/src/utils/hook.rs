use crate::node::Node;
use crate::utils::env::EnvVar;
use crate::utils::output::Output;
use async_trait::async_trait;
use std::sync::Arc;

/// Execution hook trait for monitoring node execution
///
/// Hooks allow users to inject custom logic at specific points in a node's lifecycle.
/// They are useful for logging, monitoring, error reporting, or modifying execution context.
///
/// # Thread Safety
/// Hooks are executed in the same async task as the node.
/// The `ExecutionHook` trait requires `Send + Sync`, allowing hooks to be shared across threads.
/// Implementations should ensure thread safety if they access shared state.
///
/// # Performance
/// Hooks are awaited, meaning they will block the node execution until they complete.
/// Implementations should be efficient and avoid long-running blocking operations.
///
/// # Exception Handling
/// If a hook panics, it may crash the node execution task.
/// Implementations should handle their own errors internally.
#[async_trait]
pub trait ExecutionHook: Send + Sync {
    /// Called before a node starts execution.
    ///
    /// # Arguments
    /// * `node` - The node about to execute.
    /// * `env` - The global environment variables.
    async fn before_node_run(&self, node: &dyn Node, env: &Arc<EnvVar>);

    /// Called after a node completes execution (successfully).
    ///
    /// # Arguments
    /// * `node` - The node that executed.
    /// * `output` - The output produced by the node.
    /// * `env` - The global environment variables.
    async fn after_node_run(&self, node: &dyn Node, output: &Output, env: &Arc<EnvVar>);

    /// Called when a node execution fails (returns an error or panics).
    ///
    /// # Arguments
    /// * `error` - The error that occurred.
    /// * `env` - The global environment variables.
    async fn on_error(&self, error: &dyn std::error::Error, env: &Arc<EnvVar>);
}
