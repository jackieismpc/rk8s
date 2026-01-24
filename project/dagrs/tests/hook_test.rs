//! Tests for Execution Hook functionality
//!
//! This module tests the execution hooks that allow monitoring node lifecycle.
//! The `ExecutionHook` trait provides hook points for:
//! - `before_node_run`: Called before node execution
//! - `after_node_run`: Called after successful node execution
//! - `on_error`: Called when a node fails
//! - `on_retry`: Called before a node retry attempt
//! - `on_skip`: Called when a node is skipped due to branch pruning

use async_trait::async_trait;
use dagrs::node::action::Action;
use dagrs::node::default_node::DefaultNode;
use dagrs::node::router_node::{Router, RouterNode};
use dagrs::utils::hook::{ExecutionHook, RetryDecision};
use dagrs::{EnvVar, Graph, InChannels, Node, NodeTable, OutChannels, Output};
use std::sync::{Arc, Mutex};

/// A comprehensive test hook that tracks all hook invocations.
struct ComprehensiveHook {
    before_runs: Arc<Mutex<Vec<String>>>,
    after_runs: Arc<Mutex<Vec<String>>>,
    errors: Arc<Mutex<Vec<String>>>,
    skips: Arc<Mutex<Vec<String>>>,
    retries: Arc<Mutex<Vec<(String, u32)>>>,
}

#[async_trait]
impl ExecutionHook for ComprehensiveHook {
    async fn before_node_run(&self, node: &dyn Node, _env: &Arc<EnvVar>) {
        self.before_runs
            .lock()
            .unwrap()
            .push(node.name().to_string());
    }

    async fn after_node_run(&self, node: &dyn Node, _output: &Output, _env: &Arc<EnvVar>) {
        self.after_runs
            .lock()
            .unwrap()
            .push(node.name().to_string());
    }

    async fn on_error(&self, error: &(dyn std::error::Error + Send + Sync), _env: &Arc<EnvVar>) {
        let err_str = error.to_string();
        self.errors.lock().unwrap().push(err_str);
    }

    async fn on_skip(&self, node: &dyn Node, _env: &Arc<EnvVar>) {
        self.skips.lock().unwrap().push(node.name().to_string());
    }

    async fn on_retry(
        &self,
        node: &dyn Node,
        _error: &(dyn std::error::Error + Send + Sync),
        attempt: u32,
        _max_retries: u32,
        _env: &Arc<EnvVar>,
    ) -> RetryDecision {
        self.retries
            .lock()
            .unwrap()
            .push((node.name().to_string(), attempt));
        RetryDecision::Retry
    }
}

/// Simple action that does nothing.
struct NoOpAction;

#[async_trait]
impl Action for NoOpAction {
    async fn run(&self, _: &mut InChannels, _: &mut OutChannels, _: Arc<EnvVar>) -> Output {
        Output::empty()
    }
}

/// Action that fails with an error.
struct FailingAction {
    message: String,
}

#[async_trait]
impl Action for FailingAction {
    async fn run(&self, _: &mut InChannels, _: &mut OutChannels, _: Arc<EnvVar>) -> Output {
        Output::Err(self.message.clone())
    }
}

/// Router that selects only node A.
struct SelectARouter {
    target_id: usize,
}

#[async_trait]
impl Router for SelectARouter {
    async fn route(&self, _: &mut InChannels, _: &OutChannels, _: Arc<EnvVar>) -> Vec<usize> {
        vec![self.target_id]
    }
}

#[test]
fn test_hook_before_and_after() {
    let mut graph = Graph::new();
    let mut table = NodeTable::new();

    let before_runs = Arc::new(Mutex::new(Vec::new()));
    let after_runs = Arc::new(Mutex::new(Vec::new()));

    let hook = ComprehensiveHook {
        before_runs: before_runs.clone(),
        after_runs: after_runs.clone(),
        errors: Arc::new(Mutex::new(Vec::new())),
        skips: Arc::new(Mutex::new(Vec::new())),
        retries: Arc::new(Mutex::new(Vec::new())),
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        graph.add_hook(Box::new(hook)).await;
    });

    let node_a = DefaultNode::with_action("NodeA".to_string(), NoOpAction, &mut table);
    let node_b = DefaultNode::with_action("NodeB".to_string(), NoOpAction, &mut table);
    let id_a = node_a.id();
    let id_b = node_b.id();

    graph.add_node(node_a);
    graph.add_node(node_b);
    graph.add_edge(id_a, vec![id_b]);

    rt.block_on(async {
        graph.async_start().await.expect("Graph should succeed");
    });

    let before = before_runs.lock().unwrap();
    let after = after_runs.lock().unwrap();

    // Both nodes should have before and after hooks called
    assert_eq!(before.len(), 2, "before_node_run should be called twice");
    assert_eq!(after.len(), 2, "after_node_run should be called twice");
    assert!(before.contains(&"NodeA".to_string()));
    assert!(before.contains(&"NodeB".to_string()));
    assert!(after.contains(&"NodeA".to_string()));
    assert!(after.contains(&"NodeB".to_string()));
}

#[test]
fn test_hook_on_error() {
    let mut graph = Graph::new();
    let mut table = NodeTable::new();

    let errors = Arc::new(Mutex::new(Vec::new()));

    let hook = ComprehensiveHook {
        before_runs: Arc::new(Mutex::new(Vec::new())),
        after_runs: Arc::new(Mutex::new(Vec::new())),
        errors: errors.clone(),
        skips: Arc::new(Mutex::new(Vec::new())),
        retries: Arc::new(Mutex::new(Vec::new())),
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        graph.add_hook(Box::new(hook)).await;
    });

    let failing_node = DefaultNode::with_action(
        "FailingNode".to_string(),
        FailingAction {
            message: "Test error message".to_string(),
        },
        &mut table,
    );

    graph.add_node(failing_node);

    rt.block_on(async {
        let result = graph.async_start().await;
        assert!(result.is_err(), "Graph should fail due to node error");
    });

    let errors_list = errors.lock().unwrap();
    assert_eq!(errors_list.len(), 1, "on_error should be called once");
    assert!(
        errors_list[0].contains("Test error message"),
        "Error message should be captured"
    );
}

#[test]
fn test_hook_on_skip() {
    // Test that on_skip is called when a node is pruned by a router
    let mut graph = Graph::new();
    let mut table = NodeTable::new();

    let skips = Arc::new(Mutex::new(Vec::new()));

    let hook = ComprehensiveHook {
        before_runs: Arc::new(Mutex::new(Vec::new())),
        after_runs: Arc::new(Mutex::new(Vec::new())),
        errors: Arc::new(Mutex::new(Vec::new())),
        skips: skips.clone(),
        retries: Arc::new(Mutex::new(Vec::new())),
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        graph.add_hook(Box::new(hook)).await;
    });

    // Create nodes
    let node_a = DefaultNode::with_action("NodeA".to_string(), NoOpAction, &mut table);
    let id_a = node_a.id();

    let node_b = DefaultNode::with_action("NodeB".to_string(), NoOpAction, &mut table);
    let id_b = node_b.id();

    // Router that only selects NodeA
    let router = RouterNode::new(
        "Router".to_string(),
        SelectARouter {
            target_id: id_a.as_usize(),
        },
        &mut table,
    );
    let id_router = router.id();

    graph.add_node(router);
    graph.add_node(node_a);
    graph.add_node(node_b);

    // Router -> A, Router -> B
    graph.add_edge(id_router, vec![id_a, id_b]);

    rt.block_on(async {
        graph.async_start().await.expect("Graph should succeed");
    });

    let skips_list = skips.lock().unwrap();
    // NodeB should be skipped because the router only selected NodeA
    assert!(
        skips_list.contains(&"NodeB".to_string()),
        "NodeB should be skipped. Skipped nodes: {:?}",
        *skips_list
    );
}

#[test]
fn test_retry_decision_enum() {
    // Test that RetryDecision enum works correctly
    assert_eq!(RetryDecision::Retry, RetryDecision::Retry);
    assert_eq!(RetryDecision::Fail, RetryDecision::Fail);
    assert_ne!(RetryDecision::Retry, RetryDecision::Fail);

    // Test Debug trait
    assert_eq!(format!("{:?}", RetryDecision::Retry), "Retry");
    assert_eq!(format!("{:?}", RetryDecision::Fail), "Fail");

    // Test Clone trait
    let decision = RetryDecision::Retry;
    let cloned = decision.clone();
    assert_eq!(decision, cloned);
}
