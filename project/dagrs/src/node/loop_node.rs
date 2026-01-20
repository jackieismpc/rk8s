use crate::connection::{in_channel::InChannels, out_channel::OutChannels};
use crate::node::{Node, NodeId, NodeName, NodeTable};
use crate::utils::{env::EnvVar, output::FlowControl, output::Output};
use async_trait::async_trait;
use std::sync::Arc;

/// Condition trait for loop nodes
#[async_trait]
pub trait LoopCondition: Send + Sync {
    async fn should_continue(&mut self, input: &mut InChannels, env: Arc<EnvVar>) -> bool;
    fn reset(&mut self) {}
}

/// Loop node that repeats execution of a target node based on a condition
pub struct CountLoopCondition {
    max_iterations: usize,
    current_iteration: usize,
}

impl CountLoopCondition {
    /// Create a new CountLoopCondition with the specified maximum iterations
    pub fn new(max: usize) -> Self {
        Self {
            max_iterations: max,
            current_iteration: 0,
        }
    }
}

#[async_trait]
impl LoopCondition for CountLoopCondition {
    /// Determine if the loop should continue based on the current iteration count
    async fn should_continue(&mut self, _input: &mut InChannels, _env: Arc<EnvVar>) -> bool {
        if self.current_iteration < self.max_iterations {
            self.current_iteration += 1;
            true
        } else {
            false
        }
    }

    fn reset(&mut self) {
        self.current_iteration = 0;
    }
}

/// Loop node implementation
pub struct LoopNode {
    id: NodeId,
    name: NodeName,
    in_channels: InChannels,
    out_channels: OutChannels,
    target_node: NodeId,
    condition: Box<dyn LoopCondition>,
}

impl LoopNode {
    /// Create a new LoopNode
    pub fn new(
        name: NodeName,
        target_node: NodeId,
        condition: impl LoopCondition + 'static,
        node_table: &mut NodeTable,
    ) -> Self {
        Self {
            id: node_table.alloc_id_for(&name),
            name,
            in_channels: InChannels::default(),
            out_channels: OutChannels::default(),
            target_node,
            condition: Box::new(condition),
        }
    }
}

#[async_trait]
impl Node for LoopNode {
    fn id(&self) -> NodeId {
        self.id
    }
    fn name(&self) -> NodeName {
        self.name.clone()
    }
    fn input_channels(&mut self) -> &mut InChannels {
        &mut self.in_channels
    }
    fn output_channels(&mut self) -> &mut OutChannels {
        &mut self.out_channels
    }
    async fn run(&mut self, env: Arc<EnvVar>) -> Output {
        if self
            .condition
            .should_continue(&mut self.in_channels, env)
            .await
        {
            Output::Flow(FlowControl::loop_to_node(self.target_node.as_usize()))
        } else {
            Output::Flow(FlowControl::Continue)
        }
    }

    fn reset(&mut self) {
        self.condition.reset();
    }
}
