use crate::connection::{in_channel::InChannels, out_channel::OutChannels};
use crate::node::{Node, NodeId, NodeName, NodeTable};
use crate::utils::{env::EnvVar, output::FlowControl, output::Output};
use async_trait::async_trait;
use std::sync::Arc;

/// Router trait for routing logic
#[async_trait]
pub trait Router: Send + Sync {
    async fn route(
        &self,
        input: &mut InChannels,
        output: &OutChannels,
        env: Arc<EnvVar>,
    ) -> Vec<usize>;
}

/// Router node implementation
pub struct RouterNode {
    id: NodeId,
    name: NodeName,
    in_channels: InChannels,
    out_channels: OutChannels,
    router: Box<dyn Router>,
}

impl RouterNode {
    /// Create a new RouterNode
    pub fn new(name: NodeName, router: impl Router + 'static, node_table: &mut NodeTable) -> Self {
        Self {
            id: node_table.alloc_id_for(&name),
            name,
            in_channels: InChannels::default(),
            out_channels: OutChannels::default(),
            router: Box::new(router),
        }
    }
}

#[async_trait]
impl Node for RouterNode {
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
        let routes = self
            .router
            .route(&mut self.in_channels, &self.out_channels, env)
            .await;
        Output::Flow(FlowControl::Branch(routes))
    }
    fn is_condition(&self) -> bool {
        true
    }
}
