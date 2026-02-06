use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::{Arc, atomic::AtomicBool};

use tokio::sync::{Mutex, broadcast, mpsc};

use crate::connection::{
    in_channel::InChannel, information_packet::Content, out_channel::OutChannel,
};
use crate::node::{Node, NodeId, NodeTable};
use crate::utils::checkpoint::CheckpointConfig;
use crate::utils::env::EnvVar;

use super::Graph;
use super::abstract_graph::AbstractGraph;

/// Errors that can occur while building a graph synchronously.
#[derive(Debug)]
pub enum GraphBuildError {
    NodeNotFound(NodeId),
    NodeLocked(NodeId),
    GraphLoopDetected,
}

impl fmt::Display for GraphBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GraphBuildError::NodeNotFound(id) => {
                write!(f, "node not found: {}", id.0)
            }
            GraphBuildError::NodeLocked(id) => {
                write!(f, "node is locked: {}", id.0)
            }
            GraphBuildError::GraphLoopDetected => write!(f, "graph loop detected"),
        }
    }
}

impl std::error::Error for GraphBuildError {}

/// Synchronous graph builder for ergonomic construction.
///
/// Build the graph synchronously, then execute asynchronously with `Graph::start().await`.
pub struct GraphBuilder {
    nodes: HashMap<NodeId, Arc<Mutex<dyn Node>>>,
    in_degree: HashMap<NodeId, usize>,
    abstract_graph: AbstractGraph,
    env: Option<EnvVar>,
}

impl Default for GraphBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl GraphBuilder {
    /// Create a new empty builder.
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            in_degree: HashMap::new(),
            abstract_graph: AbstractGraph::new(),
            env: None,
        }
    }

    /// Set the graph environment.
    pub fn set_env(&mut self, env: EnvVar) {
        self.env = Some(env);
    }

    /// Returns a list of node IDs in the builder.
    pub fn node_ids(&self) -> Vec<NodeId> {
        self.nodes.keys().copied().collect()
    }

    /// Returns a snapshot of edges in the builder.
    pub fn edges(&self) -> HashMap<NodeId, Vec<NodeId>> {
        self.abstract_graph
            .edges
            .iter()
            .map(|(from, tos)| (*from, tos.iter().copied().collect()))
            .collect()
    }

    /// Validate the graph (cycle detection).
    pub fn validate(&self) -> Result<(), GraphBuildError> {
        if self.abstract_graph.get_topological_sort().is_none() {
            return Err(GraphBuildError::GraphLoopDetected);
        }
        Ok(())
    }

    /// Add a node to the builder.
    pub fn add_node(&mut self, node: impl Node + 'static) -> Result<(), GraphBuildError> {
        if let Some(loop_structure) = node.loop_structure() {
            let abstract_node_id = node.id();
            let mut folded_nodes = Vec::with_capacity(loop_structure.len());

            for n in loop_structure.iter() {
                let guard = n
                    .try_lock()
                    .map_err(|_| GraphBuildError::NodeLocked(abstract_node_id))?;
                folded_nodes.push(guard.id());
            }

            self.abstract_graph
                .add_folded_node(abstract_node_id, folded_nodes);

            for inner in loop_structure {
                let concrete_id = inner
                    .try_lock()
                    .map_err(|_| GraphBuildError::NodeLocked(abstract_node_id))?
                    .id();
                self.nodes.insert(concrete_id, inner);
            }
            return Ok(());
        }

        let id = node.id();
        let node = Arc::new(Mutex::new(node));
        self.nodes.insert(id, node);
        self.in_degree.insert(id, 0);
        self.abstract_graph.add_node(id);
        Ok(())
    }

    /// Add an edge between two nodes.
    pub fn add_edge(
        &mut self,
        from_id: NodeId,
        all_to_ids: Vec<NodeId>,
    ) -> Result<(), GraphBuildError> {
        if !self.nodes.contains_key(&from_id) {
            return Err(GraphBuildError::NodeNotFound(from_id));
        }

        let to_ids = remove_duplicates(all_to_ids);
        for to_id in &to_ids {
            if !self.nodes.contains_key(to_id) {
                return Err(GraphBuildError::NodeNotFound(*to_id));
            }
        }

        let mut rx_map: HashMap<NodeId, mpsc::Receiver<Content>> = HashMap::new();

        {
            let from_node_lock = self.nodes.get(&from_id).unwrap();
            let mut from_node = from_node_lock
                .try_lock()
                .map_err(|_| GraphBuildError::NodeLocked(from_id))?;
            let from_channel = from_node.output_channels();

            for to_id in &to_ids {
                if !from_channel.0.contains_key(to_id) {
                    let (tx, rx) = mpsc::channel::<Content>(32);
                    from_channel.insert(*to_id, Arc::new(Mutex::new(OutChannel::Mpsc(tx))));
                    rx_map.insert(*to_id, rx);
                    self.in_degree
                        .entry(*to_id)
                        .and_modify(|e| *e += 1)
                        .or_insert(0);

                    self.abstract_graph.add_edge(from_id, *to_id);
                }
            }
        }

        for to_id in &to_ids {
            if let Some(to_node_lock) = self.nodes.get(to_id) {
                let mut to_node = to_node_lock
                    .try_lock()
                    .map_err(|_| GraphBuildError::NodeLocked(*to_id))?;
                let to_channel = to_node.input_channels();
                if let Some(rx) = rx_map.remove(to_id) {
                    to_channel.insert(from_id, Arc::new(Mutex::new(InChannel::Mpsc(rx))));
                }
            }
        }

        Ok(())
    }

    /// Build the executable graph.
    pub fn build(mut self) -> Result<Graph, GraphBuildError> {
        self.validate()?;

        let (tx, _) = broadcast::channel(100);
        let env = self
            .env
            .take()
            .unwrap_or_else(|| EnvVar::new(NodeTable::default()));

        let node_count = self.nodes.len();

        Ok(Graph {
            nodes: self.nodes,
            execute_states: HashMap::new(),
            node_count,
            env: Arc::new(env),
            is_active: Arc::new(AtomicBool::new(true)),
            in_degree: self.in_degree,
            blocks: Vec::new(),
            node_block_map: HashMap::new(),
            abstract_graph: self.abstract_graph,
            hooks: Arc::new(tokio::sync::RwLock::new(Vec::new())),
            event_sender: tx,
            max_loop_count: 1000,
            checkpoint_store: None,
            checkpoint_config: CheckpointConfig::default(),
        })
    }
}

fn remove_duplicates<T>(vec: Vec<T>) -> Vec<T>
where
    T: Eq + std::hash::Hash + Clone,
{
    let mut seen = HashSet::new();
    vec.into_iter().filter(|x| seen.insert(x.clone())).collect()
}
