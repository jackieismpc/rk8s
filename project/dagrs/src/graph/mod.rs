mod abstract_graph;
pub mod error;
pub mod event;
pub mod loop_subgraph;

use std::hash::Hash;
use std::sync::atomic::Ordering;
use std::{
    collections::{HashMap, HashSet},
    panic::{self, AssertUnwindSafe},
    sync::{Arc, atomic::AtomicBool},
};

use crate::{
    Output,
    connection::{in_channel::InChannel, information_packet::Content, out_channel::OutChannel},
    graph::event::GraphEvent,
    node::{Node, NodeId, NodeTable},
    utils::hook::ExecutionHook,
    utils::output::FlowControl,
    utils::{env::EnvVar, execstate::ExecState},
};

use log::{debug, error, info};
use tokio::sync::Mutex;
use tokio::sync::{RwLock, broadcast, mpsc};
use tokio::task;

use abstract_graph::AbstractGraph;
use error::GraphError;

/// [`Graph`] is dagrs's main body.
///
/// ['Graph'] is a network that satisfies FBP logic, provides node dependencies, and runs all of its nodes completely asynchronously
/// A `Graph` contains multiple nodes, which can be added as long as they implement the [`Node`] trait.
/// Each node defines specific execution logic by implementing the [`Action`] trait and overriding the `run` method.
///
/// The execution process of a `Graph` proceeds as follows:
/// - The user creates a set of nodes, each implementing the [`Node`] trait. These nodes can be created programmatically
///   or Generate auto_node using parse.
/// - Dependencies between nodes are defined, creating a directed acyclic graph (DAG) structure.
/// - During execution, nodes communicate via input/output channels (`InChannel` and `OutChannel`).
///   These channels support both point-to-point communication (using `MPSC`) and broadcasting (using `Broadcast`).
/// - After all nodes complete their execution, marking the graph as inactive.
///   This ensures that the `Graph` cannot be executed again without resetting its state.
///
/// The [`Graph`] is designed to efficiently manage task execution with built-in fault tolerance and flexible scheduling.
pub struct Graph {
    /// Define the Net struct that holds all nodes
    pub(crate) nodes: HashMap<NodeId, Arc<Mutex<dyn Node>>>,
    /// Store a task's running result.Execution results will be read
    /// and written asynchronously by several threads.
    pub(crate) execute_states: HashMap<NodeId, Arc<ExecState>>,
    /// Count all the nodes
    pub(crate) node_count: usize,
    /// Global environment variables for this Net job.
    /// It should be set before the Net job runs.
    pub(crate) env: Arc<EnvVar>,
    /// Mark whether the net task can continue to execute.
    /// When an error occurs during the execution of any task, This flag will still be set to true
    pub(crate) is_active: Arc<AtomicBool>,
    /// Node's in_degree, used for check loop
    pub(crate) in_degree: HashMap<NodeId, usize>,
    /// Stores the blocks of nodes divided by conditional nodes.
    /// Each block is a HashSet of NodeIds that represents a group of nodes that will be executed together.
    pub(crate) blocks: Vec<HashSet<NodeId>>,
    /// Maps NodeId to the index of the block it belongs to.
    /// This is built during `check_loop_and_partition` and used during execution.
    pub(crate) node_block_map: HashMap<NodeId, usize>,
    /// Abstract representation of the graph structure, used for cycle detection
    pub(crate) abstract_graph: AbstractGraph,
    /// Registered execution hooks
    pub(crate) hooks: Arc<RwLock<Vec<Box<dyn ExecutionHook>>>>,
    /// Event broadcaster
    pub(crate) event_sender: broadcast::Sender<GraphEvent>,
    /// Maximum number of loop iterations allowed to prevent infinite loops.
    pub(crate) max_loop_count: usize,
}

impl Default for Graph {
    fn default() -> Self {
        Self::new()
    }
}

impl Graph {
    /// Constructs a new `Graph`
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(100);
        Graph {
            nodes: HashMap::new(),
            node_count: 0,
            execute_states: HashMap::new(),
            env: Arc::new(EnvVar::new(NodeTable::default())),
            is_active: Arc::new(AtomicBool::new(true)),
            in_degree: HashMap::new(),
            blocks: vec![],
            node_block_map: HashMap::new(),
            abstract_graph: AbstractGraph::new(),
            hooks: Arc::new(RwLock::new(Vec::new())),
            event_sender: tx,
            max_loop_count: 1000,
        }
    }

    /// Set the maximum number of loop iterations.
    pub fn set_max_loop_count(&mut self, count: usize) {
        self.max_loop_count = count;
    }

    /// Reset the graph state but keep the nodes.
    /// This method is async because it needs to acquire locks on nodes.
    pub async fn reset(&mut self) {
        self.execute_states = HashMap::new();
        self.env = Arc::new(EnvVar::new(NodeTable::default()));
        self.is_active = Arc::new(AtomicBool::new(true));
        self.blocks.clear();
        self.node_block_map.clear();

        // Re-create channels for all edges to support graph reuse
        // 1. Clear existing channels
        for node in self.nodes.values() {
            let mut node = node.lock().await;
            node.input_channels().0.clear();
            node.output_channels().0.clear();
            node.reset();
        }

        // 2. Re-establish connections based on abstract_graph
        // Clone edges to avoid borrowing conflict
        let edges = self.abstract_graph.edges.clone();

        for (from_id, to_ids) in edges {
            // Resolve abstract node ID to concrete node IDs for folded loop nodes.
            // If from_id is an abstract (folded) node, we need to find the actual
            // concrete nodes that should send data.
            let concrete_from_ids: Vec<NodeId> = self
                .abstract_graph
                .unfold_node(from_id)
                .cloned()
                .unwrap_or_else(|| vec![from_id]);

            for concrete_from_id in concrete_from_ids {
                let mut rx_map: HashMap<NodeId, mpsc::Receiver<Content>> = HashMap::new();

                // Resolve to_ids: unfold any abstract nodes to their concrete counterparts
                let concrete_to_ids: Vec<NodeId> = to_ids
                    .iter()
                    .flat_map(|to_id| {
                        self.abstract_graph
                            .unfold_node(*to_id)
                            .cloned()
                            .unwrap_or_else(|| vec![*to_id])
                    })
                    .collect();

                // Setup OutChannels for 'concrete_from_id'
                if let Some(node_lock) = self.nodes.get(&concrete_from_id) {
                    let mut node = node_lock.lock().await;
                    let out_channels = node.output_channels();

                    for to_id in &concrete_to_ids {
                        // Only create channel if target node exists in the graph
                        if self.nodes.contains_key(to_id) {
                            let (tx, rx) = mpsc::channel::<Content>(32);
                            out_channels.insert(*to_id, Arc::new(Mutex::new(OutChannel::Mpsc(tx))));
                            rx_map.insert(*to_id, rx);
                        }
                    }
                }

                // Setup InChannels for concrete to_ids
                for (to_id, rx) in rx_map {
                    if let Some(node_lock) = self.nodes.get(&to_id) {
                        let mut node = node_lock.lock().await;
                        node.input_channels()
                            .insert(concrete_from_id, Arc::new(Mutex::new(InChannel::Mpsc(rx))));
                    }
                }
            }
        }
    }

    /// Register a new execution hook
    pub async fn add_hook(&mut self, hook: Box<dyn ExecutionHook>) {
        let mut hooks = self.hooks.write().await;
        hooks.push(hook);
    }

    /// Subscribe to graph events
    pub fn subscribe(&self) -> broadcast::Receiver<GraphEvent> {
        self.event_sender.subscribe()
    }

    /// Adds a new node to the `Graph`
    pub fn add_node(&mut self, node: impl Node + 'static) {
        if let Some(loop_structure) = node.loop_structure() {
            // Expand loop subgraph, and update concrete node id -> abstract node id mapping in abstract_graph
            let abstract_node_id = node.id();

            log::debug!("Add node {:?} to abstract graph", abstract_node_id);
            self.abstract_graph.add_folded_node(
                abstract_node_id,
                loop_structure
                    .iter()
                    .map(|n| n.blocking_lock().id())
                    .collect(),
            );

            for node in loop_structure {
                let concrete_id = node.blocking_lock().id();
                log::debug!("Add node {:?} to concrete graph", concrete_id);
                self.nodes.insert(concrete_id, node.clone());
            }
        } else {
            let id = node.id();
            let node = Arc::new(Mutex::new(node));
            self.node_count += 1;
            self.nodes.insert(id, node);
            self.in_degree.insert(id, 0);
            self.abstract_graph.add_node(id);

            log::debug!("Add node {:?} to concrete & abstract graph", id);
        }
    }
    /// Adds an edge between two nodes in the `Graph`.
    /// If the outgoing port of the sending node is empty and the number of receiving nodes is > 1, use the broadcast channel
    /// An MPSC channel is used if the outgoing port of the sending node is empty and the number of receiving nodes is equal to 1
    /// If the outgoing port of the sending node is not empty, adding any number of receiving nodes will change all relevant channels to broadcast
    pub fn add_edge(&mut self, from_id: NodeId, all_to_ids: Vec<NodeId>) {
        let to_ids = Self::remove_duplicates(all_to_ids);
        let mut rx_map: HashMap<NodeId, mpsc::Receiver<Content>> = HashMap::new();

        // Update channels
        {
            let from_node_lock = self.nodes.get_mut(&from_id).unwrap();
            let mut from_node = from_node_lock.blocking_lock();
            let from_channel = from_node.output_channels();

            for to_id in &to_ids {
                if !from_channel.0.contains_key(to_id) {
                    let (tx, rx) = mpsc::channel::<Content>(32);
                    from_channel.insert(*to_id, Arc::new(Mutex::new(OutChannel::Mpsc(tx.clone()))));
                    rx_map.insert(*to_id, rx);
                    self.in_degree
                        .entry(*to_id)
                        .and_modify(|e| *e += 1)
                        .or_insert(0);

                    // Update abstract graph
                    self.abstract_graph.add_edge(from_id, *to_id);
                }
            }
        }
        for to_id in &to_ids {
            if let Some(to_node_lock) = self.nodes.get_mut(to_id) {
                let mut to_node = to_node_lock.blocking_lock();
                let to_channel = to_node.input_channels();
                if let Some(rx) = rx_map.remove(to_id) {
                    to_channel.insert(from_id, Arc::new(Mutex::new(InChannel::Mpsc(rx))));
                }
            }
        }
    }

    /// Initializes the network, setting up the nodes.
    pub(crate) fn init(&mut self) {
        self.execute_states.reserve(self.nodes.len());
        self.nodes.keys().for_each(|node| {
            self.execute_states
                .insert(*node, Arc::new(ExecState::new()));
        });
    }

    /// This function is used for the execution of a single dag.
    pub fn start(&mut self) -> Result<(), GraphError> {
        let runtime = tokio::runtime::Runtime::new()
            .map_err(|e| GraphError::RuntimeCreationFailed(e.to_string()))?;
        runtime.block_on(async { self.async_start().await })
    }
    /// Executes a single DAG within an existing async runtime.
    ///
    /// Use this method when you are already running inside an async context
    /// (for example, inside a `tokio::main` function or a task spawned on a
    /// Tokio runtime) and you do **not** want `Graph` to create and manage
    /// its own Tokio runtime.
    ///
    /// Unlike [`start`], this method:
    /// - Does not create a new Tokio runtime.
    /// - Assumes it is called on a thread where a Tokio runtime is already
    ///   active.
    /// - Can be `await`-ed like any other async function.
    ///
    /// # Requirements
    ///
    /// - A Tokio runtime must be active on the current thread when this
    ///   method is called.
    /// - The graph must have been properly configured (nodes and edges
    ///   added) before calling this method.
    ///
    /// If those conditions are not met, execution may fail at runtime.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let mut graph = build_graph_somehow();
    ///
    ///     // Use `async_start` because we are already inside a Tokio runtime.
    ///     graph.async_start().await?;
    ///
    ///     Ok(())
    /// }
    /// ```
    pub async fn async_start(&mut self) -> Result<(), GraphError> {
        self.init();
        let is_loop = self.check_loop_and_partition().await;
        if is_loop {
            return Err(GraphError::GraphLoopDetected);
        }

        if !self.is_active.load(Ordering::Relaxed) {
            return Err(GraphError::GraphNotActive);
        }
        self.run().await
    }

    /// Executes the graph's nodes in a concurrent manner, respecting the block structure.
    ///
    /// - Executes nodes in blocks, where blocks are separated by conditional nodes
    /// - Runs nodes within each block concurrently using Tokio tasks
    /// - Handles node execution failures and panics
    /// - Supports conditional execution - if a conditional node returns false, remaining blocks are aborted
    /// - Tracks execution state and errors for each node
    ///
    /// # Returns
    /// - `Ok(())` if all nodes execute successfully
    /// - `Err(GraphError)` if any node fails or panics during execution
    ///   - Returns single error if only one failure occurs
    ///   - Returns `MultipleErrors` if multiple nodes fail
    async fn run(&mut self) -> Result<(), GraphError> {
        let condition_flag = Arc::new(Mutex::new(true));
        let errors = Arc::new(Mutex::new(Vec::new()));

        // Reset all nodes
        for node in self.nodes.values() {
            let mut node = node.lock().await;
            node.reset();
        }

        let mut pc = 0;
        let mut loop_count = 0;
        let mut active_nodes: HashSet<NodeId> = self.nodes.keys().cloned().collect();

        // Build parents map for pruning logic
        let mut parents_map: HashMap<NodeId, Vec<NodeId>> = HashMap::new();
        for (parent, children) in &self.abstract_graph.edges {
            for child in children {
                parents_map.entry(*child).or_default().push(*parent);
            }
        }

        // Start the nodes by blocks
        while pc < self.blocks.len() {
            let block = &self.blocks[pc];

            let mut active_block_nodes = Vec::new();
            let mut skipped_block_nodes = Vec::new();

            for id in block {
                if active_nodes.contains(id) {
                    active_block_nodes.push(*id);
                } else {
                    skipped_block_nodes.push(*id);
                }
            }

            // Handle skipped nodes
            // Note: We do NOT close output channels here to support loop execution.
            // Channels will be closed when the entire graph finishes.
            for node_id in skipped_block_nodes {
                let _ = self
                    .event_sender
                    .send(GraphEvent::NodeSkipped { id: node_id });
                debug!("Skipped node [id: {}]", node_id.0);
            }

            if active_block_nodes.is_empty() {
                pc += 1;
                continue;
            }

            let mut tasks = vec![];
            for node_id in active_block_nodes {
                let node = self
                    .nodes
                    .get(&node_id)
                    .ok_or(GraphError::NodeIdError(node_id.0))?;
                let execute_state = self
                    .execute_states
                    .get(&node_id)
                    .ok_or(GraphError::NodeIdError(node_id.0))?
                    .clone();
                let env = Arc::clone(&self.env);
                let node = Arc::clone(node);
                let condition_flag = condition_flag.clone();
                let errors = errors.clone();
                let hooks = self.hooks.clone();
                let event_sender = self.event_sender.clone();

                let task = task::spawn({
                    let errors = Arc::clone(&errors);
                    async move {
                        let node_ref: Arc<Mutex<dyn Node>> = node.clone();
                        let mut node: tokio::sync::MutexGuard<dyn Node> = node.lock().await;
                        let node_name = node.name().to_string();
                        let node_id = node.id();
                        let id_val = node_id.0;

                        // Hook: before_node_run
                        {
                            let hooks_guard = hooks.read().await;
                            for hook in hooks_guard.iter() {
                                hook.before_node_run(&*node, &env).await;
                            }
                        }
                        // Event: NodeStart
                        let _ = event_sender.send(GraphEvent::NodeStart {
                            id: node_id,
                            timestamp: std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs(),
                        });

                        let env_for_run = env.clone();
                        let result = panic::catch_unwind(AssertUnwindSafe(|| async move {
                            node.run(env_for_run).await
                        }));

                        match result {
                            Ok(out) => {
                                let out = out.await;
                                // Note: The original `node` guard was consumed when we called
                                // `node.run(...)`. The `run` method takes `&mut self`, which
                                // means the guard was borrowed and released when the call returned.
                                // Re-acquire the lock here so we can run hooks and perform cleanup.
                                let node = node_ref.lock().await;

                                // Hook: after_node_run
                                {
                                    let hooks_guard = hooks.read().await;
                                    for hook in hooks_guard.iter() {
                                        hook.after_node_run(&*node, &out, &env).await;
                                    }
                                }

                                if out.is_err() {
                                    let error_msg = out.get_err().unwrap_or("".to_string());
                                    error!(
                                        "Execution failed [name: {}, id: {}] - {}",
                                        node_name, id_val, error_msg
                                    );
                                    let _ = event_sender.send(GraphEvent::NodeFailed {
                                        id: node_id,
                                        error: error_msg.clone(),
                                    });

                                    // Hook: on_error
                                    let err_obj = GraphError::ExecutionFailed {
                                        node_name: node_name.clone(),
                                        node_id: id_val,
                                        error: error_msg.clone(),
                                    };
                                    {
                                        let hooks_guard = hooks.read().await;
                                        for hook in hooks_guard.iter() {
                                            hook.on_error(&err_obj, &env).await;
                                        }
                                    }

                                    execute_state.set_output(out.clone());
                                    execute_state.exe_fail();
                                    let mut errors_lock = errors.lock().await;
                                    errors_lock.push(err_obj);
                                } else {
                                    if let Some(false) = out.conditional_result() {
                                        let mut cf = condition_flag.lock().await;
                                        *cf = false;
                                        info!(
                                            "Condition failed on [name: {}, id: {}]. The rest nodes will abort.",
                                            node_name, id_val,
                                        )
                                    }
                                    let _ =
                                        event_sender.send(GraphEvent::NodeSuccess { id: node_id });

                                    execute_state.set_output(out.clone());
                                    execute_state.exe_success();
                                    debug!(
                                        "Execution succeed [name: {}, id: {}]",
                                        node_name, id_val,
                                    );
                                }

                                // We typically do not close output channels here to allow loop execution
                                // where the node might run again.
                                // If the graph needs to signal EOF, it should be done when the graph finishes
                                // or explicitly by the node logic.

                                (node_id, out)
                            }
                            Err(_) => {
                                let mut node_guard: tokio::sync::MutexGuard<dyn Node> =
                                    node_ref.lock().await;
                                node_guard.input_channels().close_all();
                                node_guard.output_channels().close_all();

                                error!("Execution failed [name: {}, id: {}]", node_name, id_val,);
                                let _ = event_sender.send(GraphEvent::NodeFailed {
                                    id: node_id,
                                    error: "Panic".to_string(),
                                });

                                execute_state.set_output(Output::Err("Panic".to_string()));
                                execute_state.exe_fail();

                                let err_obj = GraphError::PanicOccurred {
                                    node_name: node_name.clone(),
                                    node_id: id_val,
                                };
                                // Hook: on_error
                                {
                                    let hooks_guard = hooks.read().await;
                                    for hook in hooks_guard.iter() {
                                        hook.on_error(&err_obj, &env).await;
                                    }
                                }

                                let mut errors_lock = errors.lock().await;
                                errors_lock.push(err_obj);
                                (node_id, Output::Err("Panic".to_string()))
                            }
                        }
                    }
                });
                tasks.push(task);
            }

            let results: Vec<Result<(NodeId, Output), tokio::task::JoinError>> =
                futures::future::join_all(tasks).await;

            // Check for errors immediately
            let errors_guard = errors.lock().await;
            if !errors_guard.is_empty() {
                if errors_guard.len() == 1 {
                    return Err(errors_guard[0].clone());
                } else {
                    return Err(GraphError::MultipleErrors(errors_guard.clone()));
                }
            }
            drop(errors_guard);

            if !(*condition_flag.lock().await) {
                break;
            }

            let mut next_pc = pc + 1;
            let mut should_abort = false;

            for (node_id, output) in results.into_iter().flatten() {
                if self.handle_flow_control(
                    output,
                    node_id,
                    &mut active_nodes,
                    &self.node_block_map,
                    &parents_map,
                    &mut next_pc,
                    self.blocks.len(),
                )? {
                    should_abort = true;
                    break;
                }
            }

            if should_abort {
                break;
            }

            // Check for loop (backward jump)
            // Only increment loop counter on actual backward jumps (next_pc < pc)
            // not on staying at the same block (next_pc == pc)
            if next_pc < pc {
                loop_count += 1;
                if loop_count >= self.max_loop_count {
                    return Err(GraphError::LoopLimitExceeded(self.max_loop_count));
                }
                // Reset active_nodes on loop iteration to allow dynamic routing.
                // This ensures that nodes pruned by a router in a previous iteration
                // can be selected in subsequent iterations (e.g., alternating between branches).
                active_nodes = self.nodes.keys().cloned().collect();
            }

            pc = next_pc;
        }

        // Send GraphFinished event BEFORE setting is_active to false
        // to avoid race conditions where subscribers see the flag change
        // before receiving the event.
        let _ = self.event_sender.send(GraphEvent::GraphFinished);

        self.is_active
            .store(false, std::sync::atomic::Ordering::Relaxed);

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_flow_control(
        &self,
        output: Output,
        node_id: NodeId,
        active_nodes: &mut HashSet<NodeId>,
        node_block_map: &HashMap<NodeId, usize>,
        parents_map: &HashMap<NodeId, Vec<NodeId>>,
        next_pc: &mut usize,
        blocks_len: usize,
    ) -> Result<bool, GraphError> {
        if let Some(flow) = output.get_flow() {
            match flow {
                FlowControl::Loop(instr) => {
                    if let Some(idx) = instr.jump_to_block_index {
                        // Validate that the jump target is within valid block range
                        if idx >= blocks_len {
                            error!(
                                "Graph configuration error: jump_to_block_index {} is out of bounds (blocks count: {})",
                                idx, blocks_len
                            );
                            return Err(GraphError::ExecutionFailed {
                                node_name: format!("Node-{}", node_id.0),
                                node_id: node_id.0,
                                error: format!(
                                    "Graph configuration error: jump_to_block_index {} is out of bounds. \
                                     Valid range is 0..{}",
                                    idx, blocks_len
                                ),
                            });
                        }
                        *next_pc = idx;
                    } else if let Some(nid) = instr.jump_to_node {
                        if let Some(&idx) = node_block_map.get(&NodeId(nid)) {
                            // Validate that the resolved block index is within valid range
                            // This should not happen in normal operation, but is a defensive check
                            if idx >= blocks_len {
                                error!(
                                    "Internal error: node_block_map contains invalid block index {} for node {} (blocks count: {})",
                                    idx, nid, blocks_len
                                );
                                return Err(GraphError::ExecutionFailed {
                                    node_name: format!("Node-{}", node_id.0),
                                    node_id: node_id.0,
                                    error: format!(
                                        "Internal error: node_block_map contains invalid block index {} for node {}. \
                                         This indicates a bug in the graph partitioning logic.",
                                        idx, nid
                                    ),
                                });
                            }
                            *next_pc = idx;
                        } else {
                            error!(
                                "Graph configuration error: invalid jump target node {} not found in block map. \
                                   This is likely due to an incorrect node ID in the LoopInstruction.",
                                nid
                            );
                            return Err(GraphError::ExecutionFailed {
                                node_name: format!("Node-{}", node_id.0),
                                node_id: node_id.0,
                                error: format!(
                                    "Graph configuration error: invalid jump target node {} not found. \
                                     Ensure the node ID exists in the graph.",
                                    nid
                                ),
                            });
                        }
                    }
                }
                FlowControl::Branch(ids) => {
                    let allowed: HashSet<usize> = ids.iter().cloned().collect();
                    if let Some(children) = self.abstract_graph.edges.get(&node_id) {
                        let mut to_prune = Vec::new();
                        let empty_vec = Vec::new();

                        // 1. Identify immediate children to prune
                        for child in children {
                            if !allowed.contains(&child.0) {
                                // Check if child has ANY active parent *excluding* current node_id
                                let parents = parents_map.get(child).unwrap_or(&empty_vec);
                                let has_other_active_parent = parents
                                    .iter()
                                    .any(|p| *p != node_id && active_nodes.contains(p));

                                if !has_other_active_parent && active_nodes.remove(child) {
                                    // Child is pruned. Now check its children.
                                    if let Some(descendants) = self.abstract_graph.edges.get(child)
                                    {
                                        for desc in descendants {
                                            to_prune.push(*desc);
                                        }
                                    }
                                }
                            }
                        }

                        // 2. Recursively prune descendants
                        while let Some(pruned_id) = to_prune.pop() {
                            // Only prune if NO active parents remain
                            let parents = parents_map.get(&pruned_id).unwrap_or(&empty_vec);
                            let has_active_parent =
                                parents.iter().any(|p| active_nodes.contains(p));

                            if !has_active_parent && active_nodes.remove(&pruned_id) {
                                // If the node was active and is now pruned, schedule its children for check
                                if let Some(descendants) = self.abstract_graph.edges.get(&pruned_id)
                                {
                                    for desc in descendants {
                                        to_prune.push(*desc);
                                    }
                                }
                            }
                        }
                    }
                }
                FlowControl::Abort => {
                    // Set next_pc beyond the last block to exit the outer while loop
                    *next_pc = usize::MAX;
                    return Ok(true);
                }
                FlowControl::Continue => {}
            }
        }
        Ok(false)
    }

    /// Checks for cycles in the abstract graph, and partitions the graph into blocks.
    /// - Groups nodes into blocks, creating a new block whenever a conditional node / loop is encountered
    ///
    /// Returns true if the graph contains a cycle, false otherwise.
    pub async fn check_loop_and_partition(&mut self) -> bool {
        // Check for cycles and get topological sort
        let sorted_nodes = match self.abstract_graph.get_topological_sort() {
            Some(nodes) => nodes,
            None => return true,
        };

        // Split into blocks based on conditional nodes
        let mut current_block = HashSet::new();
        self.blocks.clear();
        self.node_block_map.clear();

        for node_id in sorted_nodes {
            if let Some(unfolded_nodes) = self.abstract_graph.unfold_node(node_id) {
                // Create new block for unfolded nodes
                if !current_block.is_empty() {
                    self.blocks.push(current_block);
                    current_block = HashSet::new();
                }

                for node_id in unfolded_nodes {
                    current_block.insert(*node_id);
                }
                self.blocks.push(current_block);
                current_block = HashSet::new();
            } else {
                current_block.insert(node_id);

                // Create new block if conditional node / loop encountered
                let node = self.nodes.get(&node_id).unwrap();
                // Use an async lock here to avoid blocking the runtime
                let node_guard = node.lock().await;
                if node_guard.is_condition() {
                    self.blocks.push(current_block);
                    current_block = HashSet::new();
                }
            }
        }

        // Add any remaining nodes to final block
        if !current_block.is_empty() {
            self.blocks.push(current_block);
        }

        // Build node_block_map
        for (i, block) in self.blocks.iter().enumerate() {
            for node_id in block {
                self.node_block_map.insert(*node_id, i);
            }
        }

        debug!("Split the graph into blocks: {:?}", self.blocks);

        false
    }

    /// Get the output of all tasks.
    pub fn get_results<T: Send + Sync + 'static>(&self) -> HashMap<NodeId, Option<Arc<T>>> {
        self.execute_states
            .iter()
            .map(|(&id, state)| {
                let output = match state.get_output() {
                    Some(content) => content.into_inner(),
                    None => None,
                };
                (id, output)
            })
            .collect()
    }

    pub fn get_outputs(&self) -> HashMap<NodeId, Output> {
        self.execute_states
            .iter()
            .map(|(&id, state)| {
                let t = state.get_full_output();
                (id, t)
            })
            .collect()
    }

    /// Before the dag starts executing, set the dag's global environment variable.
    pub fn set_env(&mut self, env: EnvVar) {
        self.env = Arc::new(env);
    }

    ///Remove duplicate elements
    fn remove_duplicates<T>(vec: Vec<T>) -> Vec<T>
    where
        T: Eq + Hash + Clone,
    {
        let mut seen = HashSet::new();
        vec.into_iter().filter(|x| seen.insert(x.clone())).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::conditional_node::{Condition, ConditionalNode};
    use crate::node::default_node::DefaultNode;
    use crate::{
        Content, EnvVar, InChannels, Node, NodeName, NodeTable, OutChannels, Output, action::Action,
    };
    use async_trait::async_trait;
    use std::sync::Arc;

    /// An implementation of [`Action`] that returns [`Output::Out`] containing a String "Hello world" from default_node.rs.
    #[derive(Default)]
    pub struct HelloAction;
    #[async_trait]
    impl Action for HelloAction {
        async fn run(&self, _: &mut InChannels, _: &mut OutChannels, _: Arc<EnvVar>) -> Output {
            Output::Out(Some(Content::new("Hello world".to_string())))
        }
    }

    impl HelloAction {
        pub fn new() -> Self {
            Self::default()
        }
    }

    /// Test for execute a graph.
    ///
    /// Step 1: create a graph and two DefaultNode.
    ///
    /// Step 2: add the nodes to graph.
    ///
    /// Step 3: add the edge between Node X and "Node Y.
    ///
    /// Step 4: Run the graph and verify the output saved in the graph structure.

    #[test]
    fn test_graph_execution() {
        let mut graph = Graph::new();
        let mut node_table = NodeTable::new();

        let node_name = "Node X";
        let node = DefaultNode::new(NodeName::from(node_name), &mut node_table);
        let node_id = node.id();

        let node1_name = "Node Y";
        let node1 = DefaultNode::with_action(
            NodeName::from(node1_name),
            HelloAction::new(),
            &mut node_table,
        );
        let node1_id = node1.id();

        graph.add_node(node);
        graph.add_node(node1);

        graph.add_edge(node_id, vec![node1_id]);

        match graph.start() {
            Ok(_) => {
                let out = graph.execute_states[&node1_id].get_output().unwrap();
                let out: &String = out.get().unwrap();
                assert_eq!(out, "Hello world");
            }
            Err(e) => {
                eprintln!("Graph execution failed: {:?}", e);
            }
        }
    }

    /// A test condition that always fails.
    ///
    /// This condition is used in tests to verify the behavior of conditional nodes
    /// when their condition evaluates to false. The `run` method always returns false,
    /// simulating a failing condition.
    struct FailingCondition;
    #[async_trait::async_trait]
    impl Condition for FailingCondition {
        async fn run(&self, _: &mut InChannels, _: &OutChannels, _: Arc<EnvVar>) -> bool {
            false
        }
    }

    /// Step 1: Create a new graph and node table.
    ///
    /// Step 2: Create two nodes - a conditional node that will fail and a hello action node.
    ///
    /// Step 3: Add nodes to graph and set up dependencies between them.
    ///
    /// Step 4: Run the graph and verify the conditional node fails as expected.
    #[test]
    fn test_conditional_execution() {
        let mut graph = Graph::new();
        let mut node_table = NodeTable::new();

        // Create conditional node that will fail
        let node_a_name = "Node A";
        let node_a = ConditionalNode::with_condition(
            NodeName::from(node_a_name),
            FailingCondition,
            &mut node_table,
        );
        let node_a_id = node_a.id();

        // Create hello action node
        let node_b_name = "Node B";
        let node_b = DefaultNode::with_action(
            NodeName::from(node_b_name),
            HelloAction::new(),
            &mut node_table,
        );
        let node_b_id = node_b.id();

        // Add nodes to graph
        graph.add_node(node_a);
        graph.add_node(node_b);

        // Add edge from A to B
        graph.add_edge(node_a_id, vec![node_b_id]);

        // Execute graph
        match graph.start() {
            Ok(_) => {
                // Node A should have failed
                assert!(graph.execute_states[&node_a_id].get_output().is_none());
            }
            Err(e) => {
                assert!(matches!(e, GraphError::ExecutionFailed { .. }));
            }
        }
    }
}
