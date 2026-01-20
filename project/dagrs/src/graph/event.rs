use crate::node::NodeId;

/// Events emitted during graph execution
#[derive(Clone, Debug)]
pub enum GraphEvent {
    NodeStart { id: NodeId, timestamp: u64 },
    NodeSuccess { id: NodeId },
    NodeFailed { id: NodeId, error: String },
    NodeSkipped { id: NodeId },
    GraphFinished,
}
