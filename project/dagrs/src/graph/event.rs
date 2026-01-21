use crate::node::NodeId;

/// Events emitted during graph execution.
///
/// These events are broadcast via the `event_sender` channel in `Graph`.
/// Consumers can subscribe to these events to monitor the execution progress,
/// collect metrics, or trigger side effects (e.g., logging, UI updates).
///
/// # Handling Events
/// Subscribers receive a `broadcast::Receiver`. Note that if the subscriber
/// cannot keep up with the event rate, they may receive `Lagged` errors.
#[derive(Clone, Debug)]
pub enum GraphEvent {
    /// Emitted when a node starts execution.
    ///
    /// * `id`: The ID of the node that started.
    /// * `timestamp`: The Unix timestamp (in seconds) when execution began.
    NodeStart { id: NodeId, timestamp: u64 },

    /// Emitted when a node completes execution successfully.
    ///
    /// * `id`: The ID of the node that succeeded.
    NodeSuccess { id: NodeId },

    /// Emitted when a node execution fails (returns an error or panics).
    ///
    /// * `id`: The ID of the node that failed.
    /// * `error`: A string description of the error.
    NodeFailed { id: NodeId, error: String },

    /// Emitted when a node is skipped due to conditional logic (e.g., a branch not taken).
    ///
    /// * `id`: The ID of the skipped node.
    /// * `timestamp`: Time when skip decision was made (optional/implied now).
    NodeSkipped { id: NodeId },

    /// Emitted when the entire graph execution finishes (success or failure).
    ///
    /// This is the final event in the stream.
    /// Consumers can use this to signal the end of monitoring.
    GraphFinished,
}
