use crate::node::NodeId;
use std::any::Any;
use std::sync::Arc;

/// Control flow instructions for graph execution
#[derive(Debug, Clone)]
pub enum FlowControl {
    /// Continue execution normally
    Continue,
    /// Loop: request to jump to a specific block index (usually backwards)
    Loop(LoopInstruction),
    /// Branch: specify downstream node IDs that should be activated
    Branch(Vec<NodeId>),
    /// Abort: stop graph execution immediately
    Abort,
}

/// Instruction to perform a loop in the graph execution
#[derive(Debug, Clone)]
pub struct LoopInstruction {
    pub jump_to_block_index: usize,
    pub context: Option<Arc<dyn Any + Send + Sync>>,
}

impl FlowControl {
    /// Create a loop instruction to jump to a specific block index without context
    pub fn loop_to(index: usize) -> Self {
        Self::Loop(LoopInstruction {
            jump_to_block_index: index,
            context: None,
        })
    }

    /// Create a loop instruction to jump to a specific block index with context
    pub fn loop_with_context<T: Any + Send + Sync>(index: usize, context: T) -> Self {
        Self::Loop(LoopInstruction {
            jump_to_block_index: index,
            context: Some(Arc::new(context)),
        })
    }
}
