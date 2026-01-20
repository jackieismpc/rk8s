use async_trait::async_trait;
use dagrs::node::action::Action;
use dagrs::node::default_node::DefaultNode;
use dagrs::node::router_node::{Router, RouterNode};
use dagrs::{EnvVar, Graph, InChannels, Node, NodeId, NodeTable, OutChannels, Output};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct MarkAction {
    name: String,
    executed: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl Action for MarkAction {
    async fn run(&self, _: &mut InChannels, out: &mut OutChannels, _: Arc<EnvVar>) -> Output {
        self.executed.lock().unwrap().push(self.name.clone());
        out.broadcast(dagrs::Content::new(self.name.clone())).await;
        Output::empty()
    }
}

struct StaticRouter {
    target: Arc<Mutex<NodeId>>,
}

#[async_trait]
impl Router for StaticRouter {
    async fn route(&self, _: &mut InChannels, out: &OutChannels, _: Arc<EnvVar>) -> Vec<usize> {
        let t = *self.target.lock().unwrap();
        let _ = out
            .send_to(&t, dagrs::Content::new("ping".to_string()))
            .await;
        vec![t.as_usize()]
    }
}

#[test]
fn test_branch_pruning() {
    let mut graph = Graph::new();
    let mut table = NodeTable::new();
    let executed = Arc::new(Mutex::new(Vec::new()));

    // Topology:
    // Router -> A -> B
    //        -> C -> D
    //
    // If Router chooses A, then C and D should NOT run.

    let action_a = MarkAction {
        name: "A".to_string(),
        executed: executed.clone(),
    };
    let node_a = DefaultNode::with_action("A".to_string(), action_a, &mut table);
    let id_a = node_a.id();

    let action_b = MarkAction {
        name: "B".to_string(),
        executed: executed.clone(),
    };
    let node_b = DefaultNode::with_action("B".to_string(), action_b, &mut table);
    let id_b = node_b.id();

    let action_c = MarkAction {
        name: "C".to_string(),
        executed: executed.clone(),
    };
    let node_c = DefaultNode::with_action("C".to_string(), action_c, &mut table);
    let id_c = node_c.id();

    let action_d = MarkAction {
        name: "D".to_string(),
        executed: executed.clone(),
    };
    let node_d = DefaultNode::with_action("D".to_string(), action_d, &mut table);
    let id_d = node_d.id();

    let target = Arc::new(Mutex::new(id_a));
    let router = RouterNode::new(
        "Router".to_string(),
        StaticRouter {
            target: target.clone(),
        },
        &mut table,
    );
    let id_router = router.id();

    graph.add_node(router);
    graph.add_node(node_a);
    graph.add_node(node_b);
    graph.add_node(node_c);
    graph.add_node(node_d);

    // Edges
    graph.add_edge(id_router, vec![id_a, id_c]); // Router connects to A and C
    graph.add_edge(id_a, vec![id_b]); // A -> B
    graph.add_edge(id_c, vec![id_d]); // C -> D

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        graph.async_start().await.unwrap();
    });

    let exec_log = executed.lock().unwrap();
    println!("Executed nodes: {:?}", *exec_log);

    // Expect: A, B.
    // Should NOT contain: C, D.
    assert!(exec_log.contains(&"A".to_string()));
    assert!(exec_log.contains(&"B".to_string()));
    assert!(
        !exec_log.contains(&"C".to_string()),
        "Node C should be pruned"
    );
    assert!(
        !exec_log.contains(&"D".to_string()),
        "Node D should be pruned (descendant of C)"
    );
}
