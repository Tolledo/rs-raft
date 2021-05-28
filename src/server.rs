use futures::stream::{FuturesUnordered, StreamExt};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, oneshot, Mutex};

pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, PartialEq)]
enum ElectionState {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug)]
struct State {
    election_state: ElectionState,
    current_term: i64,
    voted_for: Option<i64>,
}

struct Server {
    id: i64,
    state: Arc<Mutex<State>>,
    rx: Receiver<Command>,
    tx: Sender<Command>,
    nodes: Vec<Sender<Command>>,
}

#[derive(Clone)]
struct VoteRequest {
    term: i64,
    candidate_id: i64,
}

#[derive(Clone)]
struct VoteResponse {
    term: i64,
    vote_granted: bool,
}

enum Command {
    Vote {
        request: VoteRequest,
        resp: oneshot::Sender<VoteResponse>,
    },
    RequestVote {},
    ShutDown {},
}

impl Server {
    fn new(id: i64) -> Self {
        let (tx, rx) = mpsc::channel(32);
        Server {
            state: Arc::new(Mutex::new(State {
                election_state: ElectionState::Follower,
                current_term: 0,
                voted_for: None,
            })),
            id,
            rx,
            tx,
            nodes: vec![],
        }
    }

    fn add_node(&mut self, node: Sender<Command>) {
        self.nodes.push(node)
    }

    fn add_nodes(&mut self, nodes: Vec<Sender<Command>>) {
        for node in nodes {
            self.add_node(node)
        }
    }
}

async fn start(
    mut rx: Receiver<Command>,
    id: i64,
    nodes: Vec<Sender<Command>>,
    self_state: Arc<Mutex<State>>,
) {
    use Command::*;

    println!("Starting Server id {}", id);
    loop {
        match rx.recv().await {
            Some(cmd) => match cmd {
                RequestVote {} => {
                    let self_state = self_state.clone();
                    let nodes = nodes.clone();
                    tokio::spawn(async move {
                        request_vote(id, nodes, self_state).await;
                    });
                }
                Vote { request, resp } => {
                    let self_state = self_state.clone();
                    tokio::spawn(async move {
                        let res = process_vote_request(self_state, &request).await;
                        match resp.send(res) {
                            Ok(_) => {}
                            Err(_) => {}
                        }
                    });
                }
                ShutDown { .. } => {
                    return;
                }
            },
            None => {
                return;
            }
        }
    }
}

async fn request_vote(id: i64, nodes: Vec<Sender<Command>>, self_state: Arc<Mutex<State>>) {
    let mut self_state = self_state.lock().await;
    self_state.election_state = ElectionState::Candidate;
    self_state.current_term += 1;
    self_state.voted_for = Option::from(id);

    let vote_request = VoteRequest {
        term: self_state.current_term,
        candidate_id: id,
    };

    let mut voted = 0;

    let mut receivers = FuturesUnordered::new();
    for node in &nodes {
        let (resp_tx, resp_rx) = oneshot::channel();
        let cmd = Command::Vote {
            request: vote_request.clone(),
            resp: resp_tx,
        };
        receivers.push(resp_rx);
        node.send(cmd).await;
    }

    while let Some(result) = receivers.next().await {
        match result {
            Ok(vote_response) => {
                if vote_response.term > self_state.current_term {
                    self_state.election_state = ElectionState::Follower;
                    self_state.current_term = vote_response.term;
                    return;
                }

                if vote_response.vote_granted {
                    voted += 1
                }
            }
            Err(_) => {}
        }
    }

    if voted >= nodes.len() / 2 {
        self_state.election_state = ElectionState::Leader
    }
}

async fn process_vote_request(
    self_state: Arc<Mutex<State>>,
    vote_request: &VoteRequest,
) -> VoteResponse {
    let mut vote_granted = false;
    let mut term = 0;
    let mut self_state = self_state.lock().await;

    if vote_request.term < self_state.current_term {
        term = self_state.current_term;
        vote_granted = false;
    };

    if vote_request.term == self_state.current_term {
        match self_state.voted_for {
            Some(id) => {
                if id == vote_request.candidate_id {
                    vote_granted = true;
                    term = vote_request.term;
                } else {
                    vote_granted = false;
                }
            }
            // Should probably never happen, but just in case...
            None => {
                self_state.voted_for = Option::from(vote_request.candidate_id);
                self_state.current_term = vote_request.term;
                vote_granted = true;
                term = vote_request.term;
            }
        }
    }

    if vote_request.term > self_state.current_term {
        self_state.voted_for = Option::from(vote_request.candidate_id);
        self_state.current_term = vote_request.term;
        vote_granted = true;
        term = vote_request.term;
    }

    VoteResponse { term, vote_granted }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn server_new() {
        let server = Server::new(0);
        assert_eq!(0, server.id);
        assert_eq!(
            ElectionState::Follower,
            server.state.lock().await.election_state
        );
    }

    #[tokio::test]
    async fn request_vote_win_election() {
        let mut server = Server::new(0);
        let node_1 = Server::new(1);
        let node_2 = Server::new(2);
        let node_3 = Server::new(3);
        let node_4 = Server::new(4);
        let nodes = vec![
            node_1.tx.clone(),
            node_2.tx.clone(),
            node_3.tx.clone(),
            node_4.tx.clone(),
        ];
        server.add_nodes(nodes);
        let state = server.state.clone();

        let nodes = server.nodes.clone();
        let cloned_tx = server.tx.clone();
        let shutdown = tokio::spawn(async move {
            sleep(Duration::from_millis(100)).await;
            for node in nodes {
                node.send(Command::ShutDown {}).await;
            }
            cloned_tx.send(Command::ShutDown {}).await;
        });

        tokio::join!(
            shutdown,
            server.tx.send(Command::RequestVote {}),
            start(
                server.rx,
                server.id,
                server.nodes.clone(),
                server.state.clone()
            ),
            start(
                node_1.rx,
                node_1.id,
                node_1.nodes.clone(),
                node_1.state.clone()
            ),
            start(
                node_2.rx,
                node_2.id,
                node_2.nodes.clone(),
                node_2.state.clone()
            ),
            start(
                node_3.rx,
                node_3.id,
                node_3.nodes.clone(),
                node_3.state.clone()
            ),
            start(
                node_4.rx,
                node_4.id,
                node_4.nodes.clone(),
                node_4.state.clone()
            ),
        );

        let server_state = state.lock().await;
        assert_eq!(ElectionState::Leader, server_state.election_state);
        assert_eq!(1, server_state.current_term);
    }
}
