use async_trait::async_trait;
use futures::stream::{FuturesUnordered, StreamExt};
use futures::{
    future::FutureExt, // for `.fuse()`
    pin_mut,
    select,
};
use mockall::automock;
use tokio::sync::{mpsc, oneshot};
use tokio::sync::mpsc::{Receiver, Sender};


#[derive(Clone, Debug, PartialEq)]
enum State {
    Follower,
    Candidate,
    Leader,
}

struct Server {
    state: State,
    id: i64,
    current_term: i64,
    voted_for: Option<i64>,
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
}

#[async_trait]
#[automock]
trait Node: Send + Sync {
    async fn request_vote(&mut self);

    async fn process_vote_request(&mut self, vote_request: &VoteRequest) -> VoteResponse;
}

impl Server {
    fn new(id: i64) -> Self {
        let (tx, rx) = mpsc::channel(32);
        Server {
            state: State::Follower,
            id,
            current_term: 0,
            voted_for: None,
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

    async fn start(&mut self) {
        loop {
            use Command::*;

            match self.rx.recv().await {
                Some(cmd) => {
                    match cmd {
                        Vote { request, resp } => {
                            let res = self.process_vote_request(&request).await;
                            resp.send(res);
                        }
                    }
                }
                None => {}
            }
        }
    }
}

#[async_trait]
impl Node for Server {
    async fn request_vote(&mut self) {
        self.state = State::Candidate;
        self.current_term += 1;
        self.voted_for = Option::from(self.id);

        let vote_request = VoteRequest {
            term: self.current_term,
            candidate_id: self.id,
        };

        let mut voted = 0;

        let mut receivers = FuturesUnordered::new();
        for node in &self.nodes {
            let (resp_tx, resp_rx) = oneshot::channel();
            let cmd = Command::Vote {
                request: vote_request.clone(),
                resp: resp_tx,
            };
            receivers.push(resp_rx);
            node.send(cmd);
        }

        while let Some(result) = receivers.next().await {
            match result {
                Ok(vote_response) => {
                    if vote_response.term > self.current_term {
                        self.state = State::Follower;
                        self.current_term = vote_response.term;
                        return;
                    }

                    if vote_response.vote_granted {
                        voted += 1
                    }
                }
                Err(_) => {}
            }
        }

        if voted >= self.nodes.len() / 2 {
            self.state = State::Leader
        }
    }

    async fn process_vote_request(&mut self, vote_request: &VoteRequest) -> VoteResponse {
        let mut vote_granted = false;
        let mut term = 0;

        if vote_request.term < self.current_term {
            term = self.current_term;
            vote_granted = false;
        };

        if vote_request.term == self.current_term {
            match self.voted_for {
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
                    self.voted_for = Option::from(vote_request.candidate_id);
                    self.current_term = vote_request.term;
                    vote_granted = true;
                    term = vote_request.term;
                }
            }
        }

        if vote_request.term > self.current_term {
            self.voted_for = Option::from(vote_request.candidate_id);
            self.current_term = vote_request.term;
            vote_granted = true;
            term = vote_request.term;
        }

        VoteResponse { term, vote_granted }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_new() {
        let server = Server::new(0);
        assert_eq!(0, server.id);
        assert_eq!(State::Follower, server.state);
    }

    #[tokio::test]
    async fn request_vote_win_election() {
        let mut server = Server::new(0);
        let mut node_1 = Server::new(1);
        let mut node_2 = Server::new(2);
        let mut node_3 = Server::new(3);
        let mut node_4 = Server::new(4);
        let nodes =
            vec![node_1.tx.clone(), node_2.tx.clone(), node_3.tx.clone(), node_4.tx.clone()];
        server.add_nodes(nodes);

        select! {
            () = server.start().fuse() => (),
            () = server.request_vote().fuse() => (),
        }

        assert_eq!(State::Leader, server.state);
        assert_eq!(1, server.current_term);
    }

    // #[tokio::test]
    // async fn request_vote_higher_term() {
    //     let mut server = Server::new(0);
    //     let mut node_mock = MockNode::new();
    //     node_mock
    //         .expect_process_vote_request()
    //         .return_const(VoteResponse {
    //             term: 42,
    //             vote_granted: false,
    //         });
    //     let mut node_1 = Server::new(1);
    //     let mut node_2 = Server::new(2);
    //     let mut node_3 = Server::new(3);
    //     let followers: Vec<&mut dyn Node> =
    //         vec![&mut node_mock, &mut node_1, &mut node_2, &mut node_3];
    //     server.add_followers(followers);
    //
    //     server.request_vote().await;
    //
    //     assert_eq!(State::Follower, server.state);
    //     assert_eq!(42, server.current_term);
    // }
}
