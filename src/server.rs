use std::cell::RefCell;
use std::rc::Rc;
use crate::server::State::{FOLLOWER, CANDIDATE, LEADER};
use mockall::automock;

#[derive(Debug, PartialEq)]
enum State {
    FOLLOWER,
    CANDIDATE,
    LEADER,
}

struct Server<'a> {
    state: State,
    id: i64,
    current_term: i64,
    voted_for: Option<i64>,
    followers: Vec<Rc<RefCell<&'a mut dyn Node>>>,
}

struct VoteRequest {
    term: i64,
    candidate_id: i64,
}

#[derive(Clone)]
struct VoteResponse {
    term: i64,
    vote_granted: bool,
}

#[automock]
trait Node {
    fn request_vote(&mut self);

    fn process_vote_request(&mut self, vote_request: &VoteRequest) -> VoteResponse;
}

impl<'a> Server<'a> {
    fn new(id: i64) -> Self {
        Server {
            state: State::FOLLOWER,
            id,
            current_term: 0,
            voted_for: None,
            followers: vec![],
        }
    }

    fn add_follower(&mut self, follower: &'a mut dyn Node) {
        self.followers.push(Rc::new(RefCell::new(follower)))
    }

    fn add_followers(&mut self, followers: Vec<&'a mut dyn Node>) {
        for f in followers {
            self.add_follower(f)
        }
    }
}

impl<'a> Node for Server<'a> {
    fn request_vote(&mut self) {
        self.state = CANDIDATE;
        self.current_term += 1;
        self.voted_for = Option::from(self.id);

        let vote_request = VoteRequest {
            term: self.current_term,
            candidate_id: self.id,
        };

        let mut voted = 0;
        for follower in &self.followers {
            let vote_response = follower.borrow_mut().process_vote_request(&vote_request);

            if vote_response.term > self.current_term {
                self.state = FOLLOWER;
                self.current_term = vote_response.term;
                return;
            }

            if vote_response.vote_granted {
                voted += 1
            }
        }
        if voted >= self.followers.len() / 2 {
            self.state = LEADER
        }
    }

    fn process_vote_request(&mut self, vote_request: &VoteRequest) -> VoteResponse {
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

        return VoteResponse {
            term,
            vote_granted,
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_new() {
        let server = Server::new(0);
        assert_eq!(0, server.id);
        assert_eq!(FOLLOWER, server.state);
    }

    #[test]
    fn request_vote_win_election() {
        let mut server = Server::new(0);
        let mut node_1 = Server::new(1);
        let mut node_2 = Server::new(2);
        let mut node_3 = Server::new(3);
        let mut node_4 = Server::new(4);
        let followers: Vec<&mut dyn Node> = vec![&mut node_1, &mut node_2, &mut node_3, &mut node_4];
        server.add_followers(followers);

        server.request_vote();

        assert_eq!(LEADER, server.state);
        assert_eq!(1, server.current_term);
    }

    #[test]
    fn request_vote_higher_term() {
        let mut server = Server::new(0);
        let mut node_mock=  MockNode::new();
        node_mock.expect_process_vote_request()
            .return_const(VoteResponse{
                term: 42,
                vote_granted: false
            });
        let mut node_1 = Server::new(1);
        let mut node_2 = Server::new(2);
        let mut node_3 = Server::new(3);
        let followers: Vec<&mut dyn Node> = vec![&mut node_mock, &mut node_1, &mut node_2, &mut node_3];
        server.add_followers(followers);

        server.request_vote();

        assert_eq!(FOLLOWER, server.state);
        assert_eq!(42, server.current_term);
    }
}
