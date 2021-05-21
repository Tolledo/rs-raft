use std::cell::RefCell;
use std::rc::Rc;
use crate::server::State::{FOLLOWER, CANDIDATE, LEADER};

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
    followers: Vec<Rc<RefCell<&'a mut Server<'a>>>>,
}

struct VoteRequest {
    term: i64,
    candidate_id: i64,
}

struct VoteResponse {
    term: i64,
    vote_granted: bool,
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

    fn add_follower(&mut self, follower: &'a mut Server<'a>) {
        self.followers.push(Rc::new(RefCell::new(follower)))
    }

    fn add_followers(&mut self, followers: Vec<&'a mut Server<'a>>) {
        for f in followers {
            self.add_follower(f)
        }
    }

    fn request_vote(&mut self) {
        self.state = CANDIDATE;
        let next_term = self.current_term + 1;

        let vote_request = VoteRequest {
            term: next_term,
            candidate_id: self.id,
        };

        let mut voted = 0;
        for follower in &self.followers {
            let vote_response = follower.borrow_mut().process_vote_request(&vote_request);

            if vote_response.term > next_term {
                self.state = FOLLOWER;
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
                        vote_granted == false;
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
    use crate::server::Server;
    use crate::server::State::*;

    #[test]
    fn server_new() {
        let server = Server::new(0);
        assert_eq!(0, server.id);
        assert_eq!(FOLLOWER, server.state);
    }

    #[test]
    fn request_vote() {
        let mut leader = Server::new(0);
        let mut follower_1 = Server::new(1);
        let mut follower_2 = Server::new(2);
        let mut follower_3 = Server::new(3);
        let mut follower_4 = Server::new(4);
        let followers = vec![&mut follower_1, &mut follower_2, &mut follower_3, &mut follower_4];
        leader.add_followers(followers);

        leader.request_vote();

        assert_eq!(LEADER, leader.state)
    }
}
