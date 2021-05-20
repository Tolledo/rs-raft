use std::cell::RefCell;
use std::rc::Rc;

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
            self.followers.push(Rc::new(RefCell::new(f)))
        }
    }

    fn request_vote(self) -> bool {
        let vote_request = VoteRequest {
            term: self.current_term + 1,
            candidate_id: self.id,
        };
        let mut voted = 0;
        for follower in &self.followers {
            let vote_response = follower.borrow_mut().process_vote_request(&vote_request);
            if vote_response.vote_granted {
                voted += 1;
            }
        }
        return voted >= self.followers.len() / 2;
    }

    fn process_vote_request(&mut self, vote_request: &VoteRequest) -> VoteResponse {
        if vote_request.term < self.current_term {
            return VoteResponse {
                term: self.current_term,
                vote_granted: false,
            };
        };
        return match self.voted_for {
            Some(candidate_id) => {
                if candidate_id == vote_request.candidate_id {
                    self.current_term = vote_request.term;
                    VoteResponse {
                        term: vote_request.term,
                        vote_granted: false,
                    }
                } else {
                    VoteResponse {
                        term: vote_request.term,
                        vote_granted: false,
                    }
                }
            }
            None => {
                self.voted_for = Option::from(vote_request.candidate_id);
                self.current_term = vote_request.term;
                VoteResponse {
                    term: vote_request.term,
                    vote_granted: true,
                }
            }
        };
    }
}

#[cfg(test)]
mod tests {
    use crate::server::Server;
    use crate::server::State::FOLLOWER;

    #[test]
    fn server_new() {
        let server = Server::new(0);
        assert_eq!(server.id, 0);
        assert_eq!(server.state, FOLLOWER);
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

        assert!(leader.request_vote())
    }
}
