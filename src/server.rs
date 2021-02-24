use std::collections::HashMap;

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
    followers: Vec<&'a Server<'a>>,
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

    fn add_follower(&mut self, follower: &'a Server<'a>) {
        self.followers.push(follower)
    }

    fn add_followers(&mut self, followers: &mut Vec<&'a Server<'a>>) {
        self.followers.append(followers)
    }

    fn request_vote(mut self) -> bool {
        let vote_request = VoteRequest {
            term: self.current_term + 1,
            candidate_id: self.id,
        };
        let counters = self
            .followers
            .iter_mut()
            .map(|f| f.process_vote_request(&vote_request))
            .fold(HashMap::new(), |mut acc, resp| {
                *acc.entry(resp.vote_granted).or_insert(0) += 1;
                acc
            });

        match counters.get(&true) {
            Some(count) => *count >= self.followers.len() / 2,
            None => false,
        }
    }

    //TODO fix issues with borrow checker. Add tokio and try using channels (or something else?)
    fn process_vote_request(&mut self, vote_request: &VoteRequest) -> VoteResponse {
        if vote_request.term < self.current_term {
            return VoteResponse {
                term: self.current_term,
                vote_granted: false,
            };
        };
        let response = return match self.voted_for {
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
        response
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
        let follower_1 = Server::new(1);
        let follower_2 = Server::new(2);
        let follower_3 = Server::new(3);
        let follower_4 = Server::new(4);
        let mut followers = vec![&follower_1, &follower_2, &follower_3, &follower_4];
        leader.add_followers(&mut followers);

        assert!(leader.request_vote())
    }
}
