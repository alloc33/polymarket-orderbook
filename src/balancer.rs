use std::{collections::HashSet, num::NonZeroUsize};

pub struct Balancer {
    per_socket: NonZeroUsize,
    known: HashSet<String>,
    pending: Vec<String>,
}

impl Balancer {
    pub fn new(per_socket: NonZeroUsize) -> Self {
        Self {
            per_socket,
            known: HashSet::new(),
            pending: Vec::with_capacity(per_socket.get()),
        }
    }

    pub fn add(&mut self, asset: String) -> Option<Vec<String>> {
        if !self.known.insert(asset.clone()) {
            return None;
        }
        self.pending.push(asset);
        if self.pending.len() < self.per_socket.get() {
            return None;
        }
        self.flush()
    }

    pub fn flush(&mut self) -> Option<Vec<String>> {
        if self.pending.is_empty() {
            return None;
        }
        Some(std::mem::replace(
            &mut self.pending,
            Vec::with_capacity(self.per_socket.get()),
        ))
    }

    pub fn count(&self) -> usize {
        self.known.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn balancer(per_socket: usize) -> Balancer {
        Balancer::new(NonZeroUsize::new(per_socket).unwrap())
    }

    #[test]
    fn emits_a_batch_every_per_socket_assets() {
        let mut balancer = balancer(2);

        assert_eq!(balancer.add("a".to_owned()), None);
        assert_eq!(
            balancer.add("b".to_owned()),
            Some(vec!["a".to_owned(), "b".to_owned()])
        );
        assert_eq!(balancer.add("c".to_owned()), None);
        assert_eq!(balancer.flush(), Some(vec!["c".to_owned()]));
        assert_eq!(balancer.flush(), None);
        assert_eq!(balancer.count(), 3);
    }

    #[test]
    fn ignores_assets_seen_before() {
        let mut balancer = balancer(2);

        assert_eq!(balancer.add("a".to_owned()), None);
        assert_eq!(balancer.add("a".to_owned()), None);
        assert_eq!(balancer.flush(), Some(vec!["a".to_owned()]));
        assert_eq!(balancer.count(), 1);
    }
}
