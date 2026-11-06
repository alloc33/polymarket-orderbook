use std::{
    collections::{BTreeSet, HashMap, HashSet},
    num::NonZeroUsize,
};

pub struct Balancer {
    per_socket: NonZeroUsize,
    assigned: HashMap<String, usize>,
    loads: Vec<usize>,
    spare: BTreeSet<usize>,
    pending: Vec<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Placement {
    Existing { socket: usize, asset: String },
    New { socket: usize, assets: Vec<String> },
}

impl Placement {
    pub fn count(&self) -> usize {
        match self {
            Self::Existing { .. } => 1,
            Self::New { assets, .. } => assets.len(),
        }
    }
}

impl Balancer {
    pub fn new(per_socket: NonZeroUsize) -> Self {
        Self {
            per_socket,
            assigned: HashMap::new(),
            loads: Vec::new(),
            spare: BTreeSet::new(),
            pending: Vec::with_capacity(per_socket.get()),
        }
    }

    pub fn add(&mut self, asset: String) -> Option<Placement> {
        if self.assigned.contains_key(&asset) || self.pending.contains(&asset) {
            return None;
        }
        if let Some(&socket) = self.spare.first() {
            self.assigned.insert(asset.clone(), socket);
            self.loads[socket] += 1;
            if self.loads[socket] == self.per_socket.get() {
                self.spare.remove(&socket);
            }
            return Some(Placement::Existing { socket, asset });
        }
        self.pending.push(asset);
        if self.pending.len() < self.per_socket.get() {
            return None;
        }
        self.flush()
    }

    pub fn flush(&mut self) -> Option<Placement> {
        if self.pending.is_empty() {
            return None;
        }
        let socket = self.loads.len();
        let assets =
            std::mem::replace(&mut self.pending, Vec::with_capacity(self.per_socket.get()));
        for asset in &assets {
            self.assigned.insert(asset.clone(), socket);
        }
        self.loads.push(assets.len());
        if assets.len() < self.per_socket.get() {
            self.spare.insert(socket);
        }
        Some(Placement::New { socket, assets })
    }

    pub fn remove(&mut self, asset: &str) -> Option<usize> {
        let socket = self.assigned.remove(asset)?;
        self.loads[socket] -= 1;
        self.spare.insert(socket);
        Some(socket)
    }

    pub fn gone(&self, current: &HashSet<String>) -> Vec<String> {
        self.assigned
            .keys()
            .filter(|asset| !current.contains(*asset))
            .cloned()
            .collect()
    }

    pub fn count(&self) -> usize {
        self.assigned.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn balancer(per_socket: usize) -> Balancer {
        Balancer::new(NonZeroUsize::new(per_socket).unwrap())
    }

    fn owned(assets: &[&str]) -> Vec<String> {
        assets.iter().map(|asset| (*asset).to_owned()).collect()
    }

    #[test]
    fn opens_a_new_socket_every_per_socket_assets() {
        let mut balancer = balancer(2);

        assert_eq!(balancer.add("a".to_owned()), None);
        assert_eq!(
            balancer.add("b".to_owned()),
            Some(Placement::New {
                socket: 0,
                assets: owned(&["a", "b"])
            })
        );
        assert_eq!(balancer.add("c".to_owned()), None);
        assert_eq!(
            balancer.flush(),
            Some(Placement::New {
                socket: 1,
                assets: owned(&["c"])
            })
        );
        assert_eq!(balancer.flush(), None);
        assert_eq!(balancer.count(), 3);
    }

    #[test]
    fn ignores_assets_already_placed_or_pending() {
        let mut balancer = balancer(2);

        assert_eq!(balancer.add("a".to_owned()), None);
        assert_eq!(balancer.add("a".to_owned()), None);
        balancer.flush();
        assert_eq!(balancer.add("a".to_owned()), None);
        assert_eq!(balancer.count(), 1);
    }

    #[test]
    fn fills_spare_capacity_before_opening_sockets() {
        let mut balancer = balancer(2);
        balancer.add("a".to_owned());
        balancer.add("b".to_owned());
        balancer.add("c".to_owned());
        balancer.flush();

        assert_eq!(
            balancer.add("d".to_owned()),
            Some(Placement::Existing {
                socket: 1,
                asset: "d".to_owned()
            })
        );
        assert_eq!(balancer.add("e".to_owned()), None);
        assert_eq!(balancer.count(), 4);
    }

    #[test]
    fn removed_assets_free_their_socket() {
        let mut balancer = balancer(2);
        balancer.add("a".to_owned());
        balancer.add("b".to_owned());

        assert_eq!(balancer.remove("a"), Some(0));
        assert_eq!(balancer.remove("a"), None);
        assert_eq!(
            balancer.add("c".to_owned()),
            Some(Placement::Existing {
                socket: 0,
                asset: "c".to_owned()
            })
        );
        assert_eq!(balancer.count(), 2);
    }

    #[test]
    fn gone_lists_placed_assets_missing_from_current() {
        let mut balancer = balancer(2);
        balancer.add("a".to_owned());
        balancer.add("b".to_owned());
        let current = HashSet::from(["b".to_owned(), "z".to_owned()]);

        assert_eq!(balancer.gone(&current), owned(&["a"]));
    }
}
