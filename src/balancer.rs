//! Assigns assets to socket groups, filling spare capacity before opening new ones.

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    num::NonZeroUsize,
};

pub struct Balancer {
    per_group: NonZeroUsize,
    assigned: HashMap<String, usize>,
    loads: Vec<usize>,
    spare: BTreeSet<usize>,
    pending: Vec<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Placement {
    Existing { group: usize, asset: String },
    New(Vec<String>),
}

impl Balancer {
    pub fn new(per_group: NonZeroUsize) -> Self {
        Self {
            per_group,
            assigned: HashMap::new(),
            loads: Vec::new(),
            spare: BTreeSet::new(),
            pending: Vec::with_capacity(per_group.get()),
        }
    }

    pub fn add(&mut self, asset: String) -> Option<Placement> {
        if self.assigned.contains_key(&asset) || self.pending.contains(&asset) {
            return None;
        }
        if let Some(&group) = self.spare.first() {
            self.assigned.insert(asset.clone(), group);
            self.loads[group] += 1;
            if self.loads[group] == self.per_group.get() {
                self.spare.remove(&group);
            }
            return Some(Placement::Existing { group, asset });
        }
        self.pending.push(asset);
        if self.pending.len() < self.per_group.get() {
            return None;
        }
        self.flush()
    }

    pub fn flush(&mut self) -> Option<Placement> {
        if self.pending.is_empty() {
            return None;
        }
        let group = self.loads.len();
        let assets = std::mem::replace(&mut self.pending, Vec::with_capacity(self.per_group.get()));
        for asset in &assets {
            self.assigned.insert(asset.clone(), group);
        }
        self.loads.push(assets.len());
        if assets.len() < self.per_group.get() {
            self.spare.insert(group);
        }
        Some(Placement::New(assets))
    }

    pub fn remove(&mut self, asset: &str) -> Option<usize> {
        let group = self.assigned.remove(asset)?;
        self.loads[group] -= 1;
        self.spare.insert(group);
        Some(group)
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

    fn balancer(per_group: usize) -> Balancer {
        Balancer::new(NonZeroUsize::new(per_group).unwrap())
    }

    fn owned(assets: &[&str]) -> Vec<String> {
        assets.iter().map(|asset| (*asset).to_owned()).collect()
    }

    #[test]
    fn opens_a_new_group_every_per_group_assets() {
        let mut balancer = balancer(2);

        assert_eq!(balancer.add("a".to_owned()), None);
        assert_eq!(
            balancer.add("b".to_owned()),
            Some(Placement::New(owned(&["a", "b"])))
        );
        assert_eq!(balancer.add("c".to_owned()), None);
        assert_eq!(balancer.flush(), Some(Placement::New(owned(&["c"]))));
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
    fn fills_spare_capacity_before_opening_groups() {
        let mut balancer = balancer(2);
        balancer.add("a".to_owned());
        balancer.add("b".to_owned());
        balancer.add("c".to_owned());
        balancer.flush();

        assert_eq!(
            balancer.add("d".to_owned()),
            Some(Placement::Existing {
                group: 1,
                asset: "d".to_owned()
            })
        );
        assert_eq!(balancer.add("e".to_owned()), None);
        assert_eq!(balancer.count(), 4);
    }

    #[test]
    fn removed_assets_free_their_group() {
        let mut balancer = balancer(2);
        balancer.add("a".to_owned());
        balancer.add("b".to_owned());

        assert_eq!(balancer.remove("a"), Some(0));
        assert_eq!(balancer.remove("a"), None);
        assert_eq!(
            balancer.add("c".to_owned()),
            Some(Placement::Existing {
                group: 0,
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
