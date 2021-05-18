//! Data structures to track outgoing `Publish` messages as well as a set of `pkid`s.

use crate::Publish;

/// Uses a bucket list for storage with `pkid` as index into it.
#[derive(Debug, Clone)]
pub struct OutgoingPublishBucketList {
    vec: Vec<Option<Publish>>,
    len: usize,
}

#[derive(Debug, Copy, Clone)]
pub struct OutOfBounds(pub u16);

impl OutgoingPublishBucketList {
    pub fn with_limit(max_pkid: u16) -> Self {
        Self {
            vec: vec![None; max_pkid as usize + 1],
            len: 0,
        }
    }

    /// Returns the number of `Some(Publish)` items in the bucket list.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Removes all items from the list and inserts the `Some` items into `vec` after mapping with
    /// `map`.
    pub fn drain_into<T>(&mut self, vec: &mut Vec<T>, map: impl Fn(Publish) -> T) {
        for opt in self.vec.iter_mut() {
            if let Some(v) = opt.take() {
                vec.push(map(v));
            }
        }
        self.len = 0;
    }

    /// Inserts `value` into the bucket list returning the previous value, unless the `pkid` is out
    /// of bounds.
    ///
    /// Returns `Err(OutOfBounds)` in case the `pkid` of the Publish is out of bounds.
    pub fn insert(&mut self, value: Publish) -> Result<Option<Publish>, OutOfBounds> {
        let index = value.pkid as usize;
        if let Some(elem) = self.vec.get_mut(index) {
            let old = elem.replace(value);
            if old.is_none() {
                self.len += 1;
            }
            Ok(old)
        } else {
            Err(OutOfBounds(value.pkid))
        }
    }

    /// Removes the bucket at index `pkid` returning it's value.
    ///
    /// Returns `Err(OutOfBounds)` if `pkid` is out of bounds.
    pub fn remove(&mut self, pkid: u16) -> Result<Option<Publish>, OutOfBounds> {
        if let Some(elem) = self.vec.get_mut(pkid as usize) {
            let old = elem.take();
            if old.is_some() {
                self.len -= 1;
            }
            Ok(old)
        } else {
            Err(OutOfBounds(pkid))
        }
    }

    /// Returns a reference to the Publish paket with `pkid`.
    ///
    /// Returns `Err(OutOfBounds)` if `pkid` is out of bounds.
    pub fn get(&self, pkid: u16) -> Result<Option<&Publish>, OutOfBounds> {
        self.vec
            .get(pkid as usize)
            .ok_or(OutOfBounds(pkid))
            .map(Option::as_ref)
    }
}

/// A `set` of `pkid`'s.
///
/// Uses a bitset to determine if a `pkid` is in use or not.
#[derive(Debug, Clone)]
pub struct PkidSet {
    set: fixedbitset::FixedBitSet,
    len: usize,
}

impl PkidSet {
    pub fn full_range() -> Self {
        Self::with_limit(std::u16::MAX)
    }

    pub fn with_limit(max_pkid: u16) -> Self {
        Self {
            set: fixedbitset::FixedBitSet::with_capacity(max_pkid as usize + 1),
            len: 0,
        }
    }

    /// Returns the number of `pkid`'s in the set.
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn drain_into<T>(&mut self, vec: &mut Vec<T>, map: impl Fn(u16) -> T) {
        for pkid in self.set.ones() {
            vec.push(map(pkid as u16));
        }
        self.clear();
    }

    /// Empties the bitset.
    pub fn clear(&mut self) {
        self.set.clear();
        self.len = 0;
    }

    /// Inserts `pkid` into the set returning the previous value.
    pub fn insert(&mut self, pkid: u16) -> Result<bool, OutOfBounds> {
        let index = pkid as usize;
        if index < self.set.len() {
            let old = self.set.put(index);
            if old == false {
                self.len += 1;
            }
            Ok(old)
        } else {
            Err(OutOfBounds(pkid))
        }
    }

    #[cfg(test)]
    pub fn contains(&self, pkid: u16) -> bool {
        self.set.contains(pkid as usize)
    }

    /// Removes `pkid` from the set returning the value or `Err(OutOfBounds)`.
    pub fn remove(&mut self, pkid: u16) -> Result<bool, OutOfBounds> {
        let index = pkid as usize;
        if index < self.set.len() {
            let old = self.set.contains(index);
            self.set.set(index, false);
            if old {
                self.len -= 1;
            }
            Ok(old)
        } else {
            Err(OutOfBounds(pkid))
        }
    }
}
