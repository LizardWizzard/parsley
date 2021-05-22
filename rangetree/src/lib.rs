use std::{fmt::Debug};
use std::{borrow::Borrow, cmp::Ordering};
use thiserror::Error;

pub const B: usize = 6;
pub const CAPACITY: usize = 2 * B - 1;


#[derive(Error, Debug)]
pub enum RangeTreeError {
    #[error("range start is greater than range end")]
    InvalidRange,
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub struct RangeSpec<RangeType, DataType = ()> {
    pub start: RangeType,
    pub end: RangeType,
    pub data: DataType,
}

impl<RangeType: Ord, DataType> RangeSpec<RangeType, DataType> {
    pub fn new(start: RangeType, end: RangeType, data: DataType) -> Result<Self, RangeTreeError> {
        if start > end {
            return Err(RangeTreeError::InvalidRange);
        }
        Ok(Self::trusted_new(start, end, data))
    }

    pub fn trusted_new(start: RangeType, end: RangeType, data: DataType) -> Self {
        Self { start, end, data }
    }
}

impl<RangeType, DataType> Ord for RangeSpec<RangeType, DataType>
where
    RangeType: Ord,
    DataType: Eq + Debug,
{
    fn cmp(&self, other: &Self) -> Ordering {
        if (self.start == other.start) && (self.end == other.end) {
            return Ordering::Equal;
        } else if self.end < other.start {
            return Ordering::Less;
        } else if self.start > other.end {
            return Ordering::Greater;
        }
        unreachable!()
    }
}

impl<RangeType, DataType> PartialOrd for RangeSpec<RangeType, DataType>
where
    RangeType: Ord,
    DataType: Eq + Debug,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// type RangeWithBounds<RangeType, DataType> = Range<RangeType: Ord + Eq + Copy, DataType: Clone>;

// pub struct LeafNode<RangeType, DataType> {
//     // ranges is array in form of ranges starts plus end value of highest range
//     // for example consider ranges 1 3; 5 8; 11 15;
//     // so ranges in this case will contain: 1
//     ranges: [RangeType; CAPACITY], // u8 to range type and back?

//     data: [DataType; CAPACITY]
// }

pub trait Len {
    fn len(&self) -> usize;
}

pub trait ReadonlyRangeMap<RangeType, DataType = ()>:
    IntoIterator<Item = RangeSpec<RangeType, DataType>> + Debug + Clone
where
    RangeType: Ord,
    DataType: Eq + Debug,
{
    fn get<Q>(&self, key: &Q) -> Option<&RangeSpec<RangeType, DataType>>
    where
        Q: ?Sized + Ord + Debug + Len,
        RangeType: Borrow<Q>;
}

pub trait RangeMap<RangeType, DataType = ()>:
    Default + Debug + IntoIterator<Item = RangeSpec<RangeType, DataType>> + Clone
where
    RangeType: Ord + Debug,
    DataType: Eq + Debug,
{
    type FROZEN: ReadonlyRangeMap<RangeType, DataType> + Sync + Send + Debug + PartialEq + Clone;

    fn new() -> Self;

    fn freeze(self) -> Self::FROZEN;

    fn insert(&mut self, range: RangeSpec<RangeType, DataType>) -> (); // TODO overlapping ranges or not?

    // Returns None if no such range?
    fn delete(
        &mut self,
        range: RangeSpec<RangeType, DataType>,
    ) -> Option<RangeSpec<RangeType, DataType>>;
}

#[derive(Debug, PartialEq, Clone)]
pub struct ReadonlyRangeVec<RangeType, DataType = ()>
where
    RangeType: Ord,
    DataType: Eq + Debug,
{
    items: Vec<RangeSpec<RangeType, DataType>>,
}

unsafe impl<RangeType, DataType> Sync for ReadonlyRangeVec<RangeType, DataType>
where
    RangeType: Ord,
    DataType: Eq + Debug,
{
}

unsafe impl<RangeType, DataType> Send for ReadonlyRangeVec<RangeType, DataType>
where
    RangeType: Ord,
    DataType: Eq + Debug,
{
}

// unsafe impl<RangeType, DataDype> Sync for ReadonlyRangeVec<RangeType, DataType> {}

impl<RangeType, DataType> ReadonlyRangeVec<RangeType, DataType>
where
    RangeType: Ord,
    DataType: Eq + Debug,
{
    pub fn new(items: Vec<RangeSpec<RangeType, DataType>>) -> Self {
        Self { items }
    }
}

impl<RangeType, DataType> IntoIterator for ReadonlyRangeVec<RangeType, DataType>
where
    RangeType: Ord,
    DataType: Eq + Debug,
{
    type Item = RangeSpec<RangeType, DataType>;

    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.items.into_iter()
    }
}



impl<RangeType, DataType> ReadonlyRangeMap<RangeType, DataType>
    for ReadonlyRangeVec<RangeType, DataType>
where
    RangeType: Ord + Debug + Clone + Len,
    DataType: Eq + Debug + Clone,
{
    // TODO dedupe impl, copy paste
    fn get<Q>(&self, key: &Q) -> Option<&RangeSpec<RangeType, DataType>>
    where
        RangeType: Borrow<Q>,
        Q: ?Sized + Ord + Eq + Debug + Len,
        {
        let result = self.items.binary_search_by(|probe| {
            // FIXME in case of rangetype being vec if probe and key have different sizes comparison as is is broken
            // and have to be performed on slice of key of probe length, like probe < key[..probe.len()]
            // don't know yet how to express this requirement in generic code, may be require imple of RangeTo trait 
            debug_assert!(probe.start.len() == key.len());

            if (probe.start.borrow() <= key) && (key <= probe.end.borrow()) {
                return Ordering::Equal;
            } else if probe.end.borrow() < key {
                return Ordering::Less;
            } else if probe.start.borrow() > key {
                return Ordering::Greater;
            }
            // TODO check invariants, including bounds excluded etc. ranges should be [)
            println!(
                "probe start {:?} probe end {:?} {:?}",
                probe.start, probe.end, key
            );
            println!("contents {:?}", &self.items);
            unreachable!();
        });
        match result {
            Ok(idx) => Some(&self.items[idx]),
            Err(_) => None,
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct RangeVec<RangeType, DataType = ()>
where
    RangeType: Ord + Debug,
    DataType: Eq + Debug,
{
    items: Vec<RangeSpec<RangeType, DataType>>,
}

impl<RangeType, DataType> Default for RangeVec<RangeType, DataType>
where
    RangeType: Ord + Debug + Clone + Len,
    DataType: Eq + Debug + Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<RangeType, DataType> IntoIterator for RangeVec<RangeType, DataType>
where
    RangeType: Ord + Debug,
    DataType: Eq + Debug,
{
    type Item = RangeSpec<RangeType, DataType>;

    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.items.into_iter()
    }
}

impl<RangeType, DataType> ReadonlyRangeMap<RangeType, DataType> for RangeVec<RangeType, DataType>
where
    RangeType: Ord + Debug + Clone,
    DataType: Eq + Debug + Clone,
{
    fn get<Q>(&self, key: &Q) -> Option<&RangeSpec<RangeType, DataType>>
    where
        RangeType: Borrow<Q>,
        Q: ?Sized + Ord + Eq + Len,
    {
        let result = self.items.binary_search_by(|probe| {
            if (probe.start.borrow() <= key) && (key <= probe.end.borrow()) {
                return Ordering::Equal;
            } else if probe.end.borrow() > key {
                return Ordering::Greater;
            } else if probe.start.borrow() > key {
                return Ordering::Less;
            }
            // TODO check invariants, including bounds excluded etc. ranges should be [)
            unreachable!();
        });
        match result {
            // TODO probably can return a reference here, so Copy is not needed.
            Ok(idx) => Some(&self.items[idx]),
            Err(_) => None,
        }
    }
}

impl<RangeType, DataType> RangeMap<RangeType, DataType> for RangeVec<RangeType, DataType>
where
    RangeType: Ord + Debug + Clone + Len,
    DataType: Eq + Debug + Clone,
{
    type FROZEN = ReadonlyRangeVec<RangeType, DataType>;

    fn new() -> Self {
        Self { items: Vec::new() }
    }

    fn freeze(self) -> Self::FROZEN {
        Self::FROZEN::new(self.items)
    }

    fn insert(&mut self, range: RangeSpec<RangeType, DataType>) -> () {
        let result = self.items.binary_search(&range);
        match result {
            Ok(_) => (),                               // range already exists
            Err(idx) => self.items.insert(idx, range), // range does not exist
        }
    }

    fn delete(
        &mut self,
        range: RangeSpec<RangeType, DataType>,
    ) -> Option<RangeSpec<RangeType, DataType>> {
        let result = self.items.binary_search(&range);
        match result {
            Ok(idx) => Some(self.items.remove(idx)),
            Err(_) => None, // range does not exist
        }
    }
}

// TODO make rangevec suitable for both, simple integers and vecs or slices, maybe custom trait
impl Len for i32 {
    fn len(&self) -> usize {
        42
    }
}

impl<T> Len for Vec<T> {
    fn len(&self) -> usize {
        self.len()
    }
}

impl<T> Len for [T] {
    fn len(&self) -> usize {
        self.len()
    }
}

#[cfg(test)]
mod tests {
    use crate::RangeMap;
    use crate::RangeSpec;
    use crate::RangeVec;
    use crate::ReadonlyRangeMap;

    #[test]
    fn test_invalid_new_range() {
        let range = RangeSpec::new(10, 0, ());
        assert!(range.is_err());
    }

    #[test]
    fn test_search_insert_search_delete_search() {
        let mut rangemap = RangeVec::new();
        assert!(rangemap.get(&3).is_none());

        let range = RangeSpec::trusted_new(0, 10, 42);
        rangemap.insert(range.clone());

        assert_eq!(rangemap.get(&3), Some(&range));

        assert!(rangemap.delete(range).is_some());
        assert!(rangemap.get(&3).is_none());
    }

    #[test]
    fn test_u8_slice() {
        let mut rangemap = RangeVec::<Vec<u8>>::new();
        let key = vec![1u8, 2, 3, 4, 5];

        assert!(rangemap.get(&key).is_none());

        let range = RangeSpec::trusted_new(
            vec![0u8, 0, 0, 0, 0, 0, 0],
            vec![255u8, 255, 255, 255, 255, 255, 255],
            (),
        );
        rangemap.insert(range.clone());

        assert_eq!(rangemap.get(&key), Some(&range));

        assert!(rangemap.delete(range).is_some());
        assert!(rangemap.get(&key).is_none());
    }

    #[test]
    fn test_freeze() {
        let mut rangemap = RangeVec::<Vec<u8>>::new();
        let key = vec![1u8, 2, 3, 4, 5];

        assert!(rangemap.get(&key).is_none());

        let range = RangeSpec::trusted_new(
            vec![0u8, 0, 0, 0, 0, 0, 0],
            vec![255u8, 255, 255, 255, 255, 255, 255],
            (),
        );
        rangemap.insert(range.clone());

        let readonly_rangemap = rangemap.freeze();
        assert_eq!(readonly_rangemap.get(&key), Some(&range));
    }

    #[test]
    fn test_ranges_of_different_lengths() {
        let mut rangemap = RangeVec::<Vec<u8>>::new();

        let bounds = ((0, 21), (22, 43), (44, 65));

        let range1 =
            RangeSpec::new(vec![bounds.0 .0; 4], vec![bounds.0 .1; 4], ()).expect("always ok");
        rangemap.insert(range1);
        // datums.push(Datum::new(0, RangeSpec::trusted_new(range1.start, range1.end, ()), storages[0]));
        let range2 =
            RangeSpec::new(vec![bounds.1 .0; 4], vec![bounds.1 .1; 4], ()).expect("always ok");
        rangemap.insert(range2);
        // datums.push(Datum::new(0, RangeSpec::trusted_new(range2.start, range2.end, ()), storages[1]));
        let range3 =
            RangeSpec::new(vec![bounds.2 .0; 4], vec![bounds.2 .1; 4], ()).expect("always ok");
        rangemap.insert(range3);
        let key = vec![0, 0, 0];
        // datums.push(Datum::new(0, RangeSpec::trusted_new(range3.start, range3.end, ()), storages[2]));
        let rangemap = rangemap.freeze();
        let result = rangemap.get(&key);
        assert!(result.is_some())
    }


    #[test]
    fn test_comparison_of_keys() {
        let mut rangemap = RangeVec::<Vec<u8>>::new();

        let bounds = ((0, 21), (22, 43), (44, 65));
        let bound_len = 3;

        let range1 =
            RangeSpec::new(vec![bounds.0 .0; bound_len], vec![bounds.0 .1; bound_len], ()).expect("always ok");
        rangemap.insert(range1);
        // datums.push(Datum::new(0, RangeSpec::trusted_new(range1.start, range1.end, ()), storages[0]));
        let range2 =
            RangeSpec::new(vec![bounds.1 .0; bound_len], vec![bounds.1 .1; bound_len], ()).expect("always ok");
        rangemap.insert(range2);
        // datums.push(Datum::new(0, RangeSpec::trusted_new(range2.start, range2.end, ()), storages[1]));
        let range3 =
            RangeSpec::new(vec![bounds.2 .0; bound_len], vec![bounds.2 .1; bound_len], ()).expect("always ok");
        rangemap.insert(range3);
        let key = vec![44, 60, 62];
        // datums.push(Datum::new(0, RangeSpec::trusted_new(range3.start, range3.end, ()), storages[2]));
        let rangemap = rangemap.freeze();
        let result = rangemap.get(&key);
        assert!(result.is_some())
    }
}
