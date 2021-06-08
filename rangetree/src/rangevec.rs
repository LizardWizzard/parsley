use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt::Debug;

use crate::RangeMap;
use crate::{DataTypeBounds, RangeSpec, RangeTypeBounds, ReadonlyRangeMap, SubLenCmp};

#[derive(Debug, Clone)]
pub struct ReadonlyRangeVec<RangeType, DataType = ()>
where
    RangeType: RangeTypeBounds,
    DataType: DataTypeBounds,
{
    items: Vec<RangeSpec<RangeType, DataType>>,
}

// SAFETY: since there is no way to change this state it is indeed Sync
unsafe impl<RangeType, DataType> Sync for ReadonlyRangeVec<RangeType, DataType>
where
    RangeType: RangeTypeBounds,
    DataType: DataTypeBounds,
{
}

unsafe impl<RangeType, DataType> Send for ReadonlyRangeVec<RangeType, DataType>
where
    RangeType: RangeTypeBounds,
    DataType: DataTypeBounds,
{
}

impl<RangeType, DataType> ReadonlyRangeVec<RangeType, DataType>
where
    RangeType: RangeTypeBounds,
    DataType: DataTypeBounds,
{
    pub fn new(items: Vec<RangeSpec<RangeType, DataType>>) -> Self {
        Self { items }
    }
}

pub fn rangemap_get<'a, RangeType, DataType, Q>(
    items: &'a [RangeSpec<RangeType, DataType>],
    key: &Q,
) -> Option<&'a RangeSpec<RangeType, DataType>>
where
    RangeType: Borrow<Q> + RangeTypeBounds,
    Q: ?Sized + SubLenCmp,
{
    // let result = self.items.binary_search_by(|probe| {
    //     // FIXME in case of rangetype being vec if probe and key have different sizes comparison as is is broken
    //     // and have to be performed on slice of key of probe length, like probe < key[..probe.len()]
    //     // don't know yet how to express this requirement in generic code, may be require imple of RangeTo trait
    //     // TODO impl custom comparison trait which compares only min(a.len, b.len) items
    //     debug_assert!(probe.start.len() == key.len());

    //     if (probe.start.borrow() <= key) && (key <= probe.end.borrow()) {
    //         return Ordering::Equal;
    //     } else if probe.end.borrow() < key {
    //         return Ordering::Less;
    //     } else if probe.start.borrow() > key {
    //         return Ordering::Greater;
    //     }
    //     unreachable!();
    // });
    // match result {
    //     Ok(idx) => Some(&self.items[idx]),
    //     Err(_) => None,
    // }

    let result = items.binary_search_by(|probe| {
        // roughly equivalent of  if (probe.start.borrow() <= key) && (key <= probe.end.borrow()) but correct according to SubLenCmp 
        if matches!(probe.start.borrow().sublen_cmp(key), Ordering::Equal | Ordering::Less) &&  matches!(probe.end.borrow().sublen_cmp(key), Ordering::Greater) {
            return Ordering::Equal;
        } else if matches!(probe.end.borrow().sublen_cmp(key), Ordering::Less) {
            return Ordering::Less;
        } else if matches!(probe.start.borrow().sublen_cmp(key), Ordering::Greater) {
            return Ordering::Greater;
        }
        unreachable!();
    });
    match result {
        Ok(idx) => Some(&items[idx]),
        Err(_) => None,
    }
}

impl<RangeType, DataType> IntoIterator for ReadonlyRangeVec<RangeType, DataType>
where
    RangeType: RangeTypeBounds,
    DataType: DataTypeBounds,
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
    RangeType: RangeTypeBounds,
    DataType: DataTypeBounds,
{
    fn get<Q>(&self, key: &Q) -> Option<&RangeSpec<RangeType, DataType>>
    where
        RangeType: Borrow<Q> + RangeTypeBounds,
        Q: ?Sized + SubLenCmp + Debug,
    {
        rangemap_get(&self.items, key)
    }
}

#[derive(Debug, Clone)]
pub struct RangeVec<RangeType, DataType = ()>
where
    RangeType: RangeTypeBounds,
    DataType: DataTypeBounds,
{
    items: Vec<RangeSpec<RangeType, DataType>>,
}

impl<RangeType, DataType> Default for RangeVec<RangeType, DataType>
where
    RangeType: RangeTypeBounds,
    DataType: DataTypeBounds,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<RangeType, DataType> IntoIterator for RangeVec<RangeType, DataType>
where
    RangeType: RangeTypeBounds,
    DataType: DataTypeBounds,
{
    type Item = RangeSpec<RangeType, DataType>;

    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.items.into_iter()
    }
}

impl<RangeType, DataType> ReadonlyRangeMap<RangeType, DataType> for RangeVec<RangeType, DataType>
where
    RangeType: RangeTypeBounds,
    DataType: DataTypeBounds,
{
    fn get<Q>(&self, key: &Q) -> Option<&RangeSpec<RangeType, DataType>>
    where
        RangeType: Borrow<Q> + RangeTypeBounds,
        Q: ?Sized + SubLenCmp,
    {
        rangemap_get(&self.items, key)
    }
}

impl<RangeType, DataType> RangeMap<RangeType, DataType> for RangeVec<RangeType, DataType>
where
    RangeType: RangeTypeBounds,
    DataType: DataTypeBounds,
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

#[cfg(test)]
mod tests {
    use crate::{RangeMap, RangeSpec, ReadonlyRangeMap, rangevec::RangeVec};

    // use supe::RangeMap;
    // use crate::RangeSpec;
    // use crate::RangeVec;
    // use crate::ReadonlyRangeMap;

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
        let key = vec![1u8; 8];

        assert!(rangemap.get(&key).is_none());

        let range = RangeSpec::trusted_new(vec![0u8; 8], vec![255u8; 8], ());
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

        let range1 = RangeSpec::new(
            vec![bounds.0 .0; bound_len],
            vec![bounds.0 .1; bound_len],
            (),
        )
        .expect("always ok");
        rangemap.insert(range1);
        // datums.push(Datum::new(0, RangeSpec::trusted_new(range1.start, range1.end, ()), storages[0]));
        let range2 = RangeSpec::new(
            vec![bounds.1 .0; bound_len],
            vec![bounds.1 .1; bound_len],
            (),
        )
        .expect("always ok");
        rangemap.insert(range2);
        // datums.push(Datum::new(0, RangeSpec::trusted_new(range2.start, range2.end, ()), storages[1]));
        let range3 = RangeSpec::new(
            vec![bounds.2 .0; bound_len],
            vec![bounds.2 .1; bound_len],
            (),
        )
        .expect("always ok");
        rangemap.insert(range3);
        let key = vec![44, 60, 62];
        // datums.push(Datum::new(0, RangeSpec::trusted_new(range3.start, range3.end, ()), storages[2]));
        let rangemap = rangemap.freeze();
        let result = rangemap.get(&key);
        assert!(result.is_some())
    }
}
