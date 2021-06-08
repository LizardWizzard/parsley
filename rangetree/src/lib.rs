use std::cmp;
use std::fmt::Debug;
use std::{borrow::Borrow, cmp::Ordering};
use thiserror::Error;
pub mod rangevec;

#[derive(Error, Debug)]
pub enum RangeTreeError {
    #[error("range start is greater than range end")]
    InvalidRange,
}

// This trait needed when comparing slices of different length in rangemap.
// To keep range small it can be defined using only several bytes instead of full keys,
// so it is required to compare only equal sublength of range border with key being searched.
// For primitive types like integers this is not needed, so this impl can reuse Ord trait impl for such types
pub trait SubLenCmp {
    fn sublen_cmp(&self, other: &Self) -> Ordering;
}

// sadly no trait alias so, use this impls as aliases
pub trait RangeTypeBounds: SubLenCmp + Debug + Clone + Send + PartialOrd + PartialEq + Eq {}

impl<T> RangeTypeBounds for T where T: SubLenCmp + Debug + Clone + Send + PartialOrd + PartialEq + Eq
{}
pub trait DataTypeBounds: Debug + Clone + Send + PartialEq + Eq {}
impl<T> DataTypeBounds for T where T: Debug + Clone + Send + PartialEq + Eq {}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct RangeSpec<RangeType: RangeTypeBounds, DataType = ()> {
    pub start: RangeType,
    pub end: RangeType,
    pub data: DataType,
}

impl<RangeType: RangeTypeBounds, DataType> RangeSpec<RangeType, DataType> {
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
    RangeType: RangeTypeBounds,
    DataType: DataTypeBounds,
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
    RangeType: RangeTypeBounds,
    DataType: DataTypeBounds,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub trait ReadonlyRangeMap<RangeType, DataType = ()>:
    IntoIterator<Item = RangeSpec<RangeType, DataType>> + Debug + Clone + Send
where
    RangeType: RangeTypeBounds,
    DataType: DataTypeBounds,
{
    fn get<Q>(&self, key: &Q) -> Option<&RangeSpec<RangeType, DataType>>
    where
        Q: ?Sized + SubLenCmp + Debug,
        RangeType: Borrow<Q>;
}

pub trait RangeMap<RangeType, DataType = ()>: ReadonlyRangeMap<RangeType, DataType>
where
    RangeType: RangeTypeBounds,
    DataType: DataTypeBounds,
{
    type FROZEN: ReadonlyRangeMap<RangeType, DataType> + Sync;

    fn new() -> Self;

    fn freeze(self) -> Self::FROZEN;

    fn insert(&mut self, range: RangeSpec<RangeType, DataType>) -> (); // TODO exclude overlapping ranges

    fn delete(
        &mut self,
        range: RangeSpec<RangeType, DataType>,
    ) -> Option<RangeSpec<RangeType, DataType>>;
}

// implement needed traits for std types
impl<T: Ord> SubLenCmp for Vec<T> {
    fn sublen_cmp(&self, other: &Self) -> Ordering {
        let cmp_len = cmp::min(self.len(), other.len());
        self[..cmp_len].cmp(&other[..cmp_len])
    }
}

impl<T: Ord> SubLenCmp for [T] {
    fn sublen_cmp(&self, other: &Self) -> Ordering {
        let cmp_len = cmp::min(self.len(), other.len());
        self[..cmp_len].cmp(&other[..cmp_len])
    }
}

// TODO copypaste impls via macros for other integers
impl SubLenCmp for usize {
    fn sublen_cmp(&self, other: &Self) -> Ordering {
        self.cmp(other)
    }
}
