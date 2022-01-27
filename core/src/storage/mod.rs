use std::{cell::RefCell, rc::Rc};

use futures::Future;

pub mod memory;
pub mod memory_durable;

// this is a trait for general storage implementation
// due to lack of async trait implementing corresponding futures manually
// Rc RefCell thing is needed for sharing one storage instance between tasks in a single thread
// storage impl should use borrowing in ref cell in such a way that it doesn't cross await boundary (because if it will RefCell would panic because of other tasks)
pub trait Storage {
    type GetFuture<'key>: Future<Output = Option<Vec<u8>>>; // TODO remove vec by using custom smart pointer for sharing references to immutable data to other thread
    type SetFuture<'key, 'value>: Future<Output = ()>;
    type DelFuture<'key>: Future<Output = Option<()>>;

    fn get<'key>(instance: &Rc<RefCell<Self>>, key: &'key [u8]) -> Self::GetFuture<'key>;

    fn set<'key, 'value>(
        instance: &Rc<RefCell<Self>>,
        key: &'key [u8],
        value: &'value [u8],
    ) -> Self::SetFuture<'key, 'value>;

    fn delete<'key>(instance: &Rc<RefCell<Self>>, key: &'key [u8]) -> Self::DelFuture<'key>;
}
