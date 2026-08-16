// SPDX-License-Identifier: MIT OR Apache-2.0

//! Store-agnostic handle for the address book.
//!
//! The address book actor, its api and the discovery strategies all need *a*
//! [`AddressBookStore`], not a particular one. [`AddressBookStoreHandle`] is
//! that indirection: it owns some concrete store behind a trait object and
//! implements [`AddressBookStore`] itself, so every existing consumer keeps
//! its non-generic types.
//!
//! Two things make the erasure necessary. [`AddressBookStore`] returns
//! `impl Future` from its methods, which is not dyn-compatible, so
//! `DynAddressBookStore` mirrors it with boxed futures. And its associated
//! `Error` differs per backend, so [`StoreError`] boxes it behind one type
//! that still satisfies the trait's `Error` bound.
//!
//! Transactions fold into the write methods rather than being exposed. Every
//! call site in the actor wrapped exactly one store operation in `tx!`, so a
//! permit spanning several operations was never taken; a backend whose
//! operations are already atomic needs no permit at all, and one that provides
//! [`Transaction`] takes one per write, which is what the actor did before.

use std::collections::HashSet;
use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use futures_util::future::LocalBoxFuture;
use p2panda_core::Topic;
use p2panda_store::Transaction;
use p2panda_store::address_book::AddressBookStore;

use crate::NodeId;
use crate::addrs::NodeInfo;

/// Error from the address book's backing store, with the backend's own type erased.
#[derive(Debug)]
pub struct StoreError(Box<dyn Error + Send + Sync + 'static>);

impl StoreError {
    pub(crate) fn new<E: Error + Send + Sync + 'static>(error: E) -> Self {
        Self(Box::new(error))
    }
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl Error for StoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.0.source()
    }
}

/// Future returned by every `DynAddressBookStore` method.
///
/// Deliberately not `Send`. [`AddressBookStore`] does not declare its returned
/// futures `Send`, so there is no stable way to require it of an arbitrary
/// backend, and nothing here needs it: the address book actor, the discovery
/// manager, its walker and its sessions are all `ThreadLocalActor`s, so a
/// future is created and awaited on one thread. This also leaves room for a
/// browser backend awaiting JS promises, which could not be `Send` at all.
///
/// The *handle* is still `Send + Sync`, because ractor moves actor arguments
/// and messages to the actor's thread. Only the futures stay put.
type StoreFuture<'a, T> = LocalBoxFuture<'a, Result<T, StoreError>>;

/// Dyn-compatible mirror of `AddressBookStore<NodeId, NodeInfo>`.
trait DynAddressBookStore: Send + Sync + 'static {
    fn insert_node_info(&self, info: NodeInfo) -> StoreFuture<'_, bool>;

    fn remove_node_info(&self, id: &NodeId) -> StoreFuture<'_, bool>;

    fn remove_older_than(&self, duration: Duration) -> StoreFuture<'_, usize>;

    fn node_info(&self, id: &NodeId) -> StoreFuture<'_, Option<NodeInfo>>;

    fn node_topics(&self, id: &NodeId) -> StoreFuture<'_, HashSet<Topic>>;

    fn all_node_infos(&self) -> StoreFuture<'_, Vec<NodeInfo>>;

    fn all_nodes_len(&self) -> StoreFuture<'_, usize>;

    fn all_bootstrap_nodes_len(&self) -> StoreFuture<'_, usize>;

    fn selected_node_infos(&self, ids: &[NodeId]) -> StoreFuture<'_, Vec<NodeInfo>>;

    fn set_topics(&self, id: NodeId, topics: HashSet<Topic>) -> StoreFuture<'_, ()>;

    fn node_infos_by_topics(&self, topics: &[Topic]) -> StoreFuture<'_, Vec<NodeInfo>>;

    fn random_node(&self) -> StoreFuture<'_, Option<NodeInfo>>;

    fn random_bootstrap_node(&self) -> StoreFuture<'_, Option<NodeInfo>>;
}

/// Generates the read-only methods, which are identical for both wrappers.
macro_rules! dyn_reads {
    () => {
        fn node_info(&self, id: &NodeId) -> StoreFuture<'_, Option<NodeInfo>> {
            let id = *id;
            Box::pin(async move {
                AddressBookStore::<NodeId, NodeInfo>::node_info(&self.0, &id)
                    .await
                    .map_err(StoreError::new)
            })
        }

        fn node_topics(&self, id: &NodeId) -> StoreFuture<'_, HashSet<Topic>> {
            let id = *id;
            Box::pin(async move {
                AddressBookStore::<NodeId, NodeInfo>::node_topics(&self.0, &id)
                    .await
                    .map_err(StoreError::new)
            })
        }

        fn all_node_infos(&self) -> StoreFuture<'_, Vec<NodeInfo>> {
            Box::pin(async move {
                AddressBookStore::<NodeId, NodeInfo>::all_node_infos(&self.0)
                    .await
                    .map_err(StoreError::new)
            })
        }

        fn all_nodes_len(&self) -> StoreFuture<'_, usize> {
            Box::pin(async move {
                AddressBookStore::<NodeId, NodeInfo>::all_nodes_len(&self.0)
                    .await
                    .map_err(StoreError::new)
            })
        }

        fn all_bootstrap_nodes_len(&self) -> StoreFuture<'_, usize> {
            Box::pin(async move {
                AddressBookStore::<NodeId, NodeInfo>::all_bootstrap_nodes_len(&self.0)
                    .await
                    .map_err(StoreError::new)
            })
        }

        fn selected_node_infos(&self, ids: &[NodeId]) -> StoreFuture<'_, Vec<NodeInfo>> {
            let ids = ids.to_vec();
            Box::pin(async move {
                AddressBookStore::<NodeId, NodeInfo>::selected_node_infos(&self.0, &ids)
                    .await
                    .map_err(StoreError::new)
            })
        }

        fn node_infos_by_topics(&self, topics: &[Topic]) -> StoreFuture<'_, Vec<NodeInfo>> {
            let topics = topics.to_vec();
            Box::pin(async move {
                AddressBookStore::<NodeId, NodeInfo>::node_infos_by_topics(&self.0, &topics)
                    .await
                    .map_err(StoreError::new)
            })
        }

        fn random_node(&self) -> StoreFuture<'_, Option<NodeInfo>> {
            Box::pin(async move {
                AddressBookStore::<NodeId, NodeInfo>::random_node(&self.0)
                    .await
                    .map_err(StoreError::new)
            })
        }

        fn random_bootstrap_node(&self) -> StoreFuture<'_, Option<NodeInfo>> {
            Box::pin(async move {
                AddressBookStore::<NodeId, NodeInfo>::random_bootstrap_node(&self.0)
                    .await
                    .map_err(StoreError::new)
            })
        }
    };
}

/// Wrapper for a store whose own operations are already atomic.
struct Direct<S>(S);

impl<S> DynAddressBookStore for Direct<S>
where
    S: AddressBookStore<NodeId, NodeInfo> + Send + Sync + 'static,
    <S as AddressBookStore<NodeId, NodeInfo>>::Error: Send + Sync + 'static,
{
    fn insert_node_info(&self, info: NodeInfo) -> StoreFuture<'_, bool> {
        Box::pin(async move {
            AddressBookStore::<NodeId, NodeInfo>::insert_node_info(&self.0, info)
                .await
                .map_err(StoreError::new)
        })
    }

    fn remove_node_info(&self, id: &NodeId) -> StoreFuture<'_, bool> {
        let id = *id;
        Box::pin(async move {
            AddressBookStore::<NodeId, NodeInfo>::remove_node_info(&self.0, &id)
                .await
                .map_err(StoreError::new)
        })
    }

    fn remove_older_than(&self, duration: Duration) -> StoreFuture<'_, usize> {
        Box::pin(async move {
            AddressBookStore::<NodeId, NodeInfo>::remove_older_than(&self.0, duration)
                .await
                .map_err(StoreError::new)
        })
    }

    fn set_topics(&self, id: NodeId, topics: HashSet<Topic>) -> StoreFuture<'_, ()> {
        Box::pin(async move {
            AddressBookStore::<NodeId, NodeInfo>::set_topics(&self.0, id, topics)
                .await
                .map_err(StoreError::new)
        })
    }

    dyn_reads!();
}

/// Wrapper for a store that provides [`Transaction`], taking one permit per write.
struct Transacted<S>(S);

impl<S> DynAddressBookStore for Transacted<S>
where
    S: AddressBookStore<NodeId, NodeInfo> + Transaction + Send + Sync + 'static,
    <S as AddressBookStore<NodeId, NodeInfo>>::Error: Send + Sync + 'static,
    <S as Transaction>::Error: Send + Sync + 'static,
{
    fn insert_node_info(&self, info: NodeInfo) -> StoreFuture<'_, bool> {
        Box::pin(async move {
            let permit = Transaction::begin(&self.0).await.map_err(StoreError::new)?;
            let result = AddressBookStore::<NodeId, NodeInfo>::insert_node_info(&self.0, info)
                .await
                .map_err(StoreError::new)?;
            Transaction::commit(&self.0, permit)
                .await
                .map_err(StoreError::new)?;
            Ok(result)
        })
    }

    fn remove_node_info(&self, id: &NodeId) -> StoreFuture<'_, bool> {
        let id = *id;
        Box::pin(async move {
            let permit = Transaction::begin(&self.0).await.map_err(StoreError::new)?;
            let result = AddressBookStore::<NodeId, NodeInfo>::remove_node_info(&self.0, &id)
                .await
                .map_err(StoreError::new)?;
            Transaction::commit(&self.0, permit)
                .await
                .map_err(StoreError::new)?;
            Ok(result)
        })
    }

    fn remove_older_than(&self, duration: Duration) -> StoreFuture<'_, usize> {
        Box::pin(async move {
            let permit = Transaction::begin(&self.0).await.map_err(StoreError::new)?;
            let result = AddressBookStore::<NodeId, NodeInfo>::remove_older_than(&self.0, duration)
                .await
                .map_err(StoreError::new)?;
            Transaction::commit(&self.0, permit)
                .await
                .map_err(StoreError::new)?;
            Ok(result)
        })
    }

    fn set_topics(&self, id: NodeId, topics: HashSet<Topic>) -> StoreFuture<'_, ()> {
        Box::pin(async move {
            let permit = Transaction::begin(&self.0).await.map_err(StoreError::new)?;
            AddressBookStore::<NodeId, NodeInfo>::set_topics(&self.0, id, topics)
                .await
                .map_err(StoreError::new)?;
            Transaction::commit(&self.0, permit)
                .await
                .map_err(StoreError::new)?;
            Ok(())
        })
    }

    dyn_reads!();
}

/// A cloneable handle to whichever store backs the address book.
#[derive(Clone)]
pub struct AddressBookStoreHandle(Arc<dyn DynAddressBookStore>);

impl AddressBookStoreHandle {
    /// Wraps a store whose operations are already atomic on their own.
    pub fn new<S>(store: S) -> Self
    where
        S: AddressBookStore<NodeId, NodeInfo> + Send + Sync + 'static,
        <S as AddressBookStore<NodeId, NodeInfo>>::Error: Send + Sync + 'static,
    {
        Self(Arc::new(Direct(store)))
    }

    /// Wraps a store that provides [`Transaction`], taking a permit per write.
    pub fn with_transactions<S>(store: S) -> Self
    where
        S: AddressBookStore<NodeId, NodeInfo> + Transaction + Send + Sync + 'static,
        <S as AddressBookStore<NodeId, NodeInfo>>::Error: Send + Sync + 'static,
        <S as Transaction>::Error: Send + Sync + 'static,
    {
        Self(Arc::new(Transacted(store)))
    }
}

impl fmt::Debug for AddressBookStoreHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AddressBookStoreHandle")
    }
}

impl AddressBookStore<NodeId, NodeInfo> for AddressBookStoreHandle {
    type Error = StoreError;

    fn insert_node_info(&self, info: NodeInfo) -> impl Future<Output = Result<bool, Self::Error>> {
        self.0.insert_node_info(info)
    }

    fn remove_node_info(&self, id: &NodeId) -> impl Future<Output = Result<bool, Self::Error>> {
        self.0.remove_node_info(id)
    }

    fn remove_older_than(
        &self,
        duration: Duration,
    ) -> impl Future<Output = Result<usize, Self::Error>> {
        self.0.remove_older_than(duration)
    }

    fn node_info(&self, id: &NodeId) -> impl Future<Output = Result<Option<NodeInfo>, Self::Error>> {
        self.0.node_info(id)
    }

    fn node_topics(
        &self,
        id: &NodeId,
    ) -> impl Future<Output = Result<HashSet<Topic>, Self::Error>> {
        self.0.node_topics(id)
    }

    fn all_node_infos(&self) -> impl Future<Output = Result<Vec<NodeInfo>, Self::Error>> {
        self.0.all_node_infos()
    }

    fn all_nodes_len(&self) -> impl Future<Output = Result<usize, Self::Error>> {
        self.0.all_nodes_len()
    }

    fn all_bootstrap_nodes_len(&self) -> impl Future<Output = Result<usize, Self::Error>> {
        self.0.all_bootstrap_nodes_len()
    }

    fn selected_node_infos(
        &self,
        ids: &[NodeId],
    ) -> impl Future<Output = Result<Vec<NodeInfo>, Self::Error>> {
        self.0.selected_node_infos(ids)
    }

    fn set_topics(
        &self,
        id: NodeId,
        topics: HashSet<Topic>,
    ) -> impl Future<Output = Result<(), Self::Error>> {
        self.0.set_topics(id, topics)
    }

    fn node_infos_by_topics(
        &self,
        topics: &[Topic],
    ) -> impl Future<Output = Result<Vec<NodeInfo>, Self::Error>> {
        self.0.node_infos_by_topics(topics)
    }

    fn random_node(&self) -> impl Future<Output = Result<Option<NodeInfo>, Self::Error>> {
        self.0.random_node()
    }

    fn random_bootstrap_node(&self) -> impl Future<Output = Result<Option<NodeInfo>, Self::Error>> {
        self.0.random_bootstrap_node()
    }
}
