// SPDX-License-Identifier: MIT OR Apache-2.0

use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use p2panda_core::{Extensions, Hash, LogId, Operation, SeqNum, Topic, VerifyingKey};
use p2panda_store::logs::LogStore;
use p2panda_store::topics::TopicStore;
use p2panda_sync::protocols::TopicLogSyncEvent;
use ractor::concurrency::JoinHandle;
use ractor::{ActorRef, call};
use thiserror::Error;
use tokio::sync::RwLock;

use crate::gossip::Gossip;
use crate::iroh_endpoint::Endpoint;
use crate::sync::actors::ToSyncManager;
use crate::sync::handle::SyncHandle;
use crate::sync::log_sync::Builder;

/// Eventually consistent, local-first sync protocol based on append-only logs.
///
/// ## Example
///
/// See [`chat.rs`] for a full example using the sync protocol.
///
/// ## Local-first
///
/// In local-first applications we want to converge towards the same state eventually, which
/// requires nodes to catch up on missed messages - independent of if they've been offline or
/// not.
///
/// `p2panda-net` comes with a default `LogSync` protocol implementation which uses p2panda's
/// **append-only log** Base Convergent Data Type (CDT).
///
/// After initial sync has finished, nodes switch to **live-mode** to directly push new messages to the
/// network using a gossip protocol.
///
/// [`chat.rs`]: https://github.com/p2panda/p2panda/blob/main/p2panda-net/examples/chat.rs
#[derive(Clone, Debug)]
pub struct LogSync<S, L, E>
where
    S: LogStore<Operation<E>, VerifyingKey, L, SeqNum, Hash>
        + TopicStore<Topic, VerifyingKey, L>
        + Clone
        + Send
        + 'static,
    L: LogId + Debug + Send + 'static,
    E: Extensions + Send + 'static,
{
    inner: Arc<RwLock<Inner<E>>>,
    _phantom: PhantomData<(S, L)>,
}

#[derive(Debug)]
struct Inner<E>
where
    E: Extensions + Send + 'static,
{
    #[allow(clippy::type_complexity)]
    actor_ref: ActorRef<ToSyncManager<Operation<E>, TopicLogSyncEvent<E>>>,
    actor_task: Option<JoinHandle<()>>,
}

impl<S, L, E> LogSync<S, L, E>
where
    S: LogStore<Operation<E>, VerifyingKey, L, SeqNum, Hash>
        + TopicStore<Topic, VerifyingKey, L>
        + Clone
        + Send
        + 'static,
    L: LogId + Debug + Send + 'static,
    E: Extensions + Send + 'static,
{
    #[allow(clippy::type_complexity)]
    pub(crate) fn new(
        actor_ref: ActorRef<ToSyncManager<Operation<E>, TopicLogSyncEvent<E>>>,
        actor_task: JoinHandle<()>,
    ) -> Self {
        Self {
            inner: Arc::new(RwLock::new(Inner {
                actor_ref,
                actor_task: Some(actor_task),
            })),
            _phantom: PhantomData,
        }
    }

    pub fn builder(store: S, endpoint: Endpoint, gossip: Gossip) -> Builder<S, L, E> {
        Builder::<S, L, E>::new(store, endpoint, gossip)
    }

    // TODO: Extensions should be generic over a stream handle, not over this struct.
    pub async fn stream(
        &self,
        topic: Topic,
        live_mode: bool,
    ) -> Result<SyncHandle<Operation<E>, TopicLogSyncEvent<E>>, LogSyncError<E>> {
        let inner = self.inner.read().await;
        let sync_manager_ref =
            call!(inner.actor_ref, ToSyncManager::Create, topic, live_mode).map_err(Box::new)?;

        Ok(SyncHandle::new(
            topic,
            inner.actor_ref.clone(),
            sync_manager_ref,
        ))
    }

    /// Stop the sync manager and wait until its actor has released the store.
    ///
    /// Dropping a session requests the same stop, but does not wait for the
    /// actor to finish. A resident process needs the stronger boundary before
    /// it can reopen an exclusively locked durable backend during restart.
    /// This stops the shared session even when another `LogSync` clone exists;
    /// it is an authority-level close, not a per-handle detach.
    pub async fn shutdown(self) -> Result<(), LogSyncError<E>> {
        let (actor_ref, actor_task) = {
            let mut inner = self.inner.write().await;
            (inner.actor_ref.clone(), inner.actor_task.take())
        };
        let stop_error = actor_ref
            .stop_and_wait(None, None)
            .await
            .err()
            .map(|error| error.to_string());
        let task_error = match actor_task {
            Some(actor_task) => actor_task.await.err().map(|error| error.to_string()),
            None => None,
        };
        if let Some(error) = stop_error {
            return Err(LogSyncError::ActorStop(error));
        }
        if let Some(error) = task_error {
            return Err(LogSyncError::ActorTask(error));
        }
        Ok(())
    }
}

impl<E> Drop for Inner<E>
where
    E: Extensions + Send + 'static,
{
    fn drop(&mut self) {
        self.actor_ref.stop(None);
    }
}

#[derive(Debug, Error)]
pub enum LogSyncError<E> {
    /// Spawning the internal actor failed.
    #[error(transparent)]
    ActorSpawn(#[from] ractor::SpawnErr),

    /// Messaging with internal actor via RPC failed.
    #[error(transparent)]
    ActorRpc(#[from] Box<ractor::RactorErr<ToSyncManager<Operation<E>, TopicLogSyncEvent<E>>>>),

    /// Stopping the internal actor failed before its resources were released.
    #[error("stopping logsync actor: {0}")]
    ActorStop(String),

    /// The actor stopped but its owning task failed before dropping state.
    #[error("joining logsync actor task: {0}")]
    ActorTask(String),
}
