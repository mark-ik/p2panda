// SPDX-License-Identifier: MIT OR Apache-2.0

use std::fmt::Debug;
use std::marker::PhantomData;

use p2panda_core::{Extensions, Hash, LogId, Operation, SeqNum, Topic, VerifyingKey};
use p2panda_store::logs::LogStore;
use p2panda_store::topics::TopicStore;
use p2panda_sync::manager::TopicSyncManager;
use ractor::thread_local::{ThreadLocalActor, ThreadLocalActorSpawner};

use crate::gossip::Gossip;
use crate::iroh_endpoint::Endpoint;
use crate::sync::actors::SyncManager;
use crate::sync::log_sync::{LOG_SYNC_PROTOCOL_ID, LogSync, LogSyncError};

pub struct Builder<S, L, E>
where
    S: LogStore<Operation<E>, VerifyingKey, L, SeqNum, Hash>
        + TopicStore<Topic, VerifyingKey, L>
        + Clone
        + Send
        + 'static,
    L: LogId + Debug + Send + 'static,
    E: Extensions + Send + 'static,
{
    store: S,
    endpoint: Endpoint,
    gossip: Gossip,
    protocol_id: Vec<u8>,
    _marker: PhantomData<(L, E)>,
}

impl<S, L, E> Builder<S, L, E>
where
    S: LogStore<Operation<E>, VerifyingKey, L, SeqNum, Hash>
        + TopicStore<Topic, VerifyingKey, L>
        + Clone
        + Send
        + 'static,
    L: LogId + Debug + Send + 'static,
    E: Extensions + Send + 'static,
{
    pub fn new(store: S, endpoint: Endpoint, gossip: Gossip) -> Self {
        Self {
            store,
            endpoint,
            gossip,
            protocol_id: LOG_SYNC_PROTOCOL_ID.to_vec(),
            _marker: PhantomData,
        }
    }

    /// Name this instance's sync protocol id (ALPN).
    ///
    /// The endpoint keeps exactly one handler per protocol id, so every
    /// `LogSync` sharing an endpoint must name a distinct id: with the shared
    /// default, the last-spawned instance silently receives ALL inbound sync
    /// sessions and every other instance stops converging. Both peers must
    /// name the same id for the same lane, since the initiating side dials
    /// this id and the accepting side routes by it.
    pub fn protocol_id(mut self, protocol_id: impl AsRef<[u8]>) -> Self {
        self.protocol_id = protocol_id.as_ref().to_vec();
        self
    }

    pub async fn spawn(self) -> Result<LogSync<S, L, E>, LogSyncError<E>> {
        let (actor_ref, _) = {
            let thread_pool = ThreadLocalActorSpawner::new();

            let args = (
                self.protocol_id,
                self.store,
                self.endpoint,
                self.gossip,
            );

            SyncManager::<TopicSyncManager<Topic, S, L, E>>::spawn(None, args, thread_pool).await?
        };

        Ok(LogSync::new(actor_ref))
    }
}
