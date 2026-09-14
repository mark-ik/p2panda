// SPDX-License-Identifier: MIT OR Apache-2.0

//! Poll the sync manager for events and forward them to all subscribers.
use std::fmt::Debug;
use std::marker::PhantomData;

use futures_util::{FutureExt, Stream, StreamExt};
use p2panda_sync::FromSync;
use ractor::thread_local::ThreadLocalActor;
use ractor::{ActorProcessingErr, ActorRef};
use tokio::sync::{broadcast, oneshot};

pub enum ToSyncPoller {
    /// Wait for an event from the sync manager.
    WaitForEvent,
}

pub struct SyncPollerState<St, Ev> {
    stream: St,
    sender: broadcast::Sender<FromSync<Ev>>,
    /// Signals that no further events will be produced and the poller may terminate.
    finish: Option<oneshot::Receiver<()>>,
}

pub struct SyncPoller<S, Ev> {
    _marker: PhantomData<(S, Ev)>,
}

impl<St, Ev> Default for SyncPoller<St, Ev> {
    fn default() -> Self {
        Self {
            _marker: Default::default(),
        }
    }
}

impl<St, Ev> ThreadLocalActor for SyncPoller<St, Ev>
where
    St: Stream<Item = FromSync<Ev>> + Send + Unpin + 'static,
    Ev: Debug + Send + 'static,
{
    type State = SyncPollerState<St, Ev>;

    type Msg = ToSyncPoller;

    type Arguments = (St, broadcast::Sender<FromSync<Ev>>, oneshot::Receiver<()>);

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let (stream, sender, finish) = args;

        // Invoke the handler to wait for the first stream event.
        let _ = myself.cast(ToSyncPoller::WaitForEvent);

        Ok(SyncPollerState {
            stream,
            sender,
            finish: Some(finish),
        })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        _message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        // We need to keep polling next() on the stream in order for the manager to process and
        // return events coming from running sync sessions. We then forward these events onto all
        // subscribers.
        //
        // The stream only ends when the manager is dropped, which happens after the topic
        // manager's post-stop hook has already drained us. Selecting against a finish signal lets
        // an idle poller return from this handler at once, so that drain completes rather than
        // running out its timeout.
        let SyncPollerState {
            stream,
            sender,
            finish,
        } = state;

        // A repeat invocation has nothing left to poll.
        let Some(finish) = finish.as_mut() else {
            return Ok(());
        };

        loop {
            let done = tokio::select! {
                biased;
                // Resolves on signal or if the sender is dropped; both mean "stop polling".
                _ = &mut *finish => true,
                event = stream.next() => match event {
                    Some(event) => {
                        sender.send(event).map_err(|err| err.to_string())?;
                        false
                    }
                    None => true,
                },
            };

            if done {
                break;
            }
        }

        // Forward whatever the stream can still yield without waiting, so events already buffered
        // when the signal arrived are not dropped. Send errors are ignored here: subscribers may
        // already be gone and failing would turn a clean shutdown into an actor failure.
        while let Some(Some(event)) = stream.next().now_or_never() {
            let _ = sender.send(event);
        }

        Ok(())
    }
}
