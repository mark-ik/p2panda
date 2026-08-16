// SPDX-License-Identifier: MIT OR Apache-2.0

use ractor::thread_local::{ThreadLocalActor, ThreadLocalActorSpawner};

use crate::address_book::actor::AddressBookActor;
use crate::address_book::store::AddressBookStoreHandle;
use crate::address_book::{AddressBook, AddressBookError};

pub struct Builder {
    pub(crate) store: Option<AddressBookStoreHandle>,
}

impl Builder {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self { store: None }
    }

    pub fn store(mut self, store: AddressBookStoreHandle) -> Self {
        self.store = Some(store);
        self
    }

    pub async fn spawn(self) -> Result<AddressBook, AddressBookError> {
        let store = match self.store {
            Some(store) => store,
            None => default_store().await?,
        };

        let (actor_ref, _) = {
            let thread_pool = ThreadLocalActorSpawner::new();
            let args = (store,);
            AddressBookActor::spawn(None, args, thread_pool).await?
        };

        Ok(AddressBook::new(Some(actor_ref)))
    }
}

/// Falls back to an in-memory SQLite address book when no store was given.
///
/// Only available with the `sqlite` feature. Without it the address book has no
/// backend it could pick on its own, so a store has to be supplied explicitly.
#[cfg(feature = "sqlite")]
async fn default_store() -> Result<AddressBookStoreHandle, AddressBookError> {
    use p2panda_store::SqliteStoreBuilder;

    use crate::address_book::store::StoreError;

    let store = SqliteStoreBuilder::new()
        .build()
        .await
        .map_err(|err| AddressBookError::Store(StoreError::new(err)))?;
    Ok(AddressBookStoreHandle::with_transactions(store))
}

#[cfg(not(feature = "sqlite"))]
async fn default_store() -> Result<AddressBookStoreHandle, AddressBookError> {
    Err(AddressBookError::NoStore)
}
