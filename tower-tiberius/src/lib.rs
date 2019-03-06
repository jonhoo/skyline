//! A wrapper for [`tiberius::SqlConnection`] that implements [`tower_service::Service`].
//!
//! You can either wrap an existing [`tiberius::SqlConnection`] using `Tiberius::from`, or you can
//! use the [`ConnectionStr`] service to generate one from a connection string.

#![deny(missing_docs)]

use futures::{try_ready, Async, Future, Poll, Stream};
use futures_state_stream::{StateStream, StreamEvent};
use std::borrow::Cow;
use std::cell::UnsafeCell;
use std::sync::Arc;
use tiberius::*;

/// A Microsoft SQL Server service implementation.
///
/// See [`tower_service::Service`] for how to use this type.
pub struct Tiberius<I: BoxableIo>(Lease<SqlConnection<I>>);

impl<I: BoxableIo> From<SqlConnection<I>> for Tiberius<I> {
    fn from(c: SqlConnection<I>) -> Self {
        Tiberius(Lease::from(c))
    }
}

struct Lease<S> {
    inner: Arc<StateInner<S>>,
    permit: tokio_sync::semaphore::Permit,
}

unsafe impl<S> Send for Lease<S> where S: Send + Sync {}

impl<S> From<S> for Lease<S> {
    fn from(s: S) -> Self {
        Self {
            inner: Arc::new(StateInner::from(s)),
            permit: tokio_sync::semaphore::Permit::new(),
        }
    }
}

impl<S> Lease<S> {
    fn transfer(&mut self) -> Self {
        assert!(self.permit.is_acquired());
        Self {
            inner: self.inner.clone(),
            permit: std::mem::replace(&mut self.permit, tokio_sync::semaphore::Permit::new()),
        }
    }

    fn restore(&mut self, state: S) {
        assert!(self.permit.is_acquired());
        unsafe { *self.inner.c.get() = Some(state) };
        // finally, we can now release the permit since we're done with the connection
        self.permit.release(&self.inner.s);
    }
}

struct StateInner<S> {
    c: UnsafeCell<Option<S>>,
    s: tokio_sync::semaphore::Semaphore,
}

impl<S> From<S> for StateInner<S> {
    fn from(s: S) -> Self {
        StateInner {
            c: UnsafeCell::new(Some(s)),
            s: tokio_sync::semaphore::Semaphore::new(1),
        }
    }
}

/// This is mainly a [`futures::Stream`]; you should use it as such.
// TODO: make this private using existentials.
pub struct RestoringStateStream<S: StateStream> {
    first: Option<S::Item>,
    state: Option<S::State>,
    lease: Lease<S::State>,
    rest: S,
}

/// This is mainly a [`futures::Future`]; you should use it as such.
// TODO: make this private using existentials.
pub struct RestoringStateStreamFuture<S: StateStream> {
    lease: Lease<S::State>,
    fut: futures_state_stream::IntoFuture<S>,
}

impl<S: StateStream> Future for RestoringStateStreamFuture<S> {
    type Item = RestoringStateStream<S>;
    type Error = S::Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.fut.poll() {
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Ok(Async::Ready((StreamEvent::Next(item), stream))) => {
                Ok(Async::Ready(RestoringStateStream {
                    first: Some(item),
                    lease: self.lease.transfer(),
                    rest: stream,
                    state: None,
                }))
            }
            Ok(Async::Ready((StreamEvent::Done(state), stream))) => {
                Ok(Async::Ready(RestoringStateStream {
                    first: None,
                    lease: self.lease.transfer(),
                    rest: stream,
                    state: Some(state),
                }))
            }
            Err((err, _stream)) => {
                // We need https://github.com/sfackler/futures-state-stream/pull/3
                // or rather https://github.com/steffengy/tiberius/issues/75
                // to be able to restore the connection.
                Err(err)
            }
        }
    }
}

impl<S: StateStream> RestoringStateStream<S> {
    fn restore_state(&mut self) -> Poll<Option<S::Item>, S::Error> {
        self.lease.restore(
            self.state
                .take()
                .expect("told to restore state, but don't have state"),
        );
        return Ok(Async::Ready(None));
    }
}

impl<S: StateStream> Stream for RestoringStateStream<S> {
    type Item = S::Item;
    type Error = S::Error;
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        if let Some(f) = self.first.take() {
            return Ok(Async::Ready(Some(f)));
        }
        if self.state.is_some() {
            return self.restore_state();
        }

        match try_ready!(self.rest.poll()) {
            StreamEvent::Next(item) => Ok(Async::Ready(Some(item))),
            StreamEvent::Done(state) => {
                self.state = Some(state);
                return self.restore_state();
            }
        }
    }
}

impl<I: BoxableIo + 'static, S> tower_service::Service<(S, &[&dyn ty::ToSql])> for Tiberius<I>
where
    S: Into<stmt::Statement>,
{
    type Response =
        RestoringStateStream<stmt::QueryResult<stmt::StmtStream<I, query::QueryStream<I>>>>;
    type Error = <query::QueryStream<I> as Stream>::Error;
    type Future =
        RestoringStateStreamFuture<stmt::QueryResult<stmt::StmtStream<I, query::QueryStream<I>>>>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        self.0.permit.poll_acquire(&self.0.inner.s).map_err(|_| {
            // the semaphore was closed, but we have a handle to it!
            unreachable!()
        })
    }

    fn call(&mut self, (stmt, params): (S, &[&dyn ty::ToSql])) -> Self::Future {
        // we have a permit, since poll_ready must have returned Ok(Ready)
        let fut = unsafe { &mut *self.0.inner.c.get() }
            .take()
            .expect("we got a permit, but the connection wasn't there")
            .query(stmt, params)
            .into_future();

        RestoringStateStreamFuture {
            lease: self.0.transfer(),
            fut,
        }
    }
}

// we can't provide <I> + ConnectParams w/o: https://github.com/steffengy/tiberius/issues/95

/// A connection string for connecting to a Microsoft SQL Server instance using `tiberius`.
///
/// This type is primarily used as a [`tower_service::Service`] that generates new [`Tiberius`]
/// instances.
pub struct ConnectionStr<'a>(Cow<'a, str>);
impl<'a> From<&'a str> for ConnectionStr<'a> {
    fn from(s: &'a str) -> Self {
        ConnectionStr(Cow::from(s))
    }
}
impl From<String> for ConnectionStr<'static> {
    fn from(s: String) -> Self {
        ConnectionStr(Cow::from(s))
    }
}
impl<'a> tower_service::Service<()> for ConnectionStr<'a> {
    type Response = Tiberius<Box<dyn BoxableIo>>;
    type Error = Error;
    type Future = Box<Future<Item = Self::Response, Error = Self::Error>>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        Ok(Async::Ready(()))
    }

    fn call(&mut self, _: ()) -> Self::Future {
        Box::new(SqlConnection::connect(&*self.0).map(Tiberius::from))
    }
}
