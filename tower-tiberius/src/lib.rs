//! A wrapper for [`tiberius::SqlConnection`] that implements [`tower_service::Service`].
//!
//! You can either wrap an existing [`tiberius::SqlConnection`] using `Tiberius::from`, or you can
//! use the [`ConnectionStr`] service to generate one from a connection string.
//!
//! Note that this connection type does _not_ support pipelining.

#![deny(missing_docs)]

use async_lease::Lease;
use futures::{try_ready, Async, Future, Poll, Stream};
use futures_state_stream::{StateStream, StreamEvent};
use std::borrow::Cow;
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

/// This is mainly a [`futures::Future`]; you should use it as such.
// TODO: make this private using existentials.
pub struct RestoringStateFuture<S, F> {
    lease: Lease<S>,
    fut: F,
}

impl<T, S, F> Future for RestoringStateFuture<S, F>
where
    F: Future<Item = (T, S)>,
{
    type Item = T;
    type Error = F::Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.fut.poll() {
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Ok(Async::Ready((item, state))) => {
                self.lease.restore(state);
                Ok(Async::Ready(item))
            }
            Err(err) => {
                // We need https://github.com/steffengy/tiberius/issues/75
                // to be able to restore the connection.
                Err(err)
            }
        }
    }
}

/// Arguments to [`tiberius::SqlConnection::query`].
pub struct Query<'a, S> {
    /// The query string to execute.
    pub stmt: S,
    /// The arguments to query with.
    pub params: &'a [&'a dyn ty::ToSql],
}

impl<'a, I: BoxableIo + 'static, S> tower_service::Service<Query<'a, S>> for Tiberius<I>
where
    S: Into<stmt::Statement>,
{
    type Response =
        RestoringStateStream<stmt::QueryResult<stmt::StmtStream<I, query::QueryStream<I>>>>;
    type Error = <query::QueryStream<I> as Stream>::Error;
    type Future =
        RestoringStateStreamFuture<stmt::QueryResult<stmt::StmtStream<I, query::QueryStream<I>>>>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        Ok(self.0.poll_acquire())
    }

    fn call(&mut self, Query { stmt, params }: Query<'a, S>) -> Self::Future {
        // we have the lease, since poll_ready must have returned Ok(Ready)
        let fut = self.0.take().query(stmt, params).into_future();
        RestoringStateStreamFuture {
            lease: self.0.transfer(),
            fut,
        }
    }
}

/// Arguments to [`tiberius::SqlConnection::exec`].
pub struct Exec<'a, S> {
    /// The query string to execute.
    pub stmt: S,
    /// The arguments to execute with.
    pub params: &'a [&'a dyn ty::ToSql],
}

impl<'a, I: BoxableIo + 'static, S> tower_service::Service<Exec<'a, S>> for Tiberius<I>
where
    S: Into<stmt::Statement>,
{
    type Response = u64;
    type Error = <query::QueryStream<I> as Stream>::Error;
    type Future = RestoringStateFuture<
        SqlConnection<I>,
        stmt::ExecResult<stmt::StmtStream<I, query::ExecFuture<I>>>,
    >;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        Ok(self.0.poll_acquire())
    }

    fn call(&mut self, Exec { stmt, params }: Exec<'a, S>) -> Self::Future {
        // we have the lease, since poll_ready must have returned Ok(Ready)
        let fut = self.0.take().exec(stmt, params);
        RestoringStateFuture {
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
    // TODO: existentials
    type Future = Box<Future<Item = Self::Response, Error = Self::Error> + Send>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        Ok(Async::Ready(()))
    }

    fn call(&mut self, _: ()) -> Self::Future {
        Box::new(SqlConnection::connect(&*self.0).map(Tiberius::from))
    }
}
