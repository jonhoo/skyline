//! A wrapper for [`mysql_async::Conn`] that implements [`tower_service::Service`].
//!
//! You can either wrap an existing [`mysql_async::Conn`] using `Mysql::from`, or you can
//! use the [`Opts`] service to generate one from a configuration.

#![deny(missing_docs)]

use futures::{Async, Future, Poll};
use my::{prelude::*, Conn};
use mysql_async as my;
use std::cell::UnsafeCell;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

/// A MySQL service implementation.
///
/// See [`tower_service::Service`] for how to use this type.
pub struct Mysql(Lease<Conn>);

impl From<Conn> for Mysql {
    fn from(c: Conn) -> Self {
        Mysql(Lease::from(c))
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

impl<S> Deref for Lease<S> {
    type Target = Option<S>;
    fn deref(&self) -> &Self::Target {
        assert!(self.permit.is_acquired());
        unsafe { &*self.inner.c.get() }
    }
}

impl<S> DerefMut for Lease<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        assert!(self.permit.is_acquired());
        unsafe { &mut *self.inner.c.get() }
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

/// This is mainly a [`mysql_async::QueryResult`]; you should use it as such.
pub struct QueryResult {
    result: my::QueryResult<Conn, my::BinaryProtocol>,
    lease: Lease<Conn>,
}

impl Deref for QueryResult {
    type Target = my::QueryResult<Conn, my::BinaryProtocol>;
    fn deref(&self) -> &Self::Target {
        &self.result
    }
}

impl QueryResult {
    /// See [`mysql_async::QueryResult::collect`].
    pub fn collect<R>(self) -> impl my::MyFuture<(Self, Vec<R>)>
    where
        R: FromRow,
        R: Send + 'static,
    {
        let QueryResult { result, lease } = self;
        result
            .collect()
            .map(move |(result, v)| (QueryResult { result, lease }, v))
    }

    /// See [`mysql_async::QueryResult::for_each`].
    pub fn for_each<F>(self, fun: F) -> impl Future<Item = Self, Error = my::error::Error>
    where
        F: FnMut(my::Row),
    {
        let QueryResult { result, lease } = self;
        result
            .for_each(fun)
            .map(move |result| QueryResult { result, lease })
    }

    /// See [`mysql_async::QueryResult::drop_result`].
    ///
    /// Once the result has been dropped, the [`Mysql::poll_ready`] will return `Ready` again.
    pub fn drop_result(self) -> impl my::MyFuture<()> {
        let QueryResult { result, mut lease } = self;
        result.drop_result().map(move |c| {
            lease.restore(c);
        })
    }
}

/// Arguments to [`mysql_async::Conn::prep_exec`].
pub struct PrepExec<Q, P> {
    /// The query string to execute.
    pub query: Q,
    /// The arguments to execute with.
    pub params: P,
}

impl<Q, P> tower_service::Service<PrepExec<Q, P>> for Mysql
where
    Q: AsRef<str>,
    P: Into<my::Params>,
{
    type Response = QueryResult;
    type Error = my::error::Error;
    // TODO: existential
    type Future = my::BoxFuture<Self::Response>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        self.0.permit.poll_acquire(&self.0.inner.s).map_err(|_| {
            // the semaphore was closed, but we have a handle to it!
            unreachable!()
        })
    }

    fn call(&mut self, PrepExec { query, params }: PrepExec<Q, P>) -> Self::Future {
        // we have a permit, since poll_ready must have returned Ok(Ready)
        let lease = self.0.transfer();
        Box::new(
            self.0
                .take()
                .expect("we got a permit, but the connection wasn't there")
                .prep_exec(query, params)
                .map(move |result| QueryResult { result, lease }),
        )
    }
}

/// Connection options for connecting to a MySQL instance using `mysql_async`.
///
/// This type is primarily used as a [`tower_service::Service`] that generates new [`Mysql`]
/// instances. See [`mysql_async::Opts`] for details.
pub struct Opts(my::Opts);
impl From<my::OptsBuilder> for Opts {
    fn from(o: my::OptsBuilder) -> Self {
        Opts(my::Opts::from(o))
    }
}
impl std::str::FromStr for Opts {
    type Err = my::error::UrlError;

    fn from_str(s: &str) -> std::result::Result<Self, <Self as std::str::FromStr>::Err> {
        Self::from_url(s)
    }
}
impl<'a> From<&'a str> for Opts {
    fn from(url: &'a str) -> Opts {
        Opts::from_url(url).unwrap()
    }
}
impl tower_service::Service<()> for Opts {
    type Response = Mysql;
    type Error = my::error::Error;
    type Future = Box<Future<Item = Self::Response, Error = Self::Error>>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        Ok(Async::Ready(()))
    }

    fn call(&mut self, _: ()) -> Self::Future {
        Box::new(Conn::new(self.0.clone()).map(Mysql::from))
    }
}
impl Deref for Opts {
    type Target = my::Opts;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl Opts {
    /// See [`mysql_async::Opts::from_url`].
    pub fn from_url(url: &str) -> Result<Self, my::error::UrlError> {
        Ok(Opts(my::Opts::from_url(url)?))
    }
}
