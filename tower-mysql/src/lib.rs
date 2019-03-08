//! A wrapper for [`mysql_async::Conn`] that implements [`tower_service::Service`].
//!
//! You can either wrap an existing [`mysql_async::Conn`] using `Mysql::from`, or you can
//! use the [`Opts`] service to generate one from a configuration.
//!
//! Note that this connection type does _not_ support pipelining.

#![deny(missing_docs)]

use async_lease::Lease;
use futures::{Async, Future, Poll};
use my::{prelude::*, Conn};
use mysql_async as my;
use std::ops::Deref;

/// A MySQL service implementation.
///
/// See [`tower_service::Service`] for how to use this type.
pub struct Mysql(Lease<Conn>);

impl From<Conn> for Mysql {
    fn from(c: Conn) -> Self {
        Mysql(Lease::from(c))
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
        Ok(self.0.poll_acquire())
    }

    fn call(&mut self, PrepExec { query, params }: PrepExec<Q, P>) -> Self::Future {
        // we have a permit, since poll_ready must have returned Ok(Ready)
        let mut lease = self.0.transfer();
        Box::new(
            lease
                .take()
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
    // TODO: existentials
    type Future = Box<Future<Item = Self::Response, Error = Self::Error> + Send>;

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
