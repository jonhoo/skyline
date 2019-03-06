//! A wrapper for [`redis_async::client::PairedConnection`] that implements
//! [`tower_service::Service`].
//!
//! You can either wrap an existing [`redis_async::client::PairedConnection`] using `Redis::from`,
//! or you can use the [`Address`] service to generate one from a server address.
//!
//! This connection type _does_ support pipelining.

#![deny(missing_docs)]

use futures::{Async, Future, Poll};
use redis_async::client::PairedConnection;
use redis_async::resp::RespValue;

/// A Redis service implementation.
///
/// See [`tower_service::Service`] for how to use this type.
pub struct Redis(PairedConnection);

impl From<PairedConnection> for Redis {
    fn from(c: PairedConnection) -> Self {
        Redis(c)
    }
}

impl tower_service::Service<RespValue> for Redis {
    type Response = RespValue;
    type Error = redis_async::error::Error;
    // TODO: existential
    type Future = Box<Future<Item = Self::Response, Error = Self::Error>>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        Ok(Async::Ready(()))
    }

    fn call(&mut self, req: RespValue) -> Self::Future {
        Box::new(self.0.send(req))
    }
}

/// Connection options for connecting to a Redis instance using `redis-async`.
///
/// This type is primarily used as a [`tower_service::Service`] that generates new [`Redis`]
/// instances. See [`redis_async::client::paired::paired_connect`] for details.
pub struct Address(std::net::SocketAddr);
impl From<std::net::SocketAddr> for Address {
    fn from(o: std::net::SocketAddr) -> Self {
        Address(o)
    }
}
impl tower_service::Service<()> for Address {
    type Response = Redis;
    type Error = redis_async::error::Error;
    // TODO: existentials
    type Future = Box<Future<Item = Self::Response, Error = Self::Error> + Send>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        Ok(Async::Ready(()))
    }

    fn call(&mut self, _: ()) -> Self::Future {
        Box::new(redis_async::client::paired::paired_connect(&self.0).map(Redis::from))
    }
}
