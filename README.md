# Mercurius

![Logo](assets/logo.jpg)

Mercurius is a MongoDB Change Stream multiplexer. It allows you to freely create as many change streams as you want without them counting towards the connection pool limit.<br />
Mercurius uses a single change stream per collection and uses document selectors to route the updates dynamically to the subscribers.

## Meteor

This package was mainly designed to improve [Meteor's](https://meteor.com) slow oplog trailing approach; but it can also be used by other applications that desire to create large amounts of change streams.

**Advantages**

- Significant server performance improvement: the oplog processing is now handled by the DB and Mercurius. This means that the server is now free to perform other tasks.
- Very scalable: since every instance reads the oplog separately you can deploy multiple instances and load balance between them.
- One less single point of failure compared to [redis-oplog](https://github.com/cult-of-coders/redis-oplog) in combination with [oplogtoredis](https://github.com/tulip/oplogtoredis): there is no need for an intermediary connection forwarding (pub/sub) service. Furthermore, since this app can also scale very well, you can reduce risk by deploying multiple instances across multiple datacenters.

**Disadvantages**

- Only simple queries are supported: this package uses `serde_json_matcher` which currently supports `$eq`, `$in`, `$ne`, `$nin`, `$and`, `$not`, `$or`, `$type` and `$nor`. This should, however, be more than enough for most applications.
- Less efficient for large documents: for every update the full old and new documents are requested. This is can potentially cause issues if these are very large.
