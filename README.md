FoundationDB.jl
=========

Julia bindings for [FoundationDB](http://www.foundationdb.com) key-value store.

Work in progress, and only a blocking interface for now.

Simple Usage
------------

```julia

using FoundationDB
api_version(200)
enable_trace()
d = open()
t = create_transaction(d)
set(t, "foo", "bar")
commit(t)
println(get(t, "foo"))

```
