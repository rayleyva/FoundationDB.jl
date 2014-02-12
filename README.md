fdb.jl
=========

Julia bindings for [FoundationDB](http://www.foundationdb.com) key-value store.

Work in progress, and only a blocking interface for now.

Simple Usage
------------

```julia

require("fdb.jl")
fdb.api_version(200)
fdb.enable_trace()
d = fdb.open()
t = fdb.create_transaction(d)
fdb.set(t, "foo", "bar")
fdb.commit(t)
println(fdb.get(t, "foo"))

```
