FoundationDB.jl
=========

[Julia](http://julialang.org) bindings for [FoundationDB](http://www.foundationdb.com), a distributed key-value store with ACID transactions.

This package requires Julia 0.2.0+ and supports FoundationDB 2.0 (API version 200). Use of this package requires the FoundationDB C API, part of the [FoundationDB clients package](https://foundationdb.com/get).

To install this package, in the Julia REPL run:

    Pkg.add("FoundationDB")
    
Work in progress, and only a blocking interface for now.

Documentation
-------------

Coming soon!

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
