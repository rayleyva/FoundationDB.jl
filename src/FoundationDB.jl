module FoundationDB

using StrPack

const global fdb_lib_name = @windows? "fdb_c" : "libfdb_c"
const global fdb_lib_header_version = 200
global _network_thread_handle = 0
global _network_is_running = false
global session_api_version = 0

typealias Future Ptr{Void}

type Cluster 
    cpointer::Ptr{Void}
end
type Database 
    dpointer::Ptr{Void}
end
type Transaction 
    tpointer::Ptr{Void}
end

typealias FDBError Int32
typealias Key Union(Array{Uint8}, ASCIIString)
typealias Value Union(Array {Uint8}, ASCIIString)

global clusters_and_databases = Dict{String, (Cluster, Database)}()

type FDBException <: Exception
    msg::String
end

type KeySelector
    reference::Key
    or_equal::Bool
    offset::Int
end

type KeyValue
    k::Key
    v::Value
end

export  #Type constructors
        KeySelector,
        
        #API Version selector
        api_version,
        
        #KeySelectors
        last_less_than,
        last_less_or_equal,
        first_greater_than,
        first_greater_or_equal
        
include("Tuple.jl")
include("generated.jl")

import Base.add

##############################################################################################################################

# check_error macro, and the function on which it depends

##############################################################################################################################

macro check_error(expr)
    quote
        local err = convert(FDBError, $expr)
        if(err != 0)
            throw(FDBException(get_error(err)))
        end
    end
end

function get_error(code::FDBError)
    ans = ccall( (:fdb_get_error, fdb_lib_name), Ptr{Uint8}, (Int32,), code )
    bytestring(ans)
end

##############################################################################################################################

# transactional macro -- wraps an expression with transactional retry logic

##############################################################################################################################

macro transactional(database, expr)
    quote
        local tr = create_transaction($database)
        ret = $expr
        commit(tr)
        ret
    end
end

##############################################################################################################################

# "Core" API that calls C functions and isn't metaprogrammed into existence

##############################################################################################################################

function api_version(ver::Integer)
    if session_api_version != 0
        throw(FDBException("API version already set!"))
    end
    
    if ver != 200
        throw(FDBException("The Julia bindings do not support versions prior to 200."))
    end

    @check_error ccall( (:fdb_select_api_version_impl, fdb_lib_name), Int32, (Int32, Int32), ver, fdb_lib_header_version )
 
    global session_api_version = ver
 
    syms = [:open, 
            :create_transaction,
            :get,
            :get_range,
            :get_range_startswith,
            :get_key,
            :set,
            :clear,
            :clear_range,
            :clear_range_startswith,
            :on_error,
            :commit,
            :reset,
            :cancel,
            :get_read_version,
            :set_read_version,
            :get_committed_version,
            :add_read_conflict_range,
            :add_write_conflict_range]
 
    #Make C API functions visible
    eval(Expr(:toplevel, Expr(:export, syms...)))
                                 
    #Generate atomic op functions and make them visible                             
    for k in keys(MutationType)
        local s = symbol(k)
        eval(quote
            $s(tr::Transaction,key::Key,param::Value) = _atomic_operation(tr, MutationType[$k][1], key, param)
        end)
        eval(quote
            function $s(db::Database, key::Key, param::Value)
                @transactional db _atomic_operation(tr, MutationType[$k][1], key, param)
            end
        end)
    end    
    
    syms = [symbol(k) for k in keys(MutationType)] 
    eval(Expr(:toplevel, Expr(:export, syms...)))
    
    #Generate functions to set various sorts of options, and make them visible
    for scope in [(DatabaseOption, Database), (TransactionOption, Transaction), (NetworkOption, nothing)]
        for k in keys(scope[1])
            local s = symbol("set_"*k)
            if scope[2] == nothing
                if scope[1][k][3] == nothing
                    eval(quote
                        $s() = _set_option($scope[1][$k][1], wrap_option_param(nothing))
                    end)
                else
                    eval(quote
                        $s(param::$scope[1][$k][3]) = _set_option($scope[1][$k][1], wrap_option_param(param))
                    end)
                end           
            else
                if scope[1][k][3] == nothing
                    eval(quote
                        $s(d::$scope[2]) = _set_option(d, $scope[1][$k][1], wrap_option_param(nothing))
                    end)
                else
                    eval(quote
                        $s(d::$scope[2], param::$scope[1][$k][3]) = _set_option(d, $scope[1][$k][1], wrap_option_param(param))
                    end)
                end
            end
        end
        
        syms = [symbol("set_"*k) for k in keys(scope[1])]
        eval(Expr(:toplevel, Expr(:export, syms...)))
    end
            
end

function open(cluster_file="")
    if(!_network_is_running)
        init()
    end
    if(haskey(clusters_and_databases, cluster_file))
        return clusters_and_databases[cluster_file][2]
    end
    c = create_cluster()
    d = open_database(c)
    clusters_and_databases[cluster_file] = (c,d)
    return d
end

function create_transaction(d::Database)
    out_ptr = Array(Ptr{Void}, 1)
    @check_error ccall( (:fdb_database_create_transaction, fdb_lib_name), Int32, (Ptr{Void}, Ptr{Ptr{Void}}), d.dpointer, out_ptr)
    t = Transaction(out_ptr[1])
    finalizer(t, destroy_transaction)
    return t
end    

function get(tr::Transaction, key::Key, snapshot::Bool=false)
    f = ccall( (:fdb_transaction_get, fdb_lib_name), Ptr{Void}, (Ptr{Void}, Ptr{Uint8}, Cint, Bool), tr.tpointer, key, length(key), snapshot )
    out_present = Bool[0]
    out_value = Array(Ptr{Uint8}, 1)
    out_value_length = Cint[0]
    block_until_ready(f)
    @check_error ccall( (:fdb_future_get_value, fdb_lib_name), Int32, (Ptr{Void}, Ptr{Bool}, Ptr{Ptr{Uint8}}, Ptr{Cint}), f, out_present, out_value, out_value_length)
    if(out_present[1])
        ans = outstr(pointer_to_array(out_value[1], int64(out_value_length[1]), false))
        destroy(f)
        ans
    else
        destroy(f)
        nothing
    end
end

function get_range(tr::Transaction, begin_key::KeySelector, end_key::KeySelector; limit::Int = 0, reverse::Bool = false, snapshot::Bool=false)
    return _get_range(tr, begin_key, end_key, limit, reverse, snapshot)
end

function get_range(tr::Transaction, begin_key::Key, end_key::KeySelector; limit::Int = 0, reverse::Bool = false, snapshot::Bool=false)
    return _get_range(tr, first_greater_or_equal(begin_key), end_key, limit, reverse, snapshot)
end

function get_range(tr::Transaction, begin_key::KeySelector, end_key::Key; limit::Int = 0, reverse::Bool = false, snapshot::Bool=false)
    return _get_range(tr, begin_key, first_greater_or_equal(end_key), limit, reverse, snapshot)
end

function get_range(tr::Transaction, begin_key::Key, end_key::Key; limit::Int = 0, reverse::Bool = false, snapshot::Bool=false)
    return _get_range(tr, first_greater_or_equal(begin_key), first_greater_or_equal(end_key), limit, reverse, snapshot)
end

function get_range_startswith(tr::Transaction, prefix::Key, snapshot::Bool=false)
    return get_range(tr, prefix, strinc(prefix), snapshot=snapshot)
end

function get_key(tr::Transaction, ks::KeySelector, snapshot::Bool=false)
    f = ccall( (:fdb_transaction_get_key, fdb_lib_name), Ptr{Void}, (Ptr{Void}, Ptr{Uint8}, Cint, Bool, Cint, Bool), tr.tpointer, ks.reference, length(ks.reference), ks.or_equal, ks.offset, snapshot)
    out_key = Array(Ptr{Uint8}, 1)
    out_key_length = Cint[0]
    block_until_ready(f)
    @check_error ccall( (:fdb_future_get_key, fdb_lib_name), Int32, (Ptr{Void}, Ptr{Ptr{Uint8}}, Ptr{Cint}), f, out_key, out_key_length)
    ans = outstr(pointer_to_array(out_key[1], int64(out_key_length[1]), false))
    destroy(f)
    ans
end

function get_read_version(tr::Transaction)
    f = ccall( (:fdb_transaction_get_read_version, fdb_lib_name), Ptr{Void}, (Ptr{Void},), tr.tpointer)
    out_version = Int64[0]
    block_until_ready(f)
    @check_error ccall( (:fdb_future_get_version, fdb_lib_name), Int32, (Ptr{Void}, Ptr{Clong}), f, out_version)
    ans = out_version[1]
    destroy(f)
    ans
end

function get_committed_version(tr::Transaction)
    out_version = Int64[0]
    @check_error ccall( (:fdb_transaction_get_committed_version, fdb_lib_name), Int32, (Ptr{Void}, Ptr{Clong}), tr.tpointer, out_version)
    return out_version[1]
end

function set(tr::Transaction, key::Key, value::Value)
    ccall( (:fdb_transaction_set, fdb_lib_name), Void, (Ptr{Void}, Ptr{Uint8}, Cint, Ptr{Uint8}, Cint), tr.tpointer, bytestring(key), length(key), bytestring(value), length(value))
end

function set_read_version(tr::Transaction, ver::Int64)
    f = ccall( (:fdb_transaction_set_read_version, fdb_lib_name), Void, (Ptr{Void}, Clong), tr.tpointer, ver)
end

function clear(tr::Transaction, key::Key)
    ccall( (:fdb_transaction_clear, fdb_lib_name), Void, (Ptr{Void}, Ptr{Uint8}, Cint), tr.tpointer, bytestring(key), length(key))
end

function clear_range(tr::Transaction, begin_key::Key, end_key::Key)
    ccall( (:fdb_transaction_clear_range, fdb_lib_name), Void, (Ptr{Void}, Ptr{Uint8}, Cint, Ptr{Uint8}, Cint), tr.tpointer, bytestring(begin_key), length(begin_key), bytestring(end_key), length(end_key))
end

function clear_range_startswith(tr::Transaction, prefix::Key)
    clear_range(tr, prefix, strinc(prefix))
end

function add_read_conflict_range(tr::Transaction, begin_key::Key, end_key::Key)
    @check_error ccall( (:fdb_transaction_add_conflict_range, fdb_lib_name), Int32, (Ptr{Void}, Ptr{Uint8}, Cint, Ptr{Uint8}, Cint, Cint), tr.tpointer, bytestring(begin_key), length(begin_key), bytestring(end_key), length(end_key), ConflictRangeType["read"][1])
end

function add_read_conflict_range(tr::Transaction, key::Key)
    add_read_conflict_range(tr, key, [[convert(Uint8,i) for i in key],[0x00]])
end

function add_write_conflict_range(tr::Transaction, begin_key::Key, end_key::Key)
    @check_error ccall( (:fdb_transaction_add_conflict_range, fdb_lib_name), Int32, (Ptr{Void}, Ptr{Uint8}, Cint, Ptr{Uint8}, Cint, Cint), tr.tpointer, bytestring(begin_key), length(begin_key), bytestring(end_key), length(end_key), ConflictRangeType["write"][1])
end

function add_write_conflict_range(tr::Transaction, key::Key)
    add_write_conflict_range(tr, key, [[convert(Uint8,i) for i in key],[0x00]])
end

function on_error(tr::Transaction, err::FDBError)
    f = ccall( (:fdb_transaction_on_error, fdb_lib_name), Ptr{Void}, (Ptr{Void}, Cint), tr.tpointer, err)
    block_until_ready(f)
    @check_error get_error(f)
end

function commit(tr::Transaction)
    f = ccall( (:fdb_transaction_commit, fdb_lib_name), Ptr{Void}, (Ptr{Void},), tr.tpointer)
    block_until_ready(f)
    @check_error get_error(f)
    destroy(f)
end

function reset(tr::Transaction)
    ccall( (:fdb_transaction_reset, fdb_lib_name), Ptr{Void}, (Ptr{Void},), tr.tpointer)
end

function cancel(tr::Transaction)
    ccall( (:fdb_transaction_cancel, fdb_lib_name), Ptr{Void}, (Ptr{Void},), tr.tpointer)
end

##############################################################################################################################

# Database companion methods

##############################################################################################################################

function get(db::Database, key::Key, snapshot::Bool=false)
    @transactional db get(tr, key, snapshot)
end

function get_range(db::Database, begin_key::Key, end_key::Key; limit::Int = 0, reverse::Bool = false, snapshot::Bool=false)
    @transactional db get_range(tr, begin_key, end_key, limit=limit, reverse=reverse, snapshot=snapshot)
end

function get_range(db::Database, begin_key::KeySelector, end_key::Key; limit::Int = 0, reverse::Bool = false, snapshot::Bool=false)
    @transactional db get_range(tr, begin_key, end_key, limit=limit, reverse=reverse, snapshot=snapshot)
end

function get_range(db::Database, begin_key::Key, end_key::KeySelector; limit::Int = 0, reverse::Bool = false, snapshot::Bool=false)
    @transactional db get_range(tr, begin_key, end_key, limit=limit, reverse=reverse, snapshot=snapshot)
end

function get_range(db::Database, begin_key::KeySelector, end_key::KeySelector; limit::Int = 0, reverse::Bool = false, snapshot::Bool=false)
    @transactional db get_range(tr, begin_key, end_key, limit=limit, reverse=reverse, snapshot=snapshot)
end

function get_range_startswith(db::Database, prefix::Key, snapshot::Bool = false)
    @transactional db get_range_startswith(tr, prefix, snapshot)
end

function get_key(db::Database, ks::KeySelector, snapshot::Bool=false)
    @transactional db get_key(tr, ks, snapshot)
end

function set(db::Database, key::Key, value::Value)
    @transactional db set(tr, key, value)
end

function clear(db::Database, key::Key)
    @transactional db clear(tr, key)
end

function clear_range(db::Database, begin_key::Key, end_key::Key)
    @transactional db clear_range(tr, begin_key, end_key)
end

function clear_range_startswith(db::Database, prefix::Key)
    @transactional db clear_range_startswith(tr, prefix)
end

##############################################################################################################################

# Convenience methods for common KeySelectors

##############################################################################################################################

function last_less_than(k::Key)
    return KeySelector(k, false, 0)
end

function last_less_or_equal(k::Key)
    return KeySelector(k, true, 0)
end

function first_greater_than(k::Key)
    return KeySelector(k, true, 1)
end

function first_greater_or_equal(k::Key)
    return KeySelector(k, false, 1)
end

function +(ks::KeySelector, i::Int)
    return KeySelector(ks.reference, ks.or_equal, ks.offset+i)
end

function -(ks::KeySelector, i::Int)
    return KeySelector(ks.reference, ks.or_equal, ks.offset-i)
end

##############################################################################################################################

# Low-level methods on futures (not currently relevant to users, since this binding is currently all-blocking

##############################################################################################################################

function is_ready(f::Future)
    ccall( (:fdb_future_is_ready, fdb_lib_name), Bool, (Ptr{Void},), f)
end

function cancel(f::Future)
    ccall( (:fdb_future_cancel, fdb_lib_name), Void, (Ptr{Void},), f)
end

function destroy(f::Future)
    ccall( (:fdb_future_destroy, fdb_lib_name), Void, (Ptr{Void},), f)
end

function release_memory(f::Future)
    ccall( (:fdb_future_release_memory, fdb_lib_name), Void, (Ptr{Void},), f)
end

function block_until_ready(f::Future)
    ccall( (:fdb_future_block_until_ready, fdb_lib_name), Void, (Ptr{Void},), f)
end

function get_error(f::Future)
    ans = ccall( (:fdb_future_get_error, fdb_lib_name), Int32, (Ptr{Uint8},), f )
    convert(FDBError, ans)
end

##############################################################################################################################

# Stuff that most people don't have to call

##############################################################################################################################

function init()
    @check_error ccall( (:fdb_setup_network, fdb_lib_name), Int32, ())

    _run_network = cglobal((:fdb_run_network, fdb_lib_name), Ptr{Void} )
    @windows? (
    begin
        global _network_thread_handle = ccall( :_beginthread, cdecl, Int, (Ptr{Void}, Cuint, Ptr{Void}), _run_network, 0, C_NULL)
    end
    :
    begin
        out_handle = Clonglong[0]
        pthread_return_code = ccall( (:pthread_create, "libpthread"), Cint, (Ptr{Clonglong}, Ptr{Void}, Ptr{Void}, Ptr{Void}), out_handle, C_NULL, _run_network, C_NULL)
        global _network_thread_handle = out_handle[1]
    end
    )
    global _network_is_running = true
    atexit(_shutdown)
end

function create_cluster(cluster_file="")
    f = ccall( (:fdb_create_cluster, fdb_lib_name), Ptr{Void}, (Ptr{Uint8},), bytestring(cluster_file))
    block_until_ready(f)
    out_ptr = Array(Ptr{Void}, 1)
    @check_error ccall( (:fdb_future_get_cluster, fdb_lib_name), Int32, (Ptr{Void}, Ptr{Ptr{Void}}), f, out_ptr)
    destroy(f)
    return Cluster(out_ptr[1])
end

function open_database(c::Cluster)
    f = ccall( (:fdb_cluster_create_database, fdb_lib_name), Ptr{Void}, (Ptr{Void}, Ptr{Uint8}, Cint), c.cpointer, bytestring("DB"), 2)
    block_until_ready(f)
    out_ptr = Array(Ptr{Void}, 1)
    @check_error ccall( (:fdb_future_get_database, fdb_lib_name), Int32, (Ptr{Void}, Ptr{Ptr{Void}}), f, out_ptr)
    destroy(f)
    return Database(out_ptr[1])
end 

function destroy_cluster(c::Cluster)
    ccall( (:fdb_cluster_destroy, fdb_lib_name), Int32, (Ptr{Void},), c.cpointer)
end

function destroy_database(d::Database)
    ccall( (:fdb_database_destroy, fdb_lib_name), Int32, (Ptr{Void},), d.dpointer)
end

function destroy_transaction(t::Transaction)
    ccall( (:fdb_transaction_destroy, fdb_lib_name), Int32, (Ptr{Void},), t.tpointer)
end

@struct immutable type FDBKeyValueArray_C
    key::Ptr{Uint8}
    key_length::Int32
    value::Ptr{Uint8}
    value_length::Int32 
end align_packmax(4)

function _get_range(tr::Transaction, begin_ks::KeySelector, end_ks::KeySelector, limit::Int, reverse::Bool, snapshot::Bool = false)
    #omg
    mode = limit > 0 ? StreamingMode["exact"][1] : StreamingMode["want_all"][1]
    f = ccall( (:fdb_transaction_get_range, fdb_lib_name), Ptr{Void}, 
    (Ptr{Void}, Ptr{Uint8}, Cint, Bool, Cint, Ptr{Uint8}, Cint, Bool, Cint, Cint, Cint, Cint, Cint, Bool, Bool),
    tr.tpointer, begin_ks.reference, length(begin_ks.reference), begin_ks.or_equal, begin_ks.offset, 
    end_ks.reference, length(end_ks.reference), end_ks.or_equal, end_ks.offset, limit, 0, mode, 0, snapshot, reverse)
    
    out_kvs = Array(Ptr{Uint8}, 1)
    out_count = Cint[0]
    out_more = Bool[0]
    block_until_ready(f)
    @check_error ccall( (:fdb_future_get_keyvalue_array, fdb_lib_name), Int32, (Ptr{Void}, Ptr{Ptr{Uint8}}, Ptr{Cint}, Ptr{Bool}), f, out_kvs, out_count, out_more)
        
    ret = KeyValue[]
    
    if out_count[1] == 0
        destroy(f)
        return ret
    end
    
    println(out_count[1])
        
    kvs = IOBuffer(pointer_to_array(out_kvs[1], 24*out_count[1]))       
        
    for i = 1:out_count[1]
        seek(kvs, (i-1)*24)
        kv = StrPack.unpack(kvs, FDBKeyValueArray_C)
        push!(ret, KeyValue(outstr(pointer_to_array(kv.key, int64(kv.key_length))), outstr(pointer_to_array(kv.value, int64(kv.value_length)))))
    end
    
    destroy(f)
    return ret
    
end

function _atomic_operation(tr::Transaction, code::Int, key::Key, param::Value)
    ccall( (:fdb_transaction_atomic_op, fdb_lib_name), Void, (Ptr{Void}, Ptr{Uint8}, Cint, Ptr{Uint8}, Cint, Cint), tr.tpointer, bytestring(key), length(key), bytestring(param), length(param), code)
end

function _set_option(code::Int, param::Array{Uint8})
    @check_error ccall( (:fdb_network_set_option, fdb_lib_name), Int32, (Int32, Ptr{Uint8}, Int32), code, param, param == C_NULL ? 0 : length(param))
end

function _set_option(tr::Transaction, code::Int, param::Union(Array{Uint8}, Ptr{None}))
    @check_error ccall( (:fdb_transaction_set_option, fdb_lib_name), Int32, (Ptr{Void}, Int32, Ptr{Uint8}, Int32), tr.tpointer, code, param, param == C_NULL ? 0 : length(param))
end

function _set_option(c::Cluster, code::Int, param::Union(Array{Uint8}, Ptr{None}))
    @check_error ccall( (:fdb_cluster_set_option, fdb_lib_name), Int32, (Ptr{Void}, Int32, Ptr{Uint8}, Int32), c.cpointer, code, param, param == C_NULL ? 0 : length(param))
end

function _set_option(d::Database, code::Int, param::Union(Array{Uint8}, Ptr{None}))
    @check_error ccall( (:fdb_database_set_option, fdb_lib_name), Int32, (Ptr{Void}, Int32, Ptr{Uint8}, Int32), d.dpointer, code, param, param == C_NULL ? 0 : length(param))
end

function _shutdown()
    for (c,d) in values(clusters_and_databases)
        destroy_database(d)
        destroy_cluster(c)
    end
    
    @check_error ccall( (:fdb_stop_network, fdb_lib_name), Int32, ())

    @windows? (
    begin
        windows_return_code = ccall( (:WaitForSingleObject, "Kernel32"), Uint32, (Ptr{Void}, Uint32), _network_thread_handle, 0xffffffff )
    end
    :
    begin
        out_ptr = Ptr{Void}[0]
        println(_network_thread_handle)
        pthread_return_code = ccall( (:pthread_join, "libpthread"), Int32, (Clonglong, Ptr{Ptr{Void}}), _network_thread_handle, out_ptr)
    end
    )
end

function wrap_option_param(param::Array{Uint8})
    return param
end

function wrap_option_param(param::Nothing)
    return C_NULL
end

function wrap_option_param(param::String)
    return [convert(Uint8, c) for c in bytestring(param)]
end

@struct immutable type int_param
    val::Int64
end align_default :LittleEndian

function wrap_option_param(param::Int64)
    iostr = IOString()
    StrPack.pack(iostr, int_param(param))
    buff = Array(Uint8, 8)
    seekstart(iostr)
    readbytes!(iostr, buff)
    return buff
end

function strinc(k::Key)
    if length(k) == 0
        throw(FDBException("Key must contain at least one byte not equal to 0xff."))
    end
    
    k = [convert(Uint8, i) for i in k]
    
    while true
        if k[end] == 0xff
            pop!(k)
        else
            break
        end
    end
        
    if length(k) == 0
        throw(FDBException("Key must contain at least one byte not equal to 0xff."))
    end
    
    k = _flat([k[1:end-1], convert(Uint8,k[end]+1)])
    if length(k) == 1
        k = [k]
    end
    if is_valid_ascii(k)
        return bytestring(k)
    else
        return k
    end
end

# Makes a copy
function outstr(k::Array{Uint8})
    k = bytestring(k)
    if is_valid_ascii(k)
        return k
    else
        return convert(Array{Uint8}, k)
    end
end

end