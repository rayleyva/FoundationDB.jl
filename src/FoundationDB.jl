module FoundationDB

global _network_thread_handle = 0
const global fdb_lib_name = @windows? "fdb_c" : "libfdb_c"
global _network_is_running = false

typealias Future Ptr{Void}
typealias Cluster Ptr{Void}
typealias Database Ptr{Void}
typealias Transaction Ptr{Void}
typealias FDBError Int32

type FDBException <: Exception
    msg::String
end

export  api_version,
        open,
        create_transaction,
        get,
        set,
        clear,
        clear_range,
        commit,
        enable_trace

include("Tuple.jl")

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

# Currently this is the API

##############################################################################################################################

function api_version(ver::Integer)
	@check_error ccall( (:fdb_select_api_version_impl, fdb_lib_name), Int32, (Int32, Int32), ver, 200 )
end

function open()
    if(!_network_is_running)
        init()
    end
    open_database(create_cluster())
end

function create_transaction(d::Database)
    out_ptr = Array(Ptr{Void}, 1)
    @check_error ccall( (:fdb_database_create_transaction, fdb_lib_name), Int32, (Ptr{Void}, Ptr{Ptr{Void}}), d, out_ptr)
    convert(Transaction, out_ptr[1])
end    

function get(tr::Transaction, key::ASCIIString)
    f = ccall( (:fdb_transaction_get, fdb_lib_name), Ptr{Void}, (Ptr{Void}, Ptr{Uint8}, Cint, Bool), tr, key, length(key), false )
    out_present = Bool[0]
    out_value = Array(Ptr{Uint8}, 1)
    out_value_length = Cint[0]
    block_until_ready(f)
    @check_error ccall( (:fdb_future_get_value, fdb_lib_name), Int32, (Ptr{Void}, Ptr{Bool}, Ptr{Ptr{Uint8}}, Ptr{Cint}), f, out_present, out_value, out_value_length)
    if(out_present[1])
        ans = bytestring(pointer_to_array(out_value[1], int64(out_value_length[1]), false))
        destroy(f)
        ans
    else
        destroy(f)
        nothing
    end
end

function set(tr::Transaction, key::String, value::String)
    ccall( (:fdb_transaction_set, fdb_lib_name), Void, (Ptr{Void}, Ptr{Uint8}, Cint, Ptr{Uint8}, Cint), tr, bytestring(key), length(key), bytestring(value), length(value))
end

function clear(tr::Transaction, key::String)
	ccall( (:fdb_transaction_clear, fdb_lib_name), Void, (Ptr{Void}, Ptr{Uint8}, Cint), tr, bytestring(key), length(key))
end

function clear_range(tr::Transaction, begin_key::String, end_key::String)
	ccall( (:fdb_transaction_clear_range, fdb_lib_name), Void, (Ptr{Void}, Ptr{Uint8}, Cint, Ptr{Uint8}, Cint), tr, bytestring(begin_key), length(begin_key), bytestring(end_key), length(end_key))
end

function commit(tr::Transaction)
    f = ccall( (:fdb_transaction_commit, fdb_lib_name), Ptr{Void}, (Ptr{Void},), tr)
    block_until_ready(f)
    @check_error get_error(f)
    destroy(f)
end

function enable_trace()
    @check_error ccall( (:fdb_network_set_option, fdb_lib_name), Int32, (Int32, Ptr{Void}, Int32), 30, C_NULL, 0)
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

function create_cluster()
    f = ccall( (:fdb_create_cluster, fdb_lib_name), Ptr{Void}, (Ptr{Void},), C_NULL)
    block_until_ready(f)
    out_ptr = Array(Ptr{Void}, 1)
    @check_error ccall( (:fdb_future_get_cluster, fdb_lib_name), Int32, (Ptr{Void}, Ptr{Ptr{Void}}), f, out_ptr)
    destroy(f)
    convert(Cluster, out_ptr[1])
end

function open_database(c::Cluster)
    f = ccall( (:fdb_cluster_create_database, fdb_lib_name), Ptr{Void}, (Ptr{Void}, Ptr{Uint8}, Cint), c, bytestring("DB"), 2)
    block_until_ready(f)
    out_ptr = Array(Ptr{Void}, 1)
    @check_error ccall( (:fdb_future_get_database, fdb_lib_name), Int32, (Ptr{Void}, Ptr{Ptr{Void}}), f, out_ptr)
    destroy(f)
    convert(Database, out_ptr[1])
end 

function _shutdown()
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

end