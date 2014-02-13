module FoundationDB

using StrPack

const global fdb_lib_name = @windows? "fdb_c" : "libfdb_c"
const global fdb_lib_header_version = 200
global _network_thread_handle = 0
global _network_is_running = false

typealias Future Ptr{Void}
typealias Cluster Ptr{Void}
typealias Database Ptr{Void}
typealias Transaction Ptr{Void}
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

export  #Types
		Future,
		Cluster,
		Darabase,
		Transaction,
		FDBError,
		Key,
		Value,
		KeyValue,
		KeySelector,

		# Methods
		api_version,
        open,
        create_transaction,
        get,
		get_range,
		get_range_startswith,
		get_key,
        set,
        clear,
        clear_range,
		clear_range_startswith,
        commit,
		reset,
		cancel,
        enable_trace,
		
		#KeySelectors
		last_less_than,
		last_less_or_equal,
		first_greater_than,
		first_greater_or_equal

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
	@check_error ccall( (:fdb_select_api_version_impl, fdb_lib_name), Int32, (Int32, Int32), ver, fdb_lib_header_version )
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
    @check_error ccall( (:fdb_database_create_transaction, fdb_lib_name), Int32, (Ptr{Void}, Ptr{Ptr{Void}}), d, out_ptr)
    convert(Transaction, out_ptr[1])
end    

function get(tr::Transaction, key::Key)
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

function get_range(tr::Transaction, begin_key::KeySelector, end_key::KeySelector, limit::Int = 0, reverse::Bool = false)
	return _get_range(tr, begin_key, end_key, limit, reverse)
end

function get_range(tr::Transaction, begin_key::Key, end_key::KeySelector, limit::Int = 0, reverse::Bool = false)
	return _get_range(tr, first_greater_or_equal(begin_key), end_key, limit, reverse)
end

function get_range(tr::Transaction, begin_key::KeySelector, end_key::Key, limit::Int = 0, reverse::Bool = false)
	return _get_range(tr, begin_key, first_greater_or_equal(end_key), limit, reverse)
end

function get_range(tr::Transaction, begin_key::Key, end_key::Key, limit::Int = 0, reverse::Bool = false)
	return _get_range(tr, first_greater_or_equal(begin_key), first_greater_or_equal(end_key), limit, reverse)
end

function get_range_startswith(tr::Transaction, prefix::Key)
	return get_range(tr, prefix, strinc(prefix))
end

function get_key(tr::Transaction, ks::KeySelector)
	f = ccall( (:fdb_transaction_get_key, fdb_lib_name), Ptr{Void}, (Ptr{Void}, Ptr{Uint8}, Cint, Bool, Cint, Bool), tr, ks.reference, length(ks.reference), ks.or_equal, ks.offset, false)
	out_key = Array(Ptr{Uint8}, 1)
	out_key_length = Cint[0]
	block_until_ready(f)
	@check_error ccall( (:fdb_future_get_key, fdb_lib_name), Int32, (Ptr{Void}, Ptr{Ptr{Uint8}}, Ptr{Cint}), f, out_key, out_key_length)
	ans = bytestring(pointer_to_array(out_key[1], int64(out_key_length[1]), false))
	destroy(f)
	ans
end

function set(tr::Transaction, key::Key, value::Value)
    ccall( (:fdb_transaction_set, fdb_lib_name), Void, (Ptr{Void}, Ptr{Uint8}, Cint, Ptr{Uint8}, Cint), tr, bytestring(key), length(key), bytestring(value), length(value))
end

function clear(tr::Transaction, key::Key)
	ccall( (:fdb_transaction_clear, fdb_lib_name), Void, (Ptr{Void}, Ptr{Uint8}, Cint), tr, bytestring(key), length(key))
end

function clear_range(tr::Transaction, begin_key::Key, end_key::Key)
	ccall( (:fdb_transaction_clear_range, fdb_lib_name), Void, (Ptr{Void}, Ptr{Uint8}, Cint, Ptr{Uint8}, Cint), tr, bytestring(begin_key), length(begin_key), bytestring(end_key), length(end_key))
end

function clear_range_startswith(tr::Transaction, prefix::Key)
	clear_range(tr, prefix, strinc(prefix))
end

function commit(tr::Transaction)
    f = ccall( (:fdb_transaction_commit, fdb_lib_name), Ptr{Void}, (Ptr{Void},), tr)
    block_until_ready(f)
    @check_error get_error(f)
    destroy(f)
end

function reset(tr::Transaction)
	f = ccall( (:fdb_transaction_reset, fdb_lib_name), Ptr{Void}, (Ptr{Void},), tr)
	block_until_ready(f)
	@check_error get_error(f)
	destroy(f)
end

function cancel(tr::Transaction)
	f = ccall( (:fdb_transaction_cancel, fdb_lib_name), Ptr{Void}, (Ptr{Void},), tr)
	block_until_ready(f)
	@check_error get_error(f)
	destroy(f)
end

function enable_trace()
    @check_error ccall( (:fdb_network_set_option, fdb_lib_name), Int32, (Int32, Ptr{Void}, Int32), 30, C_NULL, 0)
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

function destroy_cluster(c::Cluster)
	@check_error ccall( (:fdb_cluster_destroy, fdb_lib_name), Int32, (Ptr{Void},), c)
end

function destroy_database(d::Database)
	@check_error ccall( (:fdb_database_destroy, fdb_lib_name), Int32, (Ptr{Void},), d)
end

@struct immutable type FDBKeyValueArray_C
	key::Ptr{Uint8}
	key_length::Int32
	value::Ptr{Uint8}
	value_length::Int32	
end align_packmax(4)

function _get_range(tr::Transaction, begin_ks::KeySelector, end_ks::KeySelector, limit::Int, reverse::Bool)
	#omg
	mode = limit > 0 ? 0 : -2
	f = ccall( (:fdb_transaction_get_range, fdb_lib_name), Ptr{Void}, 
	(Ptr{Void}, Ptr{Uint8}, Cint, Bool, Cint, Ptr{Uint8}, Cint, Bool, Cint, Cint, Cint, Cint, Cint, Bool, Bool),
	tr, begin_ks.reference, length(begin_ks.reference), begin_ks.or_equal, begin_ks.offset, 
	end_ks.reference, length(end_ks.reference), end_ks.or_equal, end_ks.offset, limit, 0, mode, 0, false, reverse)
	
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
		push!(ret, KeyValue(bytestring(pointer_to_array(kv.key, int64(kv.key_length))), bytestring(pointer_to_array(kv.value, int64(kv.value_length)))))
	end
	
	destroy(f)
	return ret
	
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

end