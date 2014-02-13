export 	pack,
		unpack,
		range

function pack(t...)
    _flat([_encode(x) for x in t])
end

function unpack(v::Array{Uint8})
	pos = 1
    res = Any[]
    while pos <= length(v)
        r, pos = _decode(v, pos)
        push!(res, r)
    end
    return tuple(res)
end

#Returns a range of keyspace containing all tuples having this one as a prefix
function range(t...)
	p = pack(t...)
    return ([p,[0x00]], [p,[0xff]])
end

function _flat(A)
    return mapreduce(x->isa(x,Array)? _flat(x): x, vcat, A)
end

function _find_terminator(v, position::Int)
	while true
        if position > length(v)
            return position
        elseif v[position] == 0x00
            if position == length(v) || v[position+1] != 0xff
                return position
            else
                position += 2
            end
        else
            position += 1
        end
    end
end

function _bisect_left(v, item)
    count = 0
    for i in v
        if i >= item
            return count
        else
            count += 1
        end
    end
end

const _size_limits = [<<(BigInt(1),(i*8))-1 for i in 0:8]

function _decode(v::Array{Uint8}, position::Int)
	code = int(v[position])
    if code == 0
        return nothing, position+1
    elseif code == 1
        done::Int = _find_terminator(v, position+1)
        ret = Uint8[]
        for i = position+1:done-1
            if v[i] != 0xff || v[i-1] != 0x00
                push!(ret, v[i])
            end
        end
        if is_valid_ascii(ret)
            return ascii(ret), done+1
        else
            return ret, done+1
        end
    elseif code == 2
        done = _find_terminator(v, position+1)
        ret = Uint8[]
        for i = position+1:done-1
            if v[i] != 0xff || v[i-1] != 0x00
                push!(ret, v[i])
            end
        end
        @assert(is_valid_utf8(ret))
        return utf8(ret), done+1
    elseif code >= 20 && code <= 28
        n = code - 20
        done = position + n
        return parseint(bytes2hex(v[position+1:done]),16), done+1
    elseif code < 20 && code >= 12
        n = 20 - code
        done = position + n
		return parseint(bytes2hex(v[position+1:done]),16)-_size_limits[n+1], done+1
    else
        throw(FDBException("Unrecognized tuple type."))
    end
end

function _encode(v::Nothing)
	0x00
end

function _encode(v::ASCIIString)
	return _encode([convert(Uint8, c) for c in v])
end
   
function _encode(v::Array{Uint8})
	return _flat([0x01,[x == 0x00 ? [0x00,0xff] : [x] for x in v], 0x00])
end
   
function _encode(v::UTF8String)
	return _flat([0x02, [x == 0x00 ? [0x00,0xff] : [x] for x in convert(Array{Uint8}, v)], 0x00])
end

function _encode(v::Int)
    if v == 0
        return 0x14
    elseif v > 0
        n = _bisect_left(_size_limits, v)
        first = convert(Uint8, char(20+n))
        rest = hex2bytes(hex(v,16))[9-n:end]
        return [first, rest]
    else
        n = _bisect_left(_size_limits, -v)
        first = convert(Uint8, char(20-n))
        rest = hex2bytes(hex(uint64(v+_size_limits[n+1]),16))[9-n:end]
        return [first, rest]
    end
end
