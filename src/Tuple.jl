export 	pack,
		unpack,
		range

function pack(t)
    return _encode(t)
end

function unpack(key)
	
end

function range(t)
	
end

function _flat(A)
    return mapreduce(x->isa(x,Array)? _flat(x): x, vcat, A)
end

function _find_terminator(v, position::Int)
	
end

function _bisect_left(v, item)

end

function _decode(v, position::Int)
	
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
	
end
