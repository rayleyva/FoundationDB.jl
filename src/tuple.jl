export 	pack,
		unpack,
		range

function pack(t)
	
end

function unpack(key)
	
end

function range(t)
	
end

function _find_terminator(v, position::Int)
	
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
	return ([0x01], map((x) -> x == 0x00 ? [0x00,0xff] : [x], v)[:],  [0x00])
end

function _encode(v::UTF8String)
	
end

function _encode(v::Int)
	
end
