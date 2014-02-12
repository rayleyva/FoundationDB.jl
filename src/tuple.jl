module tuple

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
	b"\x00"
end

function _encode(v::ASCIIString)
	
end

function _encode(v::Array{Uint8})
	
end

function _encode(v::UTF8String)
	
end

function _encode(v::Int)
	
end

end