using FoundationDB

api_version(200)

function get_instructions(db::Database, prefix::Key)
    return get_range(db, range(prefix)...)
end

if length(ARGS) == 1
    db = open()
else
    db = open(ARGS[2])
end

get_instructions(db, ARGS[1])
