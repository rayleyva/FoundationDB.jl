using FoundationDB

api_version(200)

function get_instructions(db::Database, prefix::Key)
    return get_range(db, range(prefix)...)
end
