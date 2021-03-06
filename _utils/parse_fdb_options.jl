using LightXML  

options_location = ARGS[1]
output_location = ARGS[2]

warning = """# DO NOT EDIT THIS FILE BY HAND. This file was generated using
# parse_fdb_options.jl, part of the FoundationDB.jl repository, and a copy of the
# fdb.options file (installed as part of the FoundationDB client, typically
# found as /usr/include/foundationdb/fdb.options).

# To regenerate this file, from the top level of a FoundationDB.jl repository checkout,
# run:
# \$ julia _utils/parse_fdb_options.jl /usr/include/foundationdb/fdb.options src/generated.jl

"""

out_file = open(output_location, "w+")

write(out_file, warning)

xdoc = parse_file(ascii(options_location))
xroot = root(xdoc)

scopes = get_elements_by_tagname(xroot, "Scope")

for scope in scopes
    name = attribute(scope, "name")
    if endswith(name, "Option") || name == "MutationType"
        write(out_file, "global $name = [")
        options = get_elements_by_tagname(scope, "Option")
        for option in options
            oname = attribute(option, "name")
            code = attribute(option, "code")
            desc = attribute(option, "description")
            paramType = attribute(option, "paramType")
            paramType = paramType == "Bytes" ? "Array{Uint8}" : paramType
            paramDesc = attribute(option, "paramDescription")
            if desc != "Deprecated"
                write(out_file, """\n"$oname" => ($code, "$desc", $paramType, "$paramDesc")""")
                if option != options[end]
                    write(out_file, ",")
                end
            end
        end
        write(out_file, "]\n\n")
    else
        write(out_file, "global $name = [")
        options = get_elements_by_tagname(scope, "Option")
        for option in options
            oname = attribute(option, "name")
            code = attribute(option, "code")
            desc = attribute(option, "description")
            if desc != "Deprecated"
                write(out_file, """\n"$oname" => ($code, "$desc")""")
                if option != options[end]
                    write(out_file, ",")
                end
            end
        end
        write(out_file, "]\n\n")
    end
end

close(out_file)
