
const POWER_SYSTEM_DESCRIPTOR_FILE =
    joinpath(dirname(pathof(PowerSystems)), "descriptors", "power_system_inputs.json")

const INPUT_CATEGORY_NAMES = [
    ("branch", InputCategory.BRANCH),
    ("bus", InputCategory.BUS),
    ("dc_branch", InputCategory.DC_BRANCH),
    ("gen", InputCategory.GENERATOR),
    ("load", InputCategory.LOAD),
    ("reserves", InputCategory.RESERVE),
    ("storage", InputCategory.STORAGE),
]
struct PowerSystemTableData
    base_power::Float64
    category_to_df::Dict{InputCategory, DataFrames.DataFrame}
    timeseries_metadata_file::Union{String, Nothing}
    directory::String
    user_descriptors::Dict
    descriptors::Dict
    generator_mapping::Dict{NamedTuple, DataType}
end

function complete_extension(filename::AbstractString)
    if isfile(filename)
        return filename
    elseif isfile(filename * ".json")
        return filename * ".json"
    elseif isfile(filename * ".csv")
        return filename * ".csv"
    end
end

function PowerSystemTableData(
    data::Dict{String, Any},
    directory::String,
    user_descriptors::Union{String, Dict},
    descriptors::Union{String, Dict},
    generator_mapping::Union{String, Dict};
    timeseries_metadata_file = joinpath(directory, "timeseries_pointers"),
)
    category_to_df = Dict{InputCategory, DataFrames.DataFrame}()

    if !haskey(data, "bus")
        throw(DataFormatError("key 'bus' not found in input data"))
    end

    if !haskey(data, "base_power")
        @warn "key 'base_power' not found in input data; using default=$(DEFAULT_BASE_MVA)"
    end
    base_power = get(data, "base_power", DEFAULT_BASE_MVA)

    for (name, category) in INPUT_CATEGORY_NAMES
        val = get(data, name, nothing)
        if isnothing(val)
            @debug "key '$name' not found in input data, set to nothing" _group =
                IS.LOG_GROUP_PARSING
        else
            category_to_df[category] = val
        end
    end

    timeseries_metadata_file = complete_extension(timeseries_metadata_file)

    if user_descriptors isa AbstractString
        user_descriptors = _read_config_file(user_descriptors)
    end

    if descriptors isa AbstractString
        descriptors = _read_config_file(descriptors)
    end

    if generator_mapping isa AbstractString
        generator_mapping = get_generator_mapping(generator_mapping)
    end

    return PowerSystemTableData(
        base_power,
        category_to_df,
        timeseries_metadata_file,
        directory,
        user_descriptors,
        descriptors,
        generator_mapping,
    )
end

function readtablefromfile(::Val{T}, file) where T <: Any
    return nothing
end

function readtablefromfile(::Val{Symbol(".csv")}, file)
    return DataFrames.DataFrame(CSV.File(file))
end

function gettabledata_with_dir(filepath::AbstractString)
    data = Dict{String, Any}()
    @info "Parsing file(s) at $filepath ..."
    try
        if isdir(filepath)
            subdata = merge(
                (gettabledata_with_dir(file) for file in readdir(filepath, join=true))...
            )
            if length(subdata) != 0
                data[filepath] = subdata
            end
        else
            name, extension = splitext(filepath)
            table = readtablefromfile(Val(Symbol(extension)), filepath)
            if table !== nothing
                data[splitdir(name)[2]] = table
            end
        end
    catch ex
        @error "Error occurred while parsing $filepath" exception = ex
        throw(ex)
    end
    @info "Successfully parsed $filepath"
    return data
end

function gettabledata(directory::AbstractString)
    data = gettabledata_with_dir(directory)
    if length(data) == 0
        error("No data found in $directory")
    end

    return data[directory]
end

"""
Reads in all the data stored in csv files
The general format for data is
    folder:
        gen.csv
        branch.csv
        bus.csv
        ..
        load.csv

# Arguments
- `directory::AbstractString`: directory containing CSV files
- `base_power::Float64`: base power for System
- `user_descriptor_file::AbstractString`: customized input descriptor file
- `descriptor_file=POWER_SYSTEM_DESCRIPTOR_FILE`: PowerSystems descriptor file
- `generator_mapping_file=GENERATOR_MAPPING_FILE`: generator mapping configuration file
"""
function PowerSystemTableData(
    directory::AbstractString,
    base_power::Float64,
    user_descriptor_file::AbstractString;
    descriptor_file = POWER_SYSTEM_DESCRIPTOR_FILE,
    generator_mapping_file = GENERATOR_MAPPING_FILE,
    timeseries_metadata_file = "timeseries_pointers",
)
    data = gettabledata(directory)
    data["base_power"] = base_power

    return PowerSystemTableData(
        data,
        directory,
        user_descriptor_file,
        descriptor_file,
        generator_mapping_file,
        timeseries_metadata_file = timeseries_metadata_file,
    )
end

"""
Return the custom name stored in the user descriptor file.

Throws DataFormatError if a required value is not found in the file.
"""
function get_user_field(
    data::PowerSystemTableData,
    category::InputCategory,
    field::AbstractString,
)
    if !haskey(data.user_descriptors, category)
        throw(DataFormatError("Invalid category=$category"))
    end

    try
        for item in data.user_descriptors[category]
            if item["name"] == field
                return item["custom_name"]
            end
        end
    catch
        (err)
        if err == KeyError
            msg = "Failed to find category=$category field=$field in input descriptors $err"
            throw(DataFormatError(msg))
        else
            throw(err)
        end
    end
end

"""Return a vector of user-defined fields for the category."""
function get_user_fields(data::PowerSystemTableData, category::InputCategory)
    if !haskey(data.user_descriptors, category)
        throw(DataFormatError("Invalid category=$category"))
    end

    return [x["name"] for x in data.user_descriptors[category]]
end

"""Return the dataframe for the category."""
function get_dataframe(data::PowerSystemTableData, category::InputCategory)
    df = get(data.category_to_df, category, DataFrames.DataFrame())
    isempty(df) && @warn("Missing $category data.")
    return df
end

"""
Return a NamedTuple of parameters from the descriptor file for each row of a dataframe,
making type conversions as necessary.

Refer to the PowerSystems descriptor file for field names that will be created.
"""
function iterate_rows(data::PowerSystemTableData, category; na_to_nothing = true)
    df = get_dataframe(data, category)
    field_infos = _get_field_infos(data, category, names(df))
    Channel() do channel
        for row in eachrow(df)
            obj = _read_data_row(data, row, field_infos; na_to_nothing = na_to_nothing)
            put!(channel, obj)
        end
    end
end
include("table_parsers/loadzone_parser.jl")
include("table_parsers/bus_parser.jl")
include("table_parsers/branch_parser.jl")
include("table_parsers/dc_branch_parser.jl")
include("table_parsers/gen_parser.jl")
include("table_parsers/load_parser.jl")
include("table_parsers/services_parser.jl")

"""
Construct a System from PowerSystemTableData data.

# Arguments
- `time_series_resolution::Union{DateTime, Nothing}=nothing`: only store time_series that match
  this resolution.
- `time_series_in_memory::Bool=false`: Store time series data in memory instead of HDF5 file
- `time_series_directory=nothing`: Store time series data in directory instead of tmpfs
- `runchecks::Bool=true`: Validate struct fields.

Throws DataFormatError if time_series with multiple resolutions are detected.
- A time_series has a different resolution than others.
- A time_series has a different horizon than others.

"""
function System(
    data::PowerSystemTableData;
    time_series_resolution = nothing,
    time_series_in_memory = false,
    time_series_directory = nothing,
    runchecks = true,
    kwargs...,
)
    sys = System(
        data.base_power;
        time_series_in_memory = time_series_in_memory,
        time_series_directory = time_series_directory,
        runchecks = runchecks,
        kwargs...,
    )
    set_units_base_system!(sys, IS.UnitSystem.DEVICE_BASE)

    loadzone_parser!(sys, data)
    bus_parser!(sys, data)

    # Services and time_series must be last.
    parsers = (
        (get_dataframe(data, InputCategory.BRANCH), branch_parser!),
        (get_dataframe(data, InputCategory.DC_BRANCH), dc_branch_parser!),
        (get_dataframe(data, InputCategory.GENERATOR), gen_parser!),
        (get_dataframe(data, InputCategory.LOAD), load_parser!),
        (get_dataframe(data, InputCategory.RESERVE), services_parser!),
    )

    for (val, parser) in parsers
        if !isnothing(val)
            parser(sys, data)
        end
    end

    timeseries_metadata_file =
        get(kwargs, :timeseries_metadata_file, getfield(data, :timeseries_metadata_file))

    if !isnothing(timeseries_metadata_file)
        add_time_series!(sys, timeseries_metadata_file; resolution = time_series_resolution)
    end

    check(sys)
    return sys
end

function _read_config_file(file_path::String)
    return open(file_path) do io
        data = YAML.load(io)
        # Replace keys with enums.
        config_data = Dict{InputCategory, Vector}()
        for (key, val) in data
            # TODO: need to change user_descriptors.yaml to use reserve instead.
            if key == "reserves"
                key = "reserve"
            end
            config_data[get_enum_value(InputCategory, key)] = val
        end
        return config_data
    end
end

"""Stores user-customized information for required dataframe columns."""
struct _FieldInfo
    name::String
    custom_name::String
    per_unit_conversion::NamedTuple{
        (:From, :To, :Reference),
        Tuple{UnitSystem, UnitSystem, String},
    }
    unit_conversion::Union{NamedTuple{(:From, :To), Tuple{String, String}}, Nothing}
    default_value::Any
    # TODO unit, value ranges and options
end

function _get_field_infos(data::PowerSystemTableData, category::InputCategory, df_names)
    if !haskey(data.user_descriptors, category)
        throw(DataFormatError("Invalid category=$category"))
    end

    if !haskey(data.descriptors, category)
        throw(DataFormatError("Invalid category=$category"))
    end

    # Cache whether PowerSystems uses a column's values as system-per-unit.
    # The user's descriptors indicate that the raw data is already system-per-unit or not.
    per_unit = Dict{String, IS.UnitSystem}()
    unit = Dict{String, Union{String, Nothing}}()
    custom_names = Dict{String, String}()
    for descriptor in data.user_descriptors[category]
        custom_name = descriptor["custom_name"]
        if descriptor["custom_name"] in df_names
            per_unit[descriptor["name"]] = get_enum_value(
                IS.UnitSystem,
                get(descriptor, "unit_system", "NATURAL_UNITS"),
            )
            unit[descriptor["name"]] = get(descriptor, "unit", nothing)
            custom_names[descriptor["name"]] = custom_name
        else
            @warn "User-defined column name $custom_name is not in dataframe."
        end
    end

    fields = Vector{_FieldInfo}()

    for item in data.descriptors[category]
        name = item["name"]
        item_unit_system =
            get_enum_value(IS.UnitSystem, get(item, "unit_system", "NATURAL_UNITS"))
        per_unit_reference = get(item, "base_reference", "base_power")
        default_value = get(item, "default_value", "required")
        if default_value == "system_base_power"
            default_value = data.base_power
        end

        if name in keys(custom_names)
            custom_name = custom_names[name]

            if item_unit_system == IS.UnitSystem.NATURAL_UNITS &&
               per_unit[name] != IS.UnitSystem.NATURAL_UNITS
                throw(DataFormatError("$name cannot be defined as $(per_unit[name])"))
            end

            pu_conversion = (
                From = per_unit[name],
                To = item_unit_system,
                Reference = per_unit_reference,
            )

            expected_unit = get(item, "unit", nothing)
            if !isnothing(expected_unit) &&
               !isnothing(unit[name]) &&
               expected_unit != unit[name]
                unit_conversion = (From = unit[name], To = expected_unit)
            else
                unit_conversion = nothing
            end
        else
            custom_name = name
            pu_conversion = (
                From = item_unit_system,
                To = item_unit_system,
                Reference = per_unit_reference,
            )
            unit_conversion = nothing
        end

        push!(
            fields,
            _FieldInfo(name, custom_name, pu_conversion, unit_conversion, default_value),
        )
    end

    return fields
end

"""Reads values from dataframe row and performs necessary conversions."""
function _read_data_row(data::PowerSystemTableData, row, field_infos; na_to_nothing = true)
    fields = Vector{String}()
    vals = Vector()
    for field_info in field_infos
        if field_info.custom_name in names(row)
            value = row[field_info.custom_name]
        else
            value = field_info.default_value
            value == "required" && throw(DataFormatError("$(field_info.name) is required"))
            @debug "Column $(field_info.custom_name) doesn't exist in df, enabling use of default value of $(field_info.default_value)" _group =
                IS.LOG_GROUP_PARSING maxlog = 1
        end
        if ismissing(value)
            throw(DataFormatError("$(field_info.custom_name) value missing"))
        end
        if na_to_nothing && value == "NA"
            value = nothing
        end

        if !isnothing(value)
            if field_info.per_unit_conversion.From == IS.UnitSystem.NATURAL_UNITS &&
               field_info.per_unit_conversion.To == IS.UnitSystem.SYSTEM_BASE
                @debug "convert to $(field_info.per_unit_conversion.To)" _group =
                    IS.LOG_GROUP_PARSING field_info.custom_name
                value = value isa AbstractString ? tryparse(Float64, value) : value
                value = data.base_power == 0.0 ? 0.0 : value / data.base_power
            elseif field_info.per_unit_conversion.From == IS.UnitSystem.NATURAL_UNITS &&
                   field_info.per_unit_conversion.To == IS.UnitSystem.DEVICE_BASE
                reference_idx = findfirst(
                    x -> x.name == field_info.per_unit_conversion.Reference,
                    field_infos,
                )
                isnothing(reference_idx) && throw(
                    DataFormatError(
                        "$(field_info.per_unit_conversion.Reference) not found in table with $(field_info.custom_name)",
                    ),
                )
                reference_info = field_infos[reference_idx]
                @debug "convert to $(field_info.per_unit_conversion.To) using $(reference_info.custom_name)" _group =
                    IS.LOG_GROUP_PARSING field_info.custom_name maxlog = 1
                reference_value =
                    get(row, reference_info.custom_name, reference_info.default_value)
                reference_value == "required" && throw(
                    DataFormatError(
                        "$(reference_info.name) is required for p.u. conversion",
                    ),
                )
                value = value isa AbstractString ? tryparse(Float64, value) : value
                value = reference_value == 0.0 ? 0.0 : value / reference_value
            elseif field_info.per_unit_conversion.From != field_info.per_unit_conversion.To
                throw(
                    DataFormatError(
                        "conversion not supported from $(field_info.per_unit_conversion.From) to $(field_info.per_unit_conversion.To) for $(field_info.custom_name)",
                    ),
                )
            end
        else
            @debug "$(field_info.custom_name) is nothing" _group = IS.LOG_GROUP_PARSING maxlog =
                1
        end

        # TODO: need special handling for units
        if !isnothing(field_info.unit_conversion)
            @debug "convert units" _group = IS.LOG_GROUP_PARSING field_info.custom_name maxlog =
                1
            value = convert_units!(value, field_info.unit_conversion)
        end
        # TODO: validate ranges and option lists
        push!(fields, field_info.name)
        push!(vals, value)
    end
    return NamedTuple{Tuple(Symbol.(fields))}(vals)
end
