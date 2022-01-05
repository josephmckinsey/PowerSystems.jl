"""
Add services to the System from the raw data.
"""
function services_parser!(sys::System, data::PowerSystemTableData)
    bus_id_column = get_user_field(data, InputCategory.BUS, "bus_id")
    bus_area_column = get_user_field(data, InputCategory.BUS, "area")

    # Shortcut for data that looks like "(val1,val2,val3)"
    make_array(x) = isnothing(x) ? x : split(strip(x, ['(', ')']), ",")

    function _add_device!(contributing_devices, device_categories, name)
        component = []
        for dev_category in device_categories
            component_type = _get_component_type_from_category(dev_category)
            components = get_components_by_name(component_type, sys, name)
            if length(components) == 0
                # There multiple categories, so we might not find a match in some.
                continue
            elseif length(components) == 1
                push!(component, components[1])
            else
                msg = "Found duplicate names type=$component_type name=$name"
                throw(DataFormatError(msg))
            end
        end
        if length(component) > 1
            msg = "Found duplicate components with name=$name"
            throw(DataFormatError(msg))
        elseif length(component) == 1
            push!(contributing_devices, component[1])
        end
    end

    for reserve in iterate_rows(data, InputCategory.RESERVE)
        device_categories = make_array(reserve.eligible_device_categories)
        device_subcategories =
            make_array(get(reserve, :eligible_device_subcategories, nothing))
        devices = make_array(get(reserve, :contributing_devices, nothing))
        regions = make_array(reserve.eligible_regions) #TODO: rename to "area"
        requirement = get(reserve, :requirement, nothing)
        contributing_devices = Vector{Device}()

        if isnothing(device_subcategories)
            @info("Adding contributing components for $(reserve.name) by component name")
            for device in devices
                _add_device!(contributing_devices, device_categories, device)
            end
        else
            @info("Adding contributing generators for $(reserve.name) by category")
            for gen in iterate_rows(data, InputCategory.GENERATOR)
                buses = get_dataframe(data, InputCategory.BUS)
                bus_ids = buses[!, bus_id_column]
                gen_type =
                    get_generator_type(gen.fuel, gen.unit_type, data.generator_mapping)
                sys_gen = get_component(
                    get_generator_type(gen.fuel, gen.unit_type, data.generator_mapping),
                    sys,
                    gen.name,
                )
                area = string(
                    buses[bus_ids .== get_number(get_bus(sys_gen)), bus_area_column][1],
                )
                if gen.category in device_subcategories && area in regions
                    _add_device!(contributing_devices, device_categories, gen.name)
                end
            end

            unused_categories = setdiff(
                device_subcategories,
                get_dataframe(data, InputCategory.GENERATOR)[
                    !,
                    get_user_field(data, InputCategory.GENERATOR, "category"),
                ],
            )
            for cat in unused_categories
                @warn(
                    "Device category: $cat not found in generators data; adding contributing devices by category only supported for generator data"
                )
            end
        end

        if length(contributing_devices) == 0
            throw(
                DataFormatError(
                    "did not find contributing devices for service $(reserve.name)",
                ),
            )
        end

        direction = get_reserve_direction(reserve.direction)
        if isnothing(requirement)
            service = StaticReserve{direction}(reserve.name, true, reserve.timeframe, 0.0)
        else
            service = VariableReserve{direction}(
                reserve.name,
                true,
                reserve.timeframe,
                requirement,
            )
        end

        add_service!(sys, service, contributing_devices)
    end
end

function get_reserve_direction(direction::AbstractString)
    if direction == "Up"
        return ReserveUp
    elseif direction == "Down"
        return ReserveDown
    else
        throw(DataFormatError("invalid reserve direction $direction"))
    end
end

const CATEGORY_STR_TO_COMPONENT = Dict{String, DataType}(
    "Bus" => Bus,
    "Generator" => Generator,
    "Reserve" => Service,
    "LoadZone" => LoadZone,
    "ElectricLoad" => ElectricLoad,
)

function _get_component_type_from_category(category::AbstractString)
    component_type = get(CATEGORY_STR_TO_COMPONENT, category, nothing)
    if isnothing(component_type)
        throw(DataFormatError("unsupported category=$category"))
    end

    return component_type
end
