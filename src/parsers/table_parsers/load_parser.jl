"""
    load_parser!(sys::System, data::PowerSystemTableData)

Add loads to the System from the raw load data.

"""
function load_parser!(sys::System, data::PowerSystemTableData)
    for rawload in iterate_rows(data, InputCategory.LOAD)
        bus = get_bus(sys, rawload.bus_id)
        if isnothing(bus)
            throw(
                DataFormatError(
                    "could not find bus_number=$(rawload.bus_id) for load=$(rawload.name)",
                ),
            )
        end

        load = PowerLoad(
            name = rawload.name,
            available = rawload.available,
            bus = bus,
            model = LoadModels.ConstantPower,
            active_power = rawload.active_power,
            reactive_power = rawload.reactive_power,
            max_active_power = rawload.max_active_power,
            max_reactive_power = rawload.max_reactive_power,
            base_power = rawload.base_power,
        )
        add_component!(sys, load)
    end
end
