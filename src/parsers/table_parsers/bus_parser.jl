"""
Add buses and areas to the System from the raw data.

"""
function bus_parser!(sys::System, data::PowerSystemTableData)
    for bus in iterate_rows(data, InputCategory.BUS)
        name = bus.name
        bus_type =
            isnothing(bus.bus_type) ? nothing : get_enum_value(BusTypes, bus.bus_type)
        voltage_limits = make_minmaxlimits(bus.voltage_limits_min, bus.voltage_limits_max)

        area_name = string(get(bus, :area, "area"))
        area = get_component(Area, sys, area_name)
        if isnothing(area)
            area = Area(area_name)
            add_component!(sys, area)
        end
        zone = get(bus, :zone, nothing)

        ps_bus = Bus(;
            number = bus.bus_id,
            name = name,
            bustype = bus_type,
            angle = bus.angle,
            magnitude = bus.voltage,
            voltage_limits = voltage_limits,
            base_voltage = bus.base_voltage,
            area = area,
            load_zone = get_component(LoadZone, sys, string(zone)),
        )
        add_component!(sys, ps_bus)

        # add load if the following info is nonzero
        if (bus.max_active_power != 0.0) || (bus.max_reactive_power != 0.0)
            load = PowerLoad(
                name = name,
                available = true,
                bus = ps_bus,
                model = LoadModels.ConstantPower,
                active_power = bus.active_power,
                reactive_power = bus.reactive_power,
                base_power = bus.base_power,
                max_active_power = bus.max_active_power,
                max_reactive_power = bus.max_reactive_power,
            )
            add_component!(sys, load)
        end
    end
end
