"""
    loadzone_parser!(sys::System, data::PowerSystemTableData)

Add branches to the System from the raw data.

"""
function loadzone_parser!(sys::System, data::PowerSystemTableData)
    buses = get_dataframe(data, InputCategory.BUS)
    zone_column = get_user_field(data, InputCategory.BUS, "zone")
    if !in(zone_column, names(buses))
        @warn "Missing Data : no 'zone' information for buses, cannot create loads based on zones"
        return
    end

    zones = unique(buses[!, zone_column])
    for zone in zones
        bus_numbers = Set{Int}()
        active_powers = Vector{Float64}()
        reactive_powers = Vector{Float64}()
        for bus in iterate_rows(data, InputCategory.BUS)
            if bus.zone == zone
                bus_number = bus.bus_id
                push!(bus_numbers, bus_number)

                active_power = bus.max_active_power
                push!(active_powers, active_power)

                reactive_power = bus.max_reactive_power
                push!(reactive_powers, reactive_power)
            end
        end

        name = string(zone)
        load_zone = LoadZone(name, sum(active_powers), sum(reactive_powers))
        add_component!(sys, load_zone)
    end
end
