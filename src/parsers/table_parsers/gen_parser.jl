"""
Add generators to the System from the raw data.

"""
struct _HeatRateColumns
    columns::Base.Iterators.Zip{Tuple{Array{Symbol, 1}, Array{Symbol, 1}}}
end
struct _CostPointColumns
    columns::Base.Iterators.Zip{Tuple{Array{Symbol, 1}, Array{Symbol, 1}}}
end

function gen_parser!(sys::System, data::PowerSystemTableData)
    output_point_fields = Vector{Symbol}()
    heat_rate_fields = Vector{Symbol}()
    cost_point_fields = Vector{Symbol}()
    fields = get_user_fields(data, InputCategory.GENERATOR)
    for field in fields
        if occursin("output_point", field)
            push!(output_point_fields, Symbol(field))
        elseif occursin("heat_rate_", field)
            push!(heat_rate_fields, Symbol(field))
        elseif occursin("cost_point_", field)
            push!(cost_point_fields, Symbol(field))
        end
    end

    @assert length(output_point_fields) > 0
    if length(heat_rate_fields) > 0 && length(cost_point_fields) > 0
        throw(IS.ConflictingInputsError("Heat rate and cost points are both defined"))
    elseif length(heat_rate_fields) > 0
        cost_colnames = _HeatRateColumns(zip(heat_rate_fields, output_point_fields))
    elseif length(cost_point_fields) > 0
        cost_colnames = _CostPointColumns(zip(cost_point_fields, output_point_fields))
    end

    for gen in iterate_rows(data, InputCategory.GENERATOR)
        @debug "making generator:" _group = IS.LOG_GROUP_PARSING gen.name
        bus = get_bus(sys, gen.bus_id)
        if isnothing(bus)
            throw(DataFormatError("could not find $(gen.bus_id)"))
        end

        generator = make_generator(data, gen, cost_colnames, bus)
        @debug "adding gen:" _group = IS.LOG_GROUP_PARSING generator
        if !isnothing(generator)
            add_component!(sys, generator)
        end
    end
end

"""Creates a generator of any type."""
function make_generator(data::PowerSystemTableData, gen, cost_colnames, bus)
    generator = nothing
    gen_type =
        get_generator_type(gen.fuel, get(gen, :unit_type, nothing), data.generator_mapping)

    if isnothing(gen_type)
        @error "Cannot recognize generator type" gen.name
    elseif gen_type == ThermalStandard
        generator = make_thermal_generator(data, gen, cost_colnames, bus)
    elseif gen_type == ThermalMultiStart
        generator = make_thermal_generator_multistart(data, gen, cost_colnames, bus)
    elseif gen_type <: HydroGen
        generator = make_hydro_generator(gen_type, data, gen, cost_colnames, bus)
    elseif gen_type <: RenewableGen
        generator = make_renewable_generator(gen_type, data, gen, cost_colnames, bus)
    elseif gen_type == GenericBattery
        storage = get_storage_by_generator(data, gen.name).head
        generator = make_storage(data, gen, storage, bus)
    else
        @error "Skipping unsupported generator" gen.name gen_type
    end

    return generator
end

function calculate_variable_cost(
    data::PowerSystemTableData,
    gen,
    cost_colnames::_HeatRateColumns,
    base_power,
)
    fuel_cost = gen.fuel_price / 1000.0

    vom = isnothing(gen.variable_cost) ? 0.0 : gen.variable_cost

    if fuel_cost > 0.0
        var_cost =
            [(getfield(gen, hr), getfield(gen, mw)) for (hr, mw) in cost_colnames.columns]
        var_cost = unique([
            (tryparse(Float64, string(c[1])), tryparse(Float64, string(c[2]))) for
            c in var_cost if !in(nothing, c)
        ])
        if isempty(var_cost)
            @warn "Unable to calculate variable cost for $(gen.name)" var_cost maxlog = 5
        end
    else
        var_cost = [(0.0, 0.0)]
    end

    if length(var_cost) > 1
        var_cost[2:end] = [
            (
                (
                    var_cost[i][1] * fuel_cost * (var_cost[i][2] - var_cost[i - 1][2]) +
                    var_cost[i][2] * vom
                ),
                var_cost[i][2],
            ) .* gen.active_power_limits_max .* base_power for i in 2:length(var_cost)
        ]
        var_cost[1] =
            ((var_cost[1][1] * fuel_cost + vom) * var_cost[1][2], var_cost[1][2]) .*
            gen.active_power_limits_max .* base_power

        fixed = max(
            0.0,
            var_cost[1][1] -
            (var_cost[2][1] / (var_cost[2][2] - var_cost[1][2]) * var_cost[1][2]),
        )
        var_cost[1] = (var_cost[1][1] - fixed, var_cost[1][2])

        for i in 2:length(var_cost)
            var_cost[i] = (var_cost[i - 1][1] + var_cost[i][1], var_cost[i][2])
        end

    elseif length(var_cost) == 1
        # if there is only one point, use it to determine the constant $/MW cost
        var_cost = var_cost[1][1] * fuel_cost + vom
        fixed = 0.0
    end
    return var_cost, fixed, fuel_cost
end

function calculate_variable_cost(
    data::PowerSystemTableData,
    gen,
    cost_colnames::_CostPointColumns,
    base_power,
)
    vom = isnothing(gen.variable_cost) ? 0.0 : gen.variable_cost

    var_cost = [(getfield(gen, c), getfield(gen, mw)) for (c, mw) in cost_colnames.columns]
    var_cost = unique([
        (tryparse(Float64, string(c[1])), tryparse(Float64, string(c[2]))) for
        c in var_cost if !in(nothing, c)
    ])

    var_cost = [
        ((var_cost[i][1] + vom) * var_cost[i][2], var_cost[i][2]) .*
        gen.active_power_limits_max .* base_power for i in 1:length(var_cost)
    ]

    if length(var_cost) > 1
        fixed = max(
            0.0,
            var_cost[1][1] -
            (var_cost[2][1] / (var_cost[2][2] - var_cost[1][2]) * var_cost[1][2]),
        )
        var_cost = [(var_cost[i][1] - fixed, var_cost[i][2]) for i in 1:length(var_cost)]
    elseif length(var_cost) == 1
        var_cost = var_cost[1][1] + vom
        fixed = 0.0
    end

    return var_cost, fixed, 0.0
end

function calculate_uc_cost(data, gen, fuel_cost)
    startup_cost = gen.startup_cost
    if isnothing(startup_cost)
        if !isnothing(gen.startup_heat_cold_cost)
            startup_cost = gen.startup_heat_cold_cost * fuel_cost * 1000
        else
            startup_cost = 0.0
            @warn "No startup_cost defined for $(gen.name), setting to $startup_cost" maxlog =
                5
        end
    end

    shutdown_cost = get(gen, :shutdown_cost, nothing)
    if isnothing(shutdown_cost)
        @warn "No shutdown_cost defined for $(gen.name), setting to 0.0" maxlog = 1
        shutdown_cost = 0.0
    end

    return startup_cost, shutdown_cost
end

function make_minmaxlimits(min::Union{Nothing, Float64}, max::Union{Nothing, Float64})
    if isnothing(min) && isnothing(max)
        minmax = nothing
    else
        minmax = (min = min, max = max)
    end
    return minmax
end

function make_ramplimits(
    gen;
    ramplimcol = :ramp_limits,
    rampupcol = :ramp_up,
    rampdncol = :ramp_down,
)
    ramp = get(gen, ramplimcol, nothing)
    if !isnothing(ramp)
        up = ramp
        down = ramp
    else
        up = get(gen, rampupcol, ramp)
        up = typeof(up) <: AbstractString ? tryparse(Float64, up) : up
        down = get(gen, rampdncol, ramp)
        down = typeof(down) <: AbstractString ? tryparse(Float64, down) : down
    end
    ramplimits = isnothing(up) && isnothing(down) ? nothing : (up = up, down = down)
    return ramplimits
end

function make_timelimits(gen, up_column::Symbol, down_column::Symbol)
    up_time = get(gen, up_column, nothing)
    up_time = typeof(up_time) <: AbstractString ? tryparse(Float64, up_time) : up_time

    down_time = get(gen, down_column, nothing)
    down_time =
        typeof(down_time) <: AbstractString ? tryparse(Float64, down_time) : down_time

    timelimits =
        isnothing(up_time) && isnothing(down_time) ? nothing :
        (up = up_time, down = down_time)
    return timelimits
end

function make_reactive_params(
    gen;
    powerfield = :reactive_power,
    minfield = :reactive_power_limits_min,
    maxfield = :reactive_power_limits_max,
)
    reactive_power = get(gen, powerfield, 0.0)
    reactive_power_limits_min = get(gen, minfield, nothing)
    reactive_power_limits_max = get(gen, maxfield, nothing)
    if isnothing(reactive_power_limits_min) && isnothing(reactive_power_limits_max)
        reactive_power_limits = nothing
    elseif isnothing(reactive_power_limits_min)
        reactive_power_limits = (min = 0.0, max = reactive_power_limits_max)
    else
        reactive_power_limits =
            (min = reactive_power_limits_min, max = reactive_power_limits_max)
    end
    return reactive_power, reactive_power_limits
end

function make_thermal_generator(data::PowerSystemTableData, gen, cost_colnames, bus)
    @debug "Making ThermaStandard" _group = IS.LOG_GROUP_PARSING gen.name
    active_power_limits =
        (min = gen.active_power_limits_min, max = gen.active_power_limits_max)
    (reactive_power, reactive_power_limits) = make_reactive_params(gen)
    rating = calculate_rating(active_power_limits, reactive_power_limits)
    ramplimits = make_ramplimits(gen)
    timelimits = make_timelimits(gen, :min_up_time, :min_down_time)
    primemover = parse_enum_mapping(PrimeMovers, gen.unit_type)
    fuel = parse_enum_mapping(ThermalFuels, gen.fuel)

    base_power = gen.base_mva
    var_cost, fixed, fuel_cost =
        calculate_variable_cost(data, gen, cost_colnames, base_power)
    startup_cost, shutdown_cost = calculate_uc_cost(data, gen, fuel_cost)
    op_cost = ThreePartCost(var_cost, fixed, startup_cost, shutdown_cost)

    return ThermalStandard(
        name = gen.name,
        available = gen.available,
        status = gen.status_at_start,
        bus = bus,
        active_power = gen.active_power,
        reactive_power = reactive_power,
        rating = rating,
        prime_mover = primemover,
        fuel = fuel,
        active_power_limits = active_power_limits,
        reactive_power_limits = reactive_power_limits,
        ramp_limits = ramplimits,
        time_limits = timelimits,
        operation_cost = op_cost,
        base_power = base_power,
    )
end

function make_thermal_generator_multistart(
    data::PowerSystemTableData,
    gen,
    cost_colnames,
    bus,
)
    thermal_gen = make_thermal_generator(data, gen, cost_colnames, bus)

    @debug "Making ThermalMultiStart" _group = IS.LOG_GROUP_PARSING gen.name
    base_power = get_base_power(thermal_gen)
    var_cost, fixed, fuel_cost =
        calculate_variable_cost(data, gen, cost_colnames, base_power)
    if var_cost isa Float64
        no_load_cost = 0.0
        var_cost = VariableCost(var_cost)
    else
        no_load_cost = var_cost[1][1]
        var_cost =
            VariableCost([(c - no_load_cost, pp - var_cost[1][2]) for (c, pp) in var_cost])
    end
    lag_hot =
        isnothing(gen.hot_start_time) ? get_time_limits(thermal_gen).down :
        gen.hot_start_time
    lag_warm = isnothing(gen.warm_start_time) ? 0.0 : gen.warm_start_time
    lag_cold = isnothing(gen.cold_start_time) ? 0.0 : gen.cold_start_time
    startup_timelimits = (hot = lag_hot, warm = lag_warm, cold = lag_cold)
    start_types = sum(values(startup_timelimits) .> 0.0)
    startup_ramp = isnothing(gen.startup_ramp) ? 0.0 : gen.startup_ramp
    shutdown_ramp = isnothing(gen.shutdown_ramp) ? 0.0 : gen.shutdown_ramp
    power_trajectory = (startup = startup_ramp, shutdown = shutdown_ramp)
    hot_start_cost = isnothing(gen.hot_start_cost) ? gen.startup_cost : gen.hot_start_cost
    if isnothing(hot_start_cost)
        if hasfield(typeof(gen), :startup_heat_cold_cost)
            hot_start_cost = gen.startup_heat_cold_cost * fuel_cost * 1000
        else
            hot_start_cost = 0.0
            @warn "No hot_start_cost or startup_cost defined for $(gen.name), setting to $startup_cost" maxlog =
                5
        end
    end
    warm_start_cost = isnothing(gen.warm_start_cost) ? START_COST : gen.hot_start_cost #TODO
    cold_start_cost = isnothing(gen.cold_start_cost) ? START_COST : gen.cold_start_cost
    startup_cost = (hot = hot_start_cost, warm = warm_start_cost, cold = cold_start_cost)

    shutdown_cost = gen.shutdown_cost
    if isnothing(shutdown_cost)
        @warn "No shutdown_cost defined for $(gen.name), setting to 0.0" maxlog = 1
        shutdown_cost = 0.0
    end

    op_cost = MultiStartCost(var_cost, no_load_cost, fixed, startup_cost, shutdown_cost)

    return ThermalMultiStart(;
        name = get_name(thermal_gen),
        available = get_available(thermal_gen),
        status = get_status(thermal_gen),
        bus = get_bus(thermal_gen),
        active_power = get_active_power(thermal_gen),
        reactive_power = get_reactive_power(thermal_gen),
        rating = get_rating(thermal_gen),
        prime_mover = get_prime_mover(thermal_gen),
        fuel = get_fuel(thermal_gen),
        active_power_limits = get_active_power_limits(thermal_gen),
        reactive_power_limits = get_reactive_power_limits(thermal_gen),
        ramp_limits = get_ramp_limits(thermal_gen),
        power_trajectory = power_trajectory,
        time_limits = get_time_limits(thermal_gen),
        start_time_limits = startup_timelimits,
        start_types = start_types,
        operation_cost = op_cost,
        base_power = get_base_power(thermal_gen),
        time_at_status = get_time_at_status(thermal_gen),
        must_run = gen.must_run,
    )
end

function make_hydro_generator(gen_type, data::PowerSystemTableData, gen, cost_colnames, bus)
    @debug "Making HydroGen" _group = IS.LOG_GROUP_PARSING gen.name
    active_power_limits =
        (min = gen.active_power_limits_min, max = gen.active_power_limits_max)
    (reactive_power, reactive_power_limits) = make_reactive_params(gen)
    rating = calculate_rating(active_power_limits, reactive_power_limits)
    ramp_limits = make_ramplimits(gen)
    min_up_time = gen.min_up_time
    min_down_time = gen.min_down_time
    time_limits = make_timelimits(gen, :min_up_time, :min_down_time)
    base_power = gen.base_mva

    if gen_type == HydroEnergyReservoir || gen_type == HydroPumpedStorage
        if !haskey(data.category_to_df, InputCategory.STORAGE)
            throw(DataFormatError("Storage information must defined in storage data"))
        end

        storage = get_storage_by_generator(data, gen.name)

        var_cost, fixed, fuel_cost =
            calculate_variable_cost(data, gen, cost_colnames, base_power)
        operation_cost = TwoPartCost(var_cost, fixed)

        if gen_type == HydroEnergyReservoir
            @debug "Creating $(gen.name) as HydroEnergyReservoir" _group =
                IS.LOG_GROUP_PARSING

            hydro_gen = HydroEnergyReservoir(
                name = gen.name,
                available = gen.available,
                bus = bus,
                active_power = gen.active_power,
                reactive_power = reactive_power,
                prime_mover = parse_enum_mapping(PrimeMovers, gen.unit_type),
                rating = rating,
                active_power_limits = active_power_limits,
                reactive_power_limits = reactive_power_limits,
                ramp_limits = ramp_limits,
                time_limits = time_limits,
                operation_cost = operation_cost,
                base_power = base_power,
                storage_capacity = storage.head.storage_capacity,
                inflow = storage.head.input_active_power_limit_max,
                initial_storage = storage.head.energy_level,
            )

        elseif gen_type == HydroPumpedStorage
            @debug "Creating $(gen.name) as HydroPumpedStorage" _group =
                IS.LOG_GROUP_PARSING

            pump_active_power_limits = (
                min = gen.pump_active_power_limits_min,
                max = gen.pump_active_power_limits_max,
            )
            (pump_reactive_power, pump_reactive_power_limits) = make_reactive_params(
                gen,
                powerfield = :pump_reactive_power,
                minfield = :pump_reactive_power_limits_min,
                maxfield = :pump_reactive_power_limits_max,
            )
            pump_rating =
                calculate_rating(pump_active_power_limits, pump_reactive_power_limits)
            pump_ramp_limits = make_ramplimits(
                gen;
                ramplimcol = :pump_ramp_limits,
                rampupcol = :pump_ramp_up,
                rampdncol = :pump_ramp_down,
            )
            pump_time_limits = make_timelimits(gen, :pump_min_up_time, :pump_min_down_time)
            hydro_gen = HydroPumpedStorage(
                name = gen.name,
                available = gen.available,
                bus = bus,
                active_power = gen.active_power,
                reactive_power = reactive_power,
                rating = rating,
                base_power = base_power,
                prime_mover = parse_enum_mapping(PrimeMovers, gen.unit_type),
                active_power_limits = active_power_limits,
                reactive_power_limits = reactive_power_limits,
                ramp_limits = ramp_limits,
                time_limits = time_limits,
                rating_pump = pump_rating,
                active_power_limits_pump = pump_active_power_limits,
                reactive_power_limits_pump = pump_reactive_power_limits,
                ramp_limits_pump = pump_ramp_limits,
                time_limits_pump = pump_time_limits,
                storage_capacity = (
                    up = storage.head.storage_capacity,
                    down = storage.head.storage_capacity,
                ),
                inflow = storage.head.input_active_power_limit_max,
                outflow = storage.tail.input_active_power_limit_max,
                initial_storage = (
                    up = storage.head.energy_level,
                    down = storage.tail.energy_level,
                ),
                storage_target = (
                    up = storage.head.storage_target,
                    down = storage.tail.storage_target,
                ),
                operation_cost = operation_cost,
                pump_efficiency = storage.tail.efficiency,
            )
        end
    elseif gen_type == HydroDispatch
        @debug "Creating $(gen.name) as HydroDispatch" _group = IS.LOG_GROUP_PARSING
        hydro_gen = HydroDispatch(
            name = gen.name,
            available = gen.available,
            bus = bus,
            active_power = gen.active_power,
            reactive_power = reactive_power,
            rating = rating,
            prime_mover = parse_enum_mapping(PrimeMovers, gen.unit_type),
            active_power_limits = active_power_limits,
            reactive_power_limits = reactive_power_limits,
            ramp_limits = ramp_limits,
            time_limits = time_limits,
            base_power = base_power,
        )
    else
        error("Tabular data parser does not currently support $gen_type creation")
    end
    return hydro_gen
end

function get_storage_by_generator(data::PowerSystemTableData, gen_name::AbstractString)
    head = []
    tail = []
    for s in iterate_rows(data, InputCategory.STORAGE)
        if s.generator_name == gen_name
            position = get(s, :position, "head")
            if position == "tail"
                push!(tail, s)
            else
                push!(head, s)
            end
        end
    end

    if length(head) != 1
        @warn "storage generator should have exactly 1 head storage defined: this will throw an error in v1.6.x" maxlog =
            1 # this currently selects the first head storage with no control on how to make that selection, in the future throw an error.
    #throw(DataFormatError("storage generator must have exactly 1 head storage defined")) #TODO: uncomment this in next version
    elseif length(tail) > 1
        throw(
            DataFormatError(
                "storage generator cannot have more than 1 tail storage defined",
            ),
        )
    end
    tail = length(tail) > 0 ? tail[1] : nothing

    return (head = head[1], tail = tail)
end

function make_renewable_generator(
    gen_type,
    data::PowerSystemTableData,
    gen,
    cost_colnames,
    bus,
)
    @debug "Making RenewableGen" _group = IS.LOG_GROUP_PARSING gen.name
    generator = nothing
    active_power_limits =
        (min = gen.active_power_limits_min, max = gen.active_power_limits_max)
    (reactive_power, reactive_power_limits) = make_reactive_params(gen)
    rating = calculate_rating(active_power_limits, reactive_power_limits)
    base_power = gen.base_mva
    var_cost, fixed, fuel_cost =
        calculate_variable_cost(data, gen, cost_colnames, base_power)
    operation_cost = TwoPartCost(var_cost, fixed)

    if gen_type == RenewableDispatch
        @debug "Creating $(gen.name) as RenewableDispatch" _group = IS.LOG_GROUP_PARSING
        generator = RenewableDispatch(
            name = gen.name,
            available = gen.available,
            bus = bus,
            active_power = gen.active_power,
            reactive_power = reactive_power,
            rating = rating,
            prime_mover = parse_enum_mapping(PrimeMovers, gen.unit_type),
            reactive_power_limits = reactive_power_limits,
            power_factor = gen.power_factor,
            operation_cost = operation_cost,
            base_power = base_power,
        )
    elseif gen_type == RenewableFix
        @debug "Creating $(gen.name) as RenewableFix" _group = IS.LOG_GROUP_PARSING
        generator = RenewableFix(
            name = gen.name,
            available = gen.available,
            bus = bus,
            active_power = gen.active_power,
            reactive_power = reactive_power,
            rating = rating,
            prime_mover = parse_enum_mapping(PrimeMovers, gen.unit_type),
            power_factor = gen.power_factor,
            base_power = base_power,
        )
    else
        error("Unsupported type $gen_type")
    end

    return generator
end

function make_storage(data::PowerSystemTableData, gen, storage, bus)
    @debug "Making Storge" _group = IS.LOG_GROUP_PARSING storage.name
    state_of_charge_limits =
        (min = storage.min_storage_capacity, max = storage.storage_capacity)
    input_active_power_limits = (
        min = storage.input_active_power_limit_min,
        max = storage.input_active_power_limit_max,
    )
    output_active_power_limits = (
        min = storage.output_active_power_limit_min,
        max = isnothing(storage.output_active_power_limit_max) ?
              gen.active_power_limits_max : storage.output_active_power_limit_max,
    )
    efficiency = (in = storage.input_efficiency, out = storage.output_efficiency)
    (reactive_power, reactive_power_limits) = make_reactive_params(storage)
    battery = GenericBattery(;
        name = gen.name,
        available = storage.available,
        bus = bus,
        prime_mover = parse_enum_mapping(PrimeMovers, gen.unit_type),
        initial_energy = storage.energy_level,
        state_of_charge_limits = state_of_charge_limits,
        rating = storage.rating,
        active_power = storage.active_power,
        input_active_power_limits = input_active_power_limits,
        output_active_power_limits = output_active_power_limits,
        efficiency = efficiency,
        reactive_power = reactive_power,
        reactive_power_limits = reactive_power_limits,
        base_power = storage.base_power,
    )

    return battery
end
