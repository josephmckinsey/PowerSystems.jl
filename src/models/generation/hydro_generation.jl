abstract type
    HydroGen <: Generator
end


struct TechHydro # TODO: should this be a subtype of a technical parameters abstract type
    realpower::Float64 # [MW]
    realpowerlimits::@NT(min::Float64, max::Float64)
    reactivepower::Union{Float64,Nothing} # [MVAr]
    reactivepowerlimits::Union{@NT(min::Float64, max::Float64),Nothing}
    ramplimits::Union{@NT(min::Float64, max::Float64),Nothing}
    timelimits::Union{@NT(min::Float64, max::Float64),Nothing}
    function TechHydro(realpower, realpowerlimits, reactivepower, reactivepowerlimits, ramplimits, timelimits)

        new(realpower, PowerSystems.orderedlimits(realpowerlimits, "Real Power"), reactivepower, PowerSystems.orderedlimits(reactivepowerlimits, "Reactive Power"), ramplimits, timelimits)

    end
end

TechHydro(; realpower = 0.0,
          realpowerlimits = @NT(min = 0.0, max = 0.0),
          reactivepower = nothing,
          reactivepowerlimits = nothing,
          ramplimits = nothing,
          timelimits = nothing
        ) = TechHydro(realpower, realpowerlimits, reactivepower, reactivepowerlimits, ramplimits, timelimits)


struct EconHydro
    curtailcost::Float64 # [$/MWh]
    interruptioncost::Union{Float64,Nothing} # [$]
end

EconHydro(; curtailcost = 0.0, interruptioncost = nothing) = EconHydro(curtailcost, interruptioncost)

struct HydroFix <: HydroGen
    name::String
    status::Bool
    bus::Bus
    tech::TechHydro
    scalingfactor::TimeSeries.TimeArray
end

HydroFix(; name="init",
        status = false,
        bus = Bus(),
        tech = TechHydro(),
        scalingfactor = TimeSeries.TimeArray(today(), [1.0])) = HydroFix(name, status, bus, tech, scalingfactor)


struct HydroCurtailment <: HydroGen
    name::String
    status::Bool
    bus::Bus
    tech::TechHydro
    econ::Union{EconHydro,Nothing}
    scalingfactor::TimeSeries.TimeArray
    function HydroCurtailment(name, status, bus, tech, curtailcost::Float64, scalingfactor)
        econ = EconHydro(curtailcost, nothing)
        new(name, status, bus, tech, econ, scalingfactor)
    end
end

HydroCurtailment(; name = "init",
                status = false,
                bus= Bus(),
                tech = TechHydro(),
                curtailcost = 0.0,
                scalingfactor = TimeSeries.TimeArray(today(), [1.0])) = HydroCurtailment(name, status, bus, tech, curtailcost, scalingfactor)


struct HydroStorage <: HydroGen
    name::String
    status::Bool
    bus::Bus
    tech::TechHydro
    econ::Union{EconHydro,Nothing}
    storagecapacity::Float64
    scalingfactor::TimeSeries.TimeArray
end

HydroStorage(; name = "init",
                status = false,
                bus= Bus(),
                tech = TechHydro(),
                econ = EconHydro(),
                storagecapacity = 0.0,
                scalingfactor = TimeSeries.TimeArray(today(), [1.0])) = HydroStorage(name, status, bus, tech, econ, storagecapacity, scalingfactor)