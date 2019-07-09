const var_cost_args = Union{Float64, NTuple{2,Float64}, Vector{NTuple{2,Float64}}}

abstract type OperationalCost <: TechnicalParams end

mutable struct VariableCost{T}
    cost::T
end

Base.length(vc::PowerSystems.VariableCost) = length(vc.cost)
Base.getindex(vc::PowerSystems.VariableCost, ix::Int64) = getindex(vc.cost, ix)
