"""
Add DC branches to the System from raw data.
"""
function dc_branch_parser!(sys::System, data::PowerSystemTableData)
    function make_dc_limits(dc_branch, min, max)
        min_lim = dc_branch[min]
        if isnothing(dc_branch[min]) && isnothing(dc_branch[max])
            throw(DataFormatError("valid limits required for $min , $max"))
        elseif isnothing(dc_branch[min])
            min_lim = dc_branch[max] * -1.0
        end
        return (min = min_lim, max = dc_branch[max])
    end

    for dc_branch in iterate_rows(data, InputCategory.DC_BRANCH)
        available = true
        bus_from = get_bus(sys, dc_branch.connection_points_from)
        bus_to = get_bus(sys, dc_branch.connection_points_to)
        connection_points = Arc(bus_from, bus_to)

        if dc_branch.control_mode == "Power"
            mw_load = dc_branch.mw_load

            activepowerlimits_from = make_dc_limits(
                dc_branch,
                :min_active_power_limit_from,
                :max_active_power_limit_from,
            )
            activepowerlimits_to = make_dc_limits(
                dc_branch,
                :min_active_power_limit_to,
                :max_active_power_limit_to,
            )
            reactivepowerlimits_from = make_dc_limits(
                dc_branch,
                :min_reactive_power_limit_from,
                :max_reactive_power_limit_from,
            )
            reactivepowerlimits_to = make_dc_limits(
                dc_branch,
                :min_reactive_power_limit_to,
                :max_reactive_power_limit_to,
            )

            loss = (l0 = 0.0, l1 = dc_branch.loss) #TODO: Can we infer this from the other data?,

            value = HVDCLine(
                name = dc_branch.name,
                available = available,
                active_power_flow = dc_branch.active_power_flow,
                arc = connection_points,
                active_power_limits_from = activepowerlimits_from,
                active_power_limits_to = activepowerlimits_to,
                reactive_power_limits_from = reactivepowerlimits_from,
                reactive_power_limits_to = reactivepowerlimits_to,
                loss = loss,
            )
        else
            rectifier_taplimits = (
                min = dc_branch.rectifier_tap_limits_min,
                max = dc_branch.rectifier_tap_limits_max,
            )
            rectifier_xrc = dc_branch.rectifier_xrc #TODO: What is this?,
            rectifier_firingangle = dc_branch.rectifier_firingangle
            inverter_taplimits = (
                min = dc_branch.inverter_tap_limits_min,
                max = dc_branch.inverter_tap_limits_max,
            )
            inverter_xrc = dc_branch.inverter_xrc #TODO: What is this?
            inverter_firingangle = (
                min = dc_branch.inverter_firing_angle_min,
                max = dc_branch.inverter_firing_angle_max,
            )
            value = VSCDCLine(
                name = dc_branch.name,
                available = true,
                active_power_flow = pf,
                arc = connection_points,
                rectifier_taplimits = rectifier_taplimits,
                rectifier_xrc = rectifier_xrc,
                rectifier_firingangle = rectifier_firingangle,
                inverter_taplimits = inverter_taplimits,
                inverter_xrc = inverter_xrc,
                inverter_firingangle = inverter_firingangle,
            )
        end

        add_component!(sys, value)
    end
end
