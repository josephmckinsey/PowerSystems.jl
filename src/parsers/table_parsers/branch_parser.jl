"""
Add branches to the System from the raw data.

"""
function branch_parser!(sys::System, data::PowerSystemTableData)
    available = true

    for branch in iterate_rows(data, InputCategory.BRANCH)
        bus_from = get_bus(sys, branch.connection_points_from)
        bus_to = get_bus(sys, branch.connection_points_to)
        name = get(branch, :name, get_name(bus_from) * "_" * get_name(bus_to))
        connection_points = Arc(bus_from, bus_to)
        pf = branch.active_power_flow
        qf = branch.reactive_power_flow

        #TODO: noop math...Phase-Shifting Transformer angle
        alpha = (branch.primary_shunt / 2) - (branch.primary_shunt / 2)
        branch_type =
            get_branch_type(branch.tap, alpha, get(branch, :is_transformer, nothing))
        if branch_type == Line
            b = branch.primary_shunt / 2
            value = Line(
                name = name,
                available = available,
                active_power_flow = pf,
                reactive_power_flow = qf,
                arc = connection_points,
                r = branch.r,
                x = branch.x,
                b = (from = b, to = b),
                rate = branch.rate,
                angle_limits = (
                    min = branch.min_angle_limits,
                    max = branch.max_angle_limits,
                ),
            )
        elseif branch_type == Transformer2W
            value = Transformer2W(
                name = name,
                available = available,
                active_power_flow = pf,
                reactive_power_flow = qf,
                arc = connection_points,
                r = branch.r,
                x = branch.x,
                primary_shunt = branch.primary_shunt,
                rate = branch.rate,
            )
        elseif branch_type == TapTransformer
            value = TapTransformer(
                name = name,
                available = available,
                active_power_flow = pf,
                reactive_power_flow = qf,
                arc = connection_points,
                r = branch.r,
                x = branch.x,
                primary_shunt = branch.primary_shunt,
                tap = branch.tap,
                rate = branch.rate,
            )
        elseif branch_type == PhaseShiftingTransformer
            # TODO create PhaseShiftingTransformer
            error("Unsupported branch type $branch_type")
        else
            error("Unsupported branch type $branch_type")
        end

        add_component!(sys, value)
    end
end
