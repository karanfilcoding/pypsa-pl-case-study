"""
Homework 04 - Polish Energy System Model

Main entry point for the Homework 04 model using Polish energy system data.
This model integrates ETL outputs from PyPSA-PL to run energy system optimization.
"""

# Load required packages
using JuMP
using Gurobi  # or other solver
using CSV
using DataFrames
using Dates

# Include and load our custom input loader module
include("src/load_inputs.jl")
using .LoadInputs

"""
    main()

Main function to run the Homework 04 Polish energy system model.
Loads data from ETL outputs and runs optimization.
"""
function main()
    println("="^60)
    println("Homework 04 - Polish Energy System Model")
    println("="^60)
    
    # Load all input data using the new loader functions
    println("\n📊 Loading Polish energy system data...")
    
    # 1. Load power demand data
    println("\n1. Loading power demand...")
    demand_df = load_power_demand()
    println("   ✓ Demand data: $(nrow(demand_df)) hours")
    println("   ✓ Date range: $(first(demand_df.timestamp)) to $(last(demand_df.timestamp))")
    
    # 2. Load data center power demand
    println("\n2. Loading data center power demand...")
    dc_df = load_power_dc()
    println("   ✓ Data center data: $(nrow(dc_df)) hours")
    
    # 3. Load existing capacity by technology
    println("\n3. Loading existing capacity...")
    cap_df = load_existing_capacity()
    println("   ✓ Technologies with existing capacity: $(nrow(cap_df))")
    println("   ✓ Technologies: $(join(cap_df.technology, ", "))")
    
    # 4. Load technology parameters
    println("\n4. Loading technology parameters...")
    tech_df = load_tech_params()
    println("   ✓ Technologies with parameters: $(nrow(tech_df))")
    println("   ✓ Technologies: $(join(tech_df.technology, ", "))")
    
    # 5. Load capacity factor profiles
    println("\n5. Loading capacity factor profiles...")
    cf_df = load_cf_profiles()
    println("   ✓ Capacity factor data: $(nrow(cf_df)) hours")
    tech_cols = [col for col in names(cf_df) if col != :timestamp]
    println("   ✓ Technologies with profiles: $(length(tech_cols))")
    println("   ✓ Technologies: $(join(tech_cols, ", "))")
    
    # Map data into model structures
    println("\n🔧 Mapping data into model structures...")
    
    # Extract time series data
    T = nrow(demand_df)  # Number of time periods
    println("   ✓ Time periods: $T")
    
    # Power demand vector
    power_demand = demand_df.load_mw
    println("   ✓ Power demand vector: $(length(power_demand)) elements")
    println("   ✓ Demand range: $(round(minimum(power_demand), digits=1)) - $(round(maximum(power_demand), digits=1)) MW")
    
    # Data center power demand vector
    power_dc = dc_df.power_dc_mw
    println("   ✓ Data center power vector: $(length(power_dc)) elements")
    println("   ✓ DC power range: $(round(minimum(power_dc), digits=1)) - $(round(maximum(power_dc), digits=1)) MW")
    
    # Initial capacities
    initial_capacities = Dict()
    for row in eachrow(cap_df)
        initial_capacities[row.technology] = row.existing_capacity_mw
    end
    println("   ✓ Initial capacities: $(length(initial_capacities)) technologies")
    for (tech, cap) in initial_capacities
        println("     - $tech: $(round(cap, digits=1)) MW")
    end
    
    # Technology parameters
    tech_params = Dict()
    for row in eachrow(tech_df)
        tech_params[row.technology] = Dict(
            :capex => row.capex_eur_per_kw,
            :var_cost => row.var_cost_eur_per_mwh,
            :efficiency => row.efficiency,
            :lifetime => row.lifetime_years
        )
    end
    println("   ✓ Technology parameters: $(length(tech_params)) technologies")
    for (tech, params) in tech_params
        println("     - $tech: CAPEX=$(round(params[:capex], digits=0)) €/kW, " *
                "VarCost=$(round(params[:var_cost], digits=1)) €/MWh, " *
                "Efficiency=$(round(params[:efficiency], digits=3)), " *
                "Lifetime=$(round(params[:lifetime], digits=0)) years")
    end
    
    # Capacity factor profiles
    capacity_factors = Dict()
    for tech in tech_cols
        capacity_factors[tech] = cf_df[!, tech]
    end
    println("   ✓ Capacity factor profiles: $(length(capacity_factors)) technologies")
    for (tech, cf_profile) in capacity_factors
        avg_cf = mean(cf_profile)
        println("     - $tech: avg CF=$(round(avg_cf, digits=3))")
    end
    
    # Create and solve the optimization model
    println("\n🚀 Creating optimization model...")
    model = create_optimization_model(
        T, power_demand, power_dc, initial_capacities, 
        tech_params, capacity_factors
    )
    
    println("\n⚡ Solving optimization model...")
    optimize!(model)
    
    # Process and display results
    println("\n📈 Processing results...")
    process_results(model, demand_df.timestamp)
    
    println("\n✅ Homework 04 model completed successfully!")
    return model
end

"""
    create_optimization_model(T, power_demand, power_dc, initial_capacities, tech_params, capacity_factors)

Create the JuMP optimization model for the Polish energy system.
"""
function create_optimization_model(T, power_demand, power_dc, initial_capacities, tech_params, capacity_factors)
    model = Model(Gurobi.Optimizer)
    
    # Get technology sets
    technologies = collect(keys(tech_params))
    renewable_techs = [tech for tech in technologies if haskey(capacity_factors, tech)]
    dispatchable_techs = setdiff(technologies, renewable_techs)
    
    println("   ✓ Technologies: $(length(technologies)) total")
    println("   ✓ Renewable technologies: $(length(renewable_techs))")
    println("   ✓ Dispatchable technologies: $(length(dispatchable_techs))")
    
    # Variables
    @variable(model, cap_new[technologies] >= 0)  # New capacity additions
    @variable(model, gen[t=1:T, tech in technologies] >= 0)  # Generation
    @variable(model, curtail[t=1:T, tech in renewable_techs] >= 0)  # Curtailment
    
    # Objective: Minimize total system cost
    @objective(model, Min, 
        sum(tech_params[tech][:capex] * cap_new[tech] for tech in technologies) +  # Investment cost
        sum(tech_params[tech][:var_cost] * gen[t, tech] for t in 1:T, tech in technologies)  # Operating cost
    )
    
    # Constraints
    # 1. Power balance constraint
    @constraint(model, power_balance[t=1:T],
        sum(gen[t, tech] for tech in technologies) == power_demand[t] + power_dc[t]
    )
    
    # 2. Capacity constraints for renewable technologies
    for t in 1:T, tech in renewable_techs
        total_cap = get(initial_capacities, tech, 0.0) + cap_new[tech]
        @constraint(model, gen[t, tech] + curtail[t, tech] == 
                   total_cap * capacity_factors[tech][t])
    end
    
    # 3. Capacity constraints for dispatchable technologies
    for t in 1:T, tech in dispatchable_techs
        total_cap = get(initial_capacities, tech, 0.0) + cap_new[tech]
        @constraint(model, gen[t, tech] <= total_cap)
    end
    
    # 4. Non-negativity constraints (already handled in variable definitions)
    
    println("   ✓ Model created with $(length(technologies)) technologies and $T time periods")
    println("   ✓ Variables: $(num_variables(model))")
    println("   ✓ Constraints: $(num_constraints(model))")
    
    return model
end

"""
    process_results(model, timestamps)

Process and display optimization results.
"""
function process_results(model, timestamps)
    if termination_status(model) == MOI.OPTIMAL
        println("   ✓ Optimization solved successfully!")
        println("   ✓ Objective value: $(round(objective_value(model), digits=0)) €")
        
        # Get variable values
        cap_new = value.(model[:cap_new])
        gen = value.(model[:gen])
        
        println("\n📊 Investment Results:")
        for tech in keys(cap_new)
            if cap_new[tech] > 1.0  # Only show significant investments
                println("   - $tech: $(round(cap_new[tech], digits=1)) MW")
            end
        end
        
        println("\n⚡ Generation Summary:")
        total_gen = sum(gen)
        for tech in keys(cap_new)
            tech_gen = sum(gen[:, tech])
            if tech_gen > 1.0  # Only show significant generation
                share = tech_gen / total_gen * 100
                println("   - $tech: $(round(tech_gen, digits=0)) MWh ($(round(share, digits=1))%)")
            end
        end
        
        # Save results to CSV
        save_results_to_csv(model, timestamps)
        
    else
        println("   ❌ Optimization failed!")
        println("   Status: $(termination_status(model))")
    end
end

"""
    save_results_to_csv(model, timestamps)

Save optimization results to CSV files.
"""
function save_results_to_csv(model, timestamps)
    println("\n💾 Saving results to CSV...")
    
    # Generation results
    gen_df = DataFrame(timestamp = timestamps)
    for tech in keys(model[:cap_new])
        gen_df[!, tech] = value.(model[:gen][:, tech])
    end
    
    CSV.write("hw4_results_generation.csv", gen_df)
    println("   ✓ Generation results saved to hw4_results_generation.csv")
    
    # Investment results
    inv_df = DataFrame(
        technology = collect(keys(model[:cap_new])),
        new_capacity_mw = [value(model[:cap_new][tech]) for tech in keys(model[:cap_new])]
    )
    
    CSV.write("hw4_results_investment.csv", inv_df)
    println("   ✓ Investment results saved to hw4_results_investment.csv")
end

# Run the model if this file is executed directly
if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
