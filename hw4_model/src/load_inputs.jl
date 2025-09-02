"""
Homework 04 - Load Inputs Module

This module provides functions to load Polish energy system data from ETL outputs.
All functions return DataFrames with standardized column names and proper data types.
"""

using CSV
using DataFrames
using Dates

"""
    load_power_demand(path="etl_outputs/power_demand_baseline.csv")

Load power demand data from CSV file.

Returns DataFrame with columns:
- :timestamp::DateTime - Timestamp for each hour
- :load_mw::Float64 - Power demand in MW

Throws error if file is missing or malformed.
"""
function load_power_demand(path="etl_outputs/power_demand_baseline.csv")
    if !isfile(path)
        throw(ArgumentError("Power demand file not found: $path"))
    end
    
    println("Loading power demand from: $path")
    
    try
        df = CSV.read(path, DataFrame)
        
        # Normalize column names to lowercase
        rename!(df, lowercase.(names(df)))
        
        # Handle different possible column names
        if hasproperty(df, :power_demand_mw)
            rename!(df, :power_demand_mw => :load_mw)
        elseif hasproperty(df, :demand_mw)
            rename!(df, :demand_mw => :load_mw)
        elseif hasproperty(df, :load)
            rename!(df, :load => :load_mw)
        end
        
        # Parse timestamp column
        if hasproperty(df, :timestamp)
            df.timestamp = parse_datetime_column(df.timestamp)
        elseif hasproperty(df, :datetime)
            df.timestamp = parse_datetime_column(df.datetime)
            select!(df, :timestamp, :load_mw)
        elseif hasproperty(df, :time)
            df.timestamp = parse_datetime_column(df.time)
            select!(df, :timestamp, :load_mw)
        end
        
        # Ensure load_mw is Float64
        df.load_mw = Float64.(df.load_mw)
        
        println("Loaded demand with $(nrow(df)) rows")
        return df
        
    catch e
        throw(ArgumentError("Error loading power demand from $path: $e"))
    end
end

"""
    load_power_dc(path="etl_outputs/power_dc_placeholder.csv")

Load data center power demand data from CSV file.

Returns DataFrame with columns:
- :timestamp::DateTime - Timestamp for each hour
- :power_dc_mw::Float64 - Data center power demand in MW

Throws error if file is missing or malformed.
"""
function load_power_dc(path="etl_outputs/power_dc_placeholder.csv")
    if !isfile(path)
        throw(ArgumentError("Data center power file not found: $path"))
    end
    
    println("Loading data center power from: $path")
    
    try
        df = CSV.read(path, DataFrame)
        
        # Normalize column names to lowercase
        rename!(df, lowercase.(names(df)))
        
        # Handle different possible column names
        if hasproperty(df, :power_dc_mw)
            # Already correct name
        elseif hasproperty(df, :dc_power_mw)
            rename!(df, :dc_power_mw => :power_dc_mw)
        elseif hasproperty(df, :datacenter_power_mw)
            rename!(df, :datacenter_power_mw => :power_dc_mw)
        end
        
        # Parse timestamp column
        if hasproperty(df, :timestamp)
            df.timestamp = parse_datetime_column(df.timestamp)
        elseif hasproperty(df, :datetime)
            df.timestamp = parse_datetime_column(df.datetime)
            select!(df, :timestamp, :power_dc_mw)
        elseif hasproperty(df, :time)
            df.timestamp = parse_datetime_column(df.time)
            select!(df, :timestamp, :power_dc_mw)
        end
        
        # Ensure power_dc_mw is Float64
        df.power_dc_mw = Float64.(df.power_dc_mw)
        
        println("Loaded data center power with $(nrow(df)) rows")
        return df
        
    catch e
        throw(ArgumentError("Error loading data center power from $path: $e"))
    end
end

"""
    load_existing_capacity(path="etl_outputs/existing_capacity_by_tech.csv")

Load existing capacity data by technology from CSV file.

Returns DataFrame with columns:
- :technology::String - Technology name
- :existing_capacity_mw::Float64 - Existing capacity in MW

Throws error if file is missing or malformed.
"""
function load_existing_capacity(path="etl_outputs/existing_capacity_by_tech.csv")
    if !isfile(path)
        throw(ArgumentError("Existing capacity file not found: $path"))
    end
    
    println("Loading existing capacity from: $path")
    
    try
        df = CSV.read(path, DataFrame)
        
        # Normalize column names to lowercase
        rename!(df, lowercase.(names(df)))
        
        # Handle different possible column names
        if hasproperty(df, :existing_capacity_mw)
            # Already correct name
        elseif hasproperty(df, :capacity_mw)
            rename!(df, :capacity_mw => :existing_capacity_mw)
        elseif hasproperty(df, :installed_capacity_mw)
            rename!(df, :installed_capacity_mw => :existing_capacity_mw)
        elseif hasproperty(df, :capacity)
            rename!(df, :capacity => :existing_capacity_mw)
        end
        
        # Ensure technology is String and capacity is Float64
        df.technology = String.(df.technology)
        df.existing_capacity_mw = Float64.(df.existing_capacity_mw)
        
        println("Loaded existing capacity for $(nrow(df)) technologies")
        return df
        
    catch e
        throw(ArgumentError("Error loading existing capacity from $path: $e"))
    end
end

"""
    load_tech_params(path="etl_outputs/technology_parameters.csv")

Load technology parameters from CSV file.

Returns DataFrame with columns:
- :technology::String - Technology name
- :capex_eur_per_kw::Float64 - Capital expenditure in EUR per kW
- :var_cost_eur_per_mwh::Float64 - Variable cost in EUR per MWh
- :efficiency::Float64 - Technology efficiency (0-1)
- :lifetime_years::Float64 - Technology lifetime in years

Throws error if file is missing or malformed.
"""
function load_tech_params(path="etl_outputs/technology_parameters.csv")
    if !isfile(path)
        throw(ArgumentError("Technology parameters file not found: $path"))
    end
    
    println("Loading technology parameters from: $path")
    
    try
        df = CSV.read(path, DataFrame)
        
        # Normalize column names to lowercase
        rename!(df, lowercase.(names(df)))
        
        # Map various possible column names to standard names
        column_mapping = Dict()
        
        # Technology column
        if hasproperty(df, :technology)
            # Already correct
        elseif hasproperty(df, :tech)
            column_mapping[:tech] = :technology
        elseif hasproperty(df, :technology_name)
            column_mapping[:technology_name] = :technology
        end
        
        # CAPEX column
        if hasproperty(df, :capex_eur_per_kw)
            # Already correct
        elseif hasproperty(df, :capex)
            column_mapping[:capex] = :capex_eur_per_kw
        elseif hasproperty(df, :capital_cost)
            column_mapping[:capital_cost] = :capex_eur_per_kw
        elseif hasproperty(df, :capex_eur_kw)
            column_mapping[:capex_eur_kw] = :capex_eur_per_kw
        end
        
        # Variable cost column
        if hasproperty(df, :var_cost_eur_per_mwh)
            # Already correct
        elseif hasproperty(df, :var_cost)
            column_mapping[:var_cost] = :var_cost_eur_per_mwh
        elseif hasproperty(df, :variable_cost)
            column_mapping[:variable_cost] = :var_cost_eur_per_mwh
        elseif hasproperty(df, :opex)
            column_mapping[:opex] = :var_cost_eur_per_mwh
        elseif hasproperty(df, :var_cost_eur_mwh)
            column_mapping[:var_cost_eur_mwh] = :var_cost_eur_per_mwh
        end
        
        # Efficiency column
        if hasproperty(df, :efficiency)
            # Already correct
        elseif hasproperty(df, :eta)
            column_mapping[:eta] = :efficiency
        elseif hasproperty(df, :conversion_efficiency)
            column_mapping[:conversion_efficiency] = :efficiency
        end
        
        # Lifetime column
        if hasproperty(df, :lifetime_years)
            # Already correct
        elseif hasproperty(df, :lifetime)
            column_mapping[:lifetime] = :lifetime_years
        elseif hasproperty(df, :economic_lifetime)
            column_mapping[:economic_lifetime] = :lifetime_years
        elseif hasproperty(df, :lifetime_yrs)
            column_mapping[:lifetime_yrs] = :lifetime_years
        end
        
        # Apply column mapping
        if !isempty(column_mapping)
            rename!(df, column_mapping)
        end
        
        # Ensure correct data types
        df.technology = String.(df.technology)
        df.capex_eur_per_kw = Float64.(df.capex_eur_per_kw)
        df.var_cost_eur_per_mwh = Float64.(df.var_cost_eur_per_mwh)
        df.efficiency = Float64.(df.efficiency)
        df.lifetime_years = Float64.(df.lifetime_years)
        
        println("Loaded technology parameters for $(nrow(df)) technologies")
        return df
        
    catch e
        throw(ArgumentError("Error loading technology parameters from $path: $e"))
    end
end

"""
    load_cf_profiles(path="etl_outputs/capacity_factors_profiles.csv")

Load capacity factor profiles from CSV file.

Returns a wide DataFrame with columns:
- :timestamp::DateTime - Timestamp for each hour
- One column per technology (e.g., :WindOnshore, :SolarPV, etc.) with capacity factors

Throws error if file is missing or malformed.
"""
function load_cf_profiles(path="etl_outputs/capacity_factors_profiles.csv")
    if !isfile(path)
        throw(ArgumentError("Capacity factors file not found: $path"))
    end
    
    println("Loading capacity factor profiles from: $path")
    
    try
        df = CSV.read(path, DataFrame)
        
        # Normalize column names to lowercase
        rename!(df, lowercase.(names(df)))
        
        # Find and parse timestamp column
        timestamp_col = nothing
        if hasproperty(df, :timestamp)
            timestamp_col = :timestamp
        elseif hasproperty(df, :datetime)
            timestamp_col = :datetime
        elseif hasproperty(df, :time)
            timestamp_col = :time
        else
            throw(ArgumentError("No timestamp column found in capacity factors file"))
        end
        
        # Parse timestamp
        df[!, timestamp_col] = parse_datetime_column(df[!, timestamp_col])
        
        # Rename timestamp column to standard name
        if timestamp_col != :timestamp
            rename!(df, timestamp_col => :timestamp)
        end
        
        # Convert all other columns to Float64 (capacity factors)
        for col in names(df)
            if col != :timestamp
                df[!, col] = Float64.(df[!, col])
            end
        end
        
        # Get technology columns (all except timestamp)
        tech_cols = [col for col in names(df) if col != :timestamp]
        
        println("Loaded capacity factor profiles with $(nrow(df)) rows for technologies: $(join(tech_cols, ", "))")
        return df
        
    catch e
        throw(ArgumentError("Error loading capacity factor profiles from $path: $e"))
    end
end

"""
    parse_datetime_column(datetime_col)

Helper function to robustly parse DateTime columns from various formats.

Supports common formats like:
- "2023-01-01 00:00:00"
- "2023-01-01T00:00:00"
- "2023-01-01"
- Unix timestamps
"""
function parse_datetime_column(datetime_col)
    # Try different parsing strategies
    parsed_times = Vector{DateTime}(undef, length(datetime_col))
    
    for (i, time_str) in enumerate(datetime_col)
        try
            # Try parsing as string first
            if isa(time_str, String)
                # Try common datetime formats
                for fmt in [
                    "yyyy-mm-dd HH:MM:SS",
                    "yyyy-mm-ddTHH:MM:SS",
                    "yyyy-mm-dd",
                    "yyyy-mm-dd HH:MM",
                    "yyyy-mm-ddTHH:MM"
                ]
                    try
                        parsed_times[i] = DateTime(time_str, fmt)
                        break
                    catch
                        continue
                    end
                end
                
                # If still not parsed, try as Unix timestamp
                if ismissing(parsed_times[i])
                    try
                        timestamp = parse(Float64, time_str)
                        parsed_times[i] = unix2datetime(timestamp)
                    catch
                        throw(ArgumentError("Could not parse datetime: $time_str"))
                    end
                end
            else
                # Already a DateTime or similar
                parsed_times[i] = DateTime(time_str)
            end
        catch e
            throw(ArgumentError("Error parsing datetime at row $i: $time_str - $e"))
        end
    end
    
    return parsed_times
end

# Export all public functions
export load_power_demand, load_power_dc, load_existing_capacity, load_tech_params, load_cf_profiles
