# Generate some simple historic flows based on the targets
source("R/interpolate_targets.R")

site_id <- 'ALEX'

# ==== Historical interpolation inflows ====
hist_interp_inflow <- interpolate_targets(targets = 'ALEX-targets-inflow.csv', 
                                          lake_directory = lake_directory,
                                          targets_dir = 'targets', 
                                          site_id = site_id, 
                                          variables = c('FLOW', 'SALT', 'TEMP'),
                                          groups = 'inflow_name',
                                          method = 'linear') |> 
  mutate(flow_number = ifelse(inflow_name == 'murray', 1, ifelse(inflow_name == 'finnis', 2, NA)), 
         parameter = 1) |> 
  rename(prediction = observation) |> 
  select(site_id, flow_number, datetime, variable, prediction, parameter)

# Write the interpolated data as the historical file
arrow::write_dataset(hist_interp_inflow,
                     glue::glue(config$flows$local_inflow_directory, "/", config$flows$historical_inflow_model))

# generate a simple "forecast" that has ensemble members
# reference_datetime <- config$run_config$forecast_start_datetime
# reference_date <- as_date(reference_datetime)
# 
# future_inflow <- hist_interp_inflow |>
#   filter(datetime >= as_datetime(reference_date),
#          datetime <= as_datetime(reference_date) + days(config$run_config$forecast_horizon)) |>
#   # reframe(prediction = rnorm(10, mean = prediction, sd = 1),
#   #         parameter = 1:10,
#   #         .by = c(site_id, datetime, variable, flow_number)) |>
#   mutate(reference_datetime = as_date(reference_date),
#          prediction = ifelse(prediction <0, 0, prediction))
# 
# arrow::write_dataset(future_inflow,
#                      glue::glue(config$flows$local_inflow_directory, "/", config$flows$future_inflow_model))
#==========================================#

# ==== Historical interpolation outflows ====
hist_interp_outflow <- interpolate_targets(targets = 'ALEX-targets-outflow.csv',
                                           lake_directory = lake_directory,
                                           targets_dir = 'targets',
                                           site_id = 'ALEX',
                                           variables = c('FLOW', 'SALT', 'TEMP'),
                                           groups = NULL,
                                           method = 'linear') |>
  mutate(flow_number = 1,
         parameter = 1) |>
  rename(prediction = observation)


# Write the interpolated data as the historal file
arrow::write_dataset(hist_interp_outflow,
                     glue::glue(config$flows$local_outflow_directory, "/", config$flows$historical_outflow_model))



# ==== Future outflow scenarios =====
# an example scenario that is the median flow of the last month
# forecast_dates <- expand_grid(datetime = seq.Date(reference_date,
#                                                   as_date(reference_date) + config$run_config$forecast_horizon,
#                                                   by = "day"),
#                               parameter = 1:31) |> 
#   mutate(site_id = 'ALEX', 
#          flow_number = 1)
# 
# future_outflow_scenario <-
#   hist_interp_outflow |> 
#   filter(datetime > as_date(config$run_config$forecast_start_datetime) - months(1),
#          datetime <= as_date(config$run_config$forecast_start_datetime)) |> 
#   reframe(.by = c(site_id, variable, flow_number),
#           scenario_q50 = quantile(probs = 0.50, prediction),
#           scenario_q10 = quantile(probs = 0.10, prediction),
#           scenario_q90 = quantile(probs = 0.90, prediction)) |> 
#   pivot_longer(scenario_q50:scenario_q90, 
#                names_to = 'model_id',
#                values_to = 'prediction') |> 
#   full_join(forecast_dates, relationship = "many-to-many", by = join_by(site_id, flow_number)) |> 
#   mutate(reference_datetime = as_date(reference_date),
#          prediction = ifelse(prediction <0, 0, prediction)) |> 
#   filter(model_id == str_extract(config$flows$future_outflow_model, "scenario_q\\d{2}")) 
# 
# arrow::write_dataset(future_outflow_scenario,
#                      glue::glue(config$flows$local_outflow_directory, "/", config$flows$future_outflow_model))


# ==== outflows ==== #
# hist_interp_outflow <- interpolate_targets(targets = 'ALEX-targets-outflow.csv', 
#                                            lake_directory = lake_directory,
#                                            targets_dir = 'targets', 
#                                            site_id = 'ALEX', 
#                                            variables = c('FLOW', 'SALT', 'TEMP'),
#                                            groups = NULL,
#                                            method = 'linear') |> 
#   mutate(flow_number = 1, 
#          parameter = 1) |> 
#   rename(prediction = observation)
# 
# 
# # Write the interpolated data as the historal file
# arrow::write_dataset(hist_interp_outflow,
#                      glue::glue(config$flows$local_outflow_directory, "/", config$flows$historical_outflow_model))
# 
# generate a simple "forecast" that has ensemble members
# future_outflow <- hist_interp_outflow |>
#   filter(datetime >= as_datetime(reference_date),
#          datetime <= as_date(reference_date) + config$run_config$forecast_horizon) |>
#   # reframe(prediction = rnorm(10, mean = prediction, sd = 1),
#   #         parameter = 1:10,
#   #         .by = c(site_id, datetime, variable, flow_number)) |>
#   mutate(reference_datetime = as_date(reference_date),
#          prediction = ifelse(prediction <0, 0, prediction))

# arrow::write_dataset(future_outflow,
#                      glue::glue(config$flows$local_outflow_directory, "/", config$flows$future_outflow_model))
