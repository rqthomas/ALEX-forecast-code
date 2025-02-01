# Combined flow drivers workflow

# Historical ----------------------------------
# Generate some simple historic flows based on the targets
source("R/interpolate_targets.R")

site_id <- 'ALEX'
message('Making historical flows')
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
  select(site_id, flow_number, datetime, variable, prediction, parameter) |> 
  # use only a single inflow
  filter(flow_number == 1)

# Write the interpolated data as the historical file
arrow::write_dataset(hist_interp_inflow,
                     glue::glue(config$flows$local_inflow_directory, "/", config$flows$historical_inflow_model))

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


# Future inflows =======
message('running inflow models')

model_id <- 'combined_inflows'
site_id <- 'ALEX'
reference_date <- as_date(config$run_config$forecast_start_datetime)
horizon <- config$run_config$forecast_horizon

source('R/inflow_salt_xgboost_temporary.R')
salt_fc <- generate_salt_inflow_fc(config)

source('R/inflow_temperature_xgboost_temporary.R')
temp_fc <- generate_temp_inflow_fc(config)


source('R/inflow_flow_process_temporary.R')
# Make sure the units for the loss data are the same as for the prediction
L_mod <- model_losses(model_dat = 'R/helper_data/modelled_losses_DEW.csv', 
                      # data are losses in GL/m at different rates of entitlement flow (GL/d)
                      formula_use = "x ~ y + group", 
                      x = 'loss', y = 'flow', group = 'month')


flow_fc <- generate_flow_inflow_fc(config = config, 
                                   upstream_unit = 'MLd',
                                   lag_t = 14, 
                                   upstream_location = 'QSA',
                                   L_mod = L_mod)  |> 
  mutate(parameter = 0,
         #convert from ML/d to m3/s
         prediction = prediction/86.4) |> 
  # make sure it has the same number of parameter values as the other forecasts!!
  reframe(parameter=unique(salt_fc$parameter), .by = everything())


inflow_fc <- bind_rows(flow_fc, temp_fc, salt_fc) |> 
  mutate(site_id = site_id,
         model_id = 'combined_inflow') |> 
  rename(reference_datetime = reference_date)


arrow::write_dataset(inflow_fc,
                     glue::glue(config$flows$local_inflow_directory,
                                "/", config$flows$future_inflow_model))


# ==== Future outflow persistenceRW =====
# fit the model only for the last month
message('Making peristence outflow forecast')
# Get the observations and interpolate
hist_interp_outflow <- interpolate_targets(targets = 'ALEX-targets-outflow.csv',
                                           lake_directory = lake_directory,
                                           targets_dir = 'targets',
                                           site_id = 'ALEX',
                                           variables = c('FLOW', 'SALT', 'TEMP'),
                                           groups = NULL,
                                           method = 'linear')

# When was the last observation? When to start the forecast
forecast_info <- hist_interp_outflow  |> 
  filter(datetime < reference_date) |> # remove observations after reference_date
  summarise(last_obs = max(datetime),
            .by = c(site_id, variable)) |> 
  mutate(horizon = as.numeric(as_date(reference_date) - as_date(last_obs) + horizon)) |> 
  # what is the total horizon including missing days of observations up to the reference_date
  mutate(start_training = last_obs - days(60))


future_outflow_RW <- hist_interp_outflow |> 
  # filter so we only train on observations within the last month of the last observation
  filter(datetime < reference_date,
         datetime >= forecast_info$start_training) |>
  
  # Make the obs into a tsibble
  as_tsibble(key = c(site_id, variable), index = datetime) |> 
  
  # Fit the model
  fabletools::model(RW = fable::RW(box_cox(observation, 0.3))) |>  
  
  # generate forecast of specific horizon with 31 parameters
  fabletools::generate(h = 30, times = 31, bootstrap = T) |> 
  as_tibble() |> 
  
  rename(parameter = .rep,
         prediction = .sim) |> 
  mutate(reference_datetime = as_date(reference_date),
         model_id = "persistenceRW",
         flow_number = 1,
         prediction = ifelse(prediction < 0, 0, prediction)) |> 
  select(-.model)

arrow::write_dataset(future_outflow_RW,
                     glue::glue(config$flows$local_outflow_directory,
                                "/", config$flows$future_outflow_model))