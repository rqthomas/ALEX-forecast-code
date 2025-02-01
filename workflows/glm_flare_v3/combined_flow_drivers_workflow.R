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

#---------------------------------------------------------------#

# Future --------------------------------------------------------

library(fable)
library(tsibble)

predictor_vars <- c("precipitation_flux","air_temperature")
model_id <- 'persistenceRW'
site_id <- 'ALEX'
reference_date <- as_date(config$run_config$forecast_start_datetime)
horizon <- config$run_config$forecast_horizon

# ==== Future inflow persistenceRW + XGBoost + lag_mod =====
message('Making inflow forecast')
# Flow model
future_inflow_lag <- generate_flow_lag(config = config, 
                                       upstream_lag = 10, 
                                       upstream_data = 'QSA') |> 
  mutate(reference_datetime = as_date(reference_date),
         model_id = "lag_flow",
         prediction = ifelse(prediction < 0, 0, prediction),
         variable = 'FLOW', 
         flow_number = 1) |> 
  filter(datetime >= as_datetime(reference_date),
         datetime <= as_datetime(reference_date) + days(horizon)) 

# copy to have 31 ensemble members
future_inflow_lag <- map_dfr(0:30, ~cbind(future_inflow_lag, parameter = .))

# fit the model only for the last month
# Get the observations and interpolate
hist_interp_inflow <- interpolate_targets(targets = 'ALEX-targets-inflow.csv', 
                                          lake_directory = lake_directory,
                                          targets_dir = 'targets', 
                                          site_id = site_id, 
                                          variables = c('FLOW', 'SALT', 'TEMP'),
                                          groups = 'inflow_name',
                                          method = 'linear') |> 
  mutate(flow_number = ifelse(inflow_name == 'murray', 1, ifelse(inflow_name == 'finnis', 2, NA)), 
         parameter = 1) |> 
  select(site_id, flow_number, datetime, variable, observation, parameter) |> 
  # use only a single inflow
  filter(flow_number == 1)

# When was the last observation? When to start the forecast
forecast_info <- hist_interp_inflow  |> 
  filter(datetime < reference_date) |> # remove observations after reference_date
  summarise(last_obs = max(datetime),
            .by = c(site_id, variable, flow_number)) |> 
  mutate(horizon = as.numeric(as_date(reference_date) - last_obs + horizon)) |> 
  # what is the total horizon including missing days of observations up to the reference_date
  mutate(start_training = last_obs - days(60))

future_inflow_RW <- NULL

site_var <- distinct(hist_interp_inflow, variable, flow_number)

for (i in 1:nrow(site_var)) {
  site <- site_var$flow_number[i]
  var <- site_var$variable[i]
  
  forecast_info_var <- filter(forecast_info, variable == var, flow_number == site)
  
  site_var_RW <- hist_interp_inflow |> 
    
    # filter so we only train on observations within the last month of the last observation
    filter(datetime < reference_date,
           datetime >= forecast_info_var$start_training,
           variable == var,
           flow_number == site) |>
    
    # Make the obs into a tsibble
    as_tsibble(key = c(site_id, variable, flow_number), index = datetime) |> 
    
    # Fit the model
    fabletools::model(RW = fable::RW(box_cox(observation, 0.3))) |>  
    
    # generate forecast of specific horizon with 31 parameters
    fabletools::generate(h = forecast_info_var$horizon, times = 31, bootstrap = T) |> 
    as_tibble() |> 
    
    # Reverse transformation
    # mutate(.sim = exp(.sim)) |> 
    
    rename(parameter = .rep,
           prediction = .sim) |> 
    mutate(reference_datetime = as_date(reference_date),
           model_id = "persistenceRW",
           parameter = as.numeric(parameter) -1,
           prediction = ifelse(prediction < 0, 0, prediction)) |> 
    select(-.model)
  
  future_inflow_RW <- bind_rows(future_inflow_RW, site_var_RW)
}

# Temperature XGBoost
# Generate training and forecast dataframes
source('R/xgboost_inflows/xgboost.R')
library(tidymodels)
library(timetk)
library(modeltime)

message('Using xgb for temperature')
training_df <- generate_training_df(config, 
                                    met_vars = predictor_vars, 
                                    flow_obs = file.path(config$file_path$qaqc_data_directory, "ALEX-targets-inflow.csv")) |> 
  filter(!between(date, as_datetime("2022-10-01"), as_datetime("2023-03-01")),
         !between(date, as_datetime("2022-10-01"), as_datetime("2023-03-01"))) # remove flood period from training

forecast_df <- generate_forecast_df(met_vars = predictor_vars,
                                    config = config) |> 
  mutate(precip = log(precip + 1))

# Apply function for temperature variables
temp_training_dfs <- training_df |> 
  filter(variable == "TEMP", inflow_name == 'murray') |> 
  select(-precip) #|> 
  # group_by(inflow_name) |> 
  # group_split() |> 
  # setNames(unique(training_df$inflow_name))

# temp recipe
# temp_recipe <- recipe(observation ~ doy + temperature + observation_lag,
#                       data = training_df)  |>
#   step_normalize(all_numeric_predictors())
temp_recipe <- 'all'

# Use function to generate temp forecast
fc_temp <- generate_xgb_flow(training_df = temp_training_dfs,
                             use_recipe = temp_recipe,
                             forecast_df = forecast_df,
                             transform_function = apply_lags_temp,
                             refit_model = F) |> 
# map(temp_training_dfs, ~ generate_xgb_flow(., 
#                                                       use_recipe = temp_recipe, 
#                                                       forecast_df = forecast_df,
#                                                       transform_function = apply_lags_temp, 
#                                                       refit_model = F)) |> 
  # list_rbind(names_to = 'flow_number') |> 
  mutate(flow_number = 1, #as.numeric(ifelse(flow_number == 'murray', 1, 2)),
         # parameter = as.character(parameter),
         model_id = 'xgb',
         site_id = 'ALEX',
         variable = 'TEMP',
         reference_datetime = reference_date)

# Combine the three models into a forecast for the inflow
inflow_fc <- future_inflow_RW |>
  filter(variable == 'SALT') |> # remove temperature + inflow RW predictions
  bind_rows(fc_temp) |> 
  bind_rows(future_inflow_lag) |> 
  mutate(model_id = 'combined_inflow_fc',
         datetime = as_date(datetime))

arrow::write_dataset(inflow_fc,
                     glue::glue(config$flows$local_inflow_directory, "/", config$flows$future_inflow_model))


# inflow_fc |>
#   left_join(targets_inflow,
#              by = join_by(site_id, datetime, variable, flow_number),
#              relationship = "many-to-many") |>
#   ggplot(aes(x=datetime, y=prediction, group = interaction(parameter, model_id), colour = model_id)) +
#   geom_line() +
#   facet_wrap(variable~flow_number, scales = 'free') +
#   geom_point(aes(y=observation), colour = 'red')


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
  
  # Reverse transformation
  # mutate(.sim = exp(.sim)) |> 
  
  rename(parameter = .rep,
         prediction = .sim) |> 
  mutate(reference_datetime = as_date(reference_date),
         model_id = "persistenceRW",
         flow_number = 1,
         prediction = ifelse(prediction < 0, 0, prediction)) |> 
  select(-.model)

# If there's no increase/decrease then save
if (sum(str_detect(config$flows$future_outflow_model, c('increase', 'decrease')) == 0)) {
  arrow::write_dataset(future_outflow_RW,
                     glue::glue(config$flows$local_outflow_directory, "/", config$flows$future_outflow_model))
}


# Future outflow scenarios -----------
# increasing the persistence values by 20%
if (str_detect(config$flows$future_outflow_model, 'increase')) {
  future_outflow_RW |> 
  mutate(prediction = prediction * 1.2) |> 
  arrow::write_dataset(glue::glue(config$flows$local_outflow_directory, "/", config$flows$future_outflow_model))
  message('using an increased outflow future scenario')
}

if (str_detect(config$flows$future_outflow_model, 'decrease')) {
  future_outflow_RW |> 
    mutate(prediction = prediction * 0.8) |> 
    arrow::write_dataset(glue::glue(config$flows$local_outflow_directory, "/", config$flows$future_outflow_model))
  message('using an decreased outflow future scenario')
  
}

