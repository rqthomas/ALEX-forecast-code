source('R/xgboost_inflows/xgboost.R')
library(tidymodels)
library(timetk)
library(modeltime)

# Inflows ----------------
predictor_vars <- c("precipitation_flux","air_temperature")
model_id <- 'xgboost'
site_id <- 'ALEX'
reference_date <- as_date(config$run_config$forecast_start_datetime)

# Generate training and forecast dataframes
message('Getting training and forecast data')
training_df <- generate_training_df(config, 
                                    met_vars = predictor_vars, 
                                    flow_obs = file.path(config$file_path$qaqc_data_directory, "ALEX-targets-inflow.csv")) |> 
  filter(!between(date, as_datetime("2022-10-01"), as_datetime("2023-03-01")),
         !between(date, as_datetime("2022-10-01"), as_datetime("2023-03-01"))) # remove flood period from training

forecast_df <- generate_forecast_df(met_vars = predictor_vars,
                                    config) |> 
  mutate(precip = log(precip + 1))

# Apply function for temperature variables
message('Temperature forecast')
temp_training_dfs <- training_df |> 
  filter(variable == "TEMP") |> 
  group_by(inflow_name) |> 
  group_split() |> 
  setNames(unique(training_df$inflow_name))

# temp recipe
temp_recipe <- recipe(observation ~ doy + temperature,
                      data = training_df)  |>
  step_normalize(all_numeric_predictors())

# Use function to generate temp forecast
fc_temp <- map(temp_training_dfs, ~ generate_xgb_flow(., 
                                                      use_recipe = temp_recipe, 
                                                      forecast_df = forecast_df,
                                                      transform_function = 'none')) |> 
  list_rbind(names_to = 'inflow_name')

# Apply function to flow variables
message('Flow forecast')
flow_training_dfs <- training_df |> 
  filter(variable == "FLOW") |> 
  mutate(observation = log(observation), # use tranformed data
         precip = log(precip + 1)) |> 
  group_by(inflow_name) |> 
  group_split() |> 
  setNames(unique(training_df$inflow_name))

# flow recipe
flow_recipe <- 'all' # uses all variables from training df + lags in recursive

# Use function to generate flow forecast
fc_flow <- map(flow_training_dfs, ~ generate_xgb_flow(.,
                                                      use_recipe = flow_recipe, 
                                                      transform_function = apply_lags_flow,
                                                      forecast_df = forecast_df)) |> 
  list_rbind(names_to = 'inflow_name') |> 
  mutate(prediction = exp(prediction)) # tranform back


# Apply function to flow variables
message('Salt forecast')
salt_training_dfs <- training_df |> 
  filter(variable == "SALT") |>
  # use only doy, temperature, and observation
  select(-any_of(c("precip"))) |> 
  group_by(inflow_name) |> 
  group_split() |> 
  setNames(unique(training_df$inflow_name))

forecast_df_salt <- forecast_df |> 
  select(-any_of(c("precip")))   # use only doy and observation

# salt_recipe <- recipe(observation ~ doy + temperature,
#                       data = training_df) |>
#   step_normalize(all_numeric_predictors())

# combines with the transformation_fucntion applied to determine
# the overall parameters used in the xgboost model fitting and forecasting
salt_recipe <- 'all'  

# Use function to generate flow forecast
fc_salt <- map(salt_training_dfs, ~ generate_xgb_flow(.,
                                                      use_recipe = salt_recipe, 
                                                      transform_function = apply_lags_salt,
                                                      forecast_df = forecast_df_salt)) |> 
  list_rbind(names_to = 'inflow_name')


inflow_fc <- bind_rows(SALT = fc_salt, 
                       FLOW = fc_flow, 
                       TEMP = fc_temp, 
                       .id = 'variable') |> 
  mutate(model_id = model_id,
         site_id = site_id,
         reference_datetime = reference_date,
         flow_number = ifelse(inflow_name == 'murray', 1, 
                              ifelse(inflow_name == 'finnis', 2, NA))) |> 
  select(site_id, flow_number, datetime, variable, prediction, parameter)

arrow::write_dataset(inflow_fc,
                     glue::glue(config$flows$local_inflow_directory, "/", config$flows$future_inflow_model))


# compare with observations ---------------
# read_csv("targets/ALEX/ALEX-targets-inflow.csv", 
#          show_col_types = FALSE) |> 
#   right_join(inflow_fc) |> 
#   ggplot(aes(x=datetime, y=prediction, group = parameter)) +
#   geom_line() +
#   facet_wrap(inflow_name~variable, scales = 'free') +
#   geom_point(aes(y=observation), colour = "red")
