library(fable)
library(tsibble)
source('R/interpolate_targets.R')
# Outflows ----------------
predictor_vars <- c("precipitation_flux","air_temperature")
model_id <- 'persistenceRW'
site_id <- 'ALEX'
reference_date <- as_date(config$run_config$forecast_start_datetime)
horizon <- config$run_config$forecast_horizon

# ==== Future outflow persistenceRW =====
# fit the model only for the last month

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
  filter(datetime <= reference_date) |> # remove observations after reference_date
  summarise(last_obs = max(datetime),
            .by = c(site_id, variable)) |> 
  mutate(horizon = as.numeric(last_obs - reference_date + horizon)) |> 
# what is the total horizon including missing days of observations up to the reference_date
  mutate(start_training = last_obs - months(2))



future_outflow_RW <- hist_interp_outflow |> 
  # filter so we only train on observations within the last month of the last observation
  filter(datetime <= reference_date,
         datetime >= forecast_info$start_training) |>
  
  # Make the obs into a tsibble
  as_tsibble(key = c(site_id, variable), index = datetime) |> 
  
  # apply a log transformation and fit model
  # mutate(observation = box_cox(observation, 0.3)) |>
  # ggplot(aes(x=datetime, y=observation)) + geom_line()
  
  # Fit the model
  fabletools::model(RW = fable::RW(box_cox(observation, 0.3))) |>  

  # generate forecast of specific horizon with 31 parameters
  fabletools::generate(h = 30, times = 62, bootstrap = T) |> 
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


arrow::write_dataset(future_outflow_RW,
                     glue::glue(config$flows$local_outflow_directory, "/", config$flows$future_outflow_model))
