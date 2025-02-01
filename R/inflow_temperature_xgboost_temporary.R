library(tidymodels)

generate_temp_inflow_fc <- function(config,
                                      met_vars = c("precipitation_flux", "air_temperature"), 
                                      flow_obs = file.path(config$file_path$qaqc_data_directory, "ALEX-targets-inflow.csv")) {
  
  # Set up
  reference_date <- as_datetime(config$run_config$forecast_start_datetime)
  noaa_date <- reference_date - lubridate::days(1)
  site_id <- config$location$site_id
  start_training <- reference_date - years(5)
  horizon <- config$run_config$forecast_horizon
  end_date <-  config$run_config$end_datetime
  
  # Get historic met ----------------
  message("getting met data")
  met_s3_historical <- arrow::s3_bucket(paste0("bio230121-bucket01/flare/drivers/met/gefs-v12/stage3/site_id=",site_id),
                                        endpoint_override = config$s3$drivers$endpoint,
                                        anonymous = TRUE) |> 
    arrow::open_dataset() |> 
    dplyr::filter(variable %in% met_vars,
                  datetime < reference_date,
                  datetime > start_training) |>
    collect() |> 
    mutate(variable = ifelse(variable == "precipitation_flux", "precipitation", variable),
           variable = ifelse(variable == "air_temperature", "temperature_2m", variable),
           prediction = ifelse(variable == "temperature_2m", prediction - 273.15, prediction)) |> 
    select(-reference_datetime)
  
  # Get future met
  met_s3_future <- arrow::s3_bucket(file.path("bio230121-bucket01/flare/drivers/met/gefs-v12/stage2", 
                                              paste0("reference_datetime=",
                                                     noaa_date),
                                              paste0("site_id=",
                                                     site_id)),
                                    endpoint_override = config$s3$drivers$endpoint,
                                    anonymous = TRUE) |> 
    arrow::open_dataset() |> 
    dplyr::filter(variable %in% met_vars, 
                  datetime >= reference_date) |> 
    collect() |> 
    mutate(variable = ifelse(variable == "precipitation_flux", "precipitation", variable),
           variable = ifelse(variable == "air_temperature", "temperature_2m", variable),
           prediction = ifelse(variable == "temperature_2m", prediction - 273.15, prediction)) 
  
  
  # combine past and future noaa data
  met_combined <- bind_rows(met_s3_historical, met_s3_future) |> 
    arrange(variable, parameter,datetime) |> 
    pivot_wider(names_from = variable, values_from = prediction) |> 
    mutate(date = lubridate::as_date(datetime)) |> 
    reframe(precip = sum(precipitation, na.rm = TRUE), # what is the total per day
            temperature = median(temperature_2m, na.rm = TRUE), # what is the average temperature per day
            .by = c("date", "parameter")) |> # retain the ensemble members 
    group_by(parameter) |> 
    mutate(#fifteen_precip = RcppRoll::roll_sum(precip, n = 3, fill = NA,align = "right"), # calculate a 15-day rolling sum
      threeday_temp = RcppRoll::roll_mean(temperature, n = 3, fill = NA, align = "right"), # calculate a 3-day rolling mean
      doy = lubridate::yday(date)) |> ungroup()
  
  if(!config$uncertainty$weather){
    met_combined <- met_combined |>
      dplyr::filter(parameter == 1)
  }
  
  
  # Get historic flow observations ---------------
  message("getting historic observations")
  inflow_targets <- read_csv(flow_obs, show_col_types = FALSE) |> 
    dplyr::filter(datetime < reference_date) |> 
    rename(date = datetime) |>
    # pivot_wider(names_from = variable,
    #             values_from = observation) |> 
    group_by(site_id, inflow_name, variable) |> 
    mutate(observation = imputeTS::na_interpolation(observation, yright = NA))
  
  
  # Generate training df -----------
  training_df <- met_combined  |> 
    pivot_longer(cols = precip:doy, names_to = 'variable', values_to = 'value') |> 
    dplyr::filter(date <= reference_date) |> # historical period only
    reframe(.by = c('date', 'variable'),
            value = median(value)) |> 
    pivot_wider(names_from = variable, values_from = 'value') |> 
    left_join(inflow_targets, by = c('date')) #|> # combine with the inflow observations
    # left_join(upstream, by = c('date')) 
  
  
  if (is.na(end_date) & !is.na(horizon)) {
    end_date <- reference_date + days(horizon)
  } else if (!is.na(end_date) & is.na(horizon)) {
    horizon <- as.numeric(end_date - reference_date)
  }
  
  
  
  # Generate forecast df ------------
  forecast_df <- met_combined |> 
    dplyr::filter(date >= reference_date,
                  date <= end_date) |> # forecast period only
    pivot_longer(cols = precip:doy, names_to = 'variable', values_to = 'value') |> 
    reframe(.by = c('parameter','date', 'variable'),
            value = median(value)) |> 
    pivot_wider(names_from = variable, values_from = 'value') #|> 
    # left_join(upstream, by = c('date')) # combine with upstream
  
  
  ## RUN PREDICTIONS
  message('Running xgboost forecast')
  
  # Apply function for temperature variables
  temp_training_df <- training_df |> 
    filter(variable == "TEMP", inflow_name == 'murray') |> 
    select(-precip) |> 
    # add in a lagged predictor of lag(obs)
    mutate(lag_obs = lag(observation, 1)) |> 
    select(any_of(c('date', 'precip', 'temperature', 'threeday_temp', 'doy', 'observation', 'lag_obs')))
  
  temp_rec <- recipe(observation ~ lag_obs + doy + threeday_temp + temperature,
                     data = temp_training_df)
  
  
  # forecast_drivers <- met_df |>
  #   left_join(targets_df, by = c('date')) |>
  #   drop_na(total_flow)
  #
  # split <- initial_split(forecast_drivers, prop = 0.80, strata = NULL)
  #
  # train_data <- training(split)
  # test_data <- testing(split)
  
  ## set training as all data prior to start of forecast
  # train_data <- forecast_drivers |>
  #   dplyr::filter(date < reference_datetime)
  
  
  ## define folds in training data
  folds <- vfold_cv(temp_training_df, v = 5) # orginally set to 10
  
  # #set the recipe
  # rec <- recipe(total_flow ~ precip + sevenday_precip + doy + temperature,
  #               data = train_data)
  #
  
  rec_preprocess <- temp_rec |>
    step_normalize(all_numeric_predictors()) #|>
  #step_dummy(doy)
  
  ## define model and tunining parameters (tuning 2/8 parameters right now)
  xgboost_mod <- boost_tree(tree_depth = tune(), trees = tune()) |> #, learn_rate = tune()) |>
    set_mode("regression") |>
    set_engine("xgboost")
  
  # define the model workflow
  xgboost_inflow_wkflow <-
    workflow() %>%
    add_model(xgboost_mod) %>%
    add_recipe(rec_preprocess)
  
  # tune the hyper-parameters
  inflow_resample_fit <- xgboost_inflow_wkflow |>
    tune_grid(resamples = folds,
              grid = 25,
              control = control_grid(save_pred = TRUE),
              metrics = metric_set(rmse))
  
  # show the results from tuning
  inflow_resample_fit %>%
    collect_metrics() |>
    arrange(mean)
  
  # select the best tuned hyper-parameters
  best_hyperparameters <- inflow_resample_fit %>%
    select_best(metric = "rmse")
  
  final_wrorkflow <- xgboost_inflow_wkflow |>
    finalize_workflow(best_hyperparameters)
  
  ## fit the model (using all available data (past and future) for now but could just use training data)
  #xgboost_inflow_fit <- fit(final_wrorkflow, data = drivers_df)
  xgboost_inflow_fit <- fit(final_wrorkflow, data = temp_training_df)
  
  
  
  #make empty dataframe to store predictions
  temp_forecast <- tail(temp_training_df, 1) |> 
    rename(prediction = observation) |> 
    mutate(parameter = 0) |> 
    select(date, prediction, parameter)
  
  forecast_dates <- seq.Date(as_date(reference_date), length.out = horizon, by = 1)
  
  for (i in 1:length(forecast_dates)) {
    
    previous_prediction <- temp_forecast |> filter(date == forecast_dates[i] - days(1)) |> pull(prediction)
    
    single_date <- dplyr::filter(forecast_df, date == forecast_dates[i]) |> 
      mutate(lag_obs = previous_prediction)
    
    ens_inflow <- predict(xgboost_inflow_fit, new_data = single_date)
    
    ens_predictions <- bind_cols(single_date, ens_inflow) |>
      rename(prediction = .pred) |>
      mutate(prediction = ifelse(prediction < 0, 0, prediction)) |>
      select(date, prediction, parameter)
    
    temp_forecast <- bind_rows(temp_forecast, ens_predictions)
    
  }
  
  
  temp_fc <- temp_forecast |> 
    filter(date >= as_date(reference_date)) |> 
    mutate(datetime = as_date(date),
           reference_date = reference_date,
           model_id = 'xgboost_temp',
           variable = 'TEMP',
           flow_number = 1) |> 
    select(any_of(c('datetime', 'prediction', 'reference_date', 
                    'model_id', 'variable', 'flow_number', 'parameter')))
  
  return(temp_fc)
}