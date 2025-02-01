#' Generate an xgboost inflow forecast
#'
#' @param training_df a dataframe with data needed to train the model (use generate_training_df).
#' @param use_recipe a tidy models recipe
#' @param forecast_df a dataframe with the data needed to predict - same as in training_df (use generate_forecast_df)
#' @param transform_function what transformations should be applied
#' @param save_model logical. Save model fit object? Default  = F
#' @param refit_model logical. Refit the model? Otherwise use saved object file. Default = T 
#' @param save_loc where should the model be saved?
#' @return A dataframe.

generate_xgb_flow <- function(training_df,
                              use_recipe = 'all',
                              forecast_df,
                              transform_function = 'none',
                              refit_model = TRUE,
                              save_model = FALSE, 
                              save_loc = '.') {
  
  horizon <- forecast_df |> ungroup() |>  distinct(date) |> nrow()
  reference_date <- forecast_df |> ungroup() |> summarise(reference_date = min(date)) |> pull()
  variable <- unique(training_df$variable)
  inflow_name <- unique(training_df$inflow_name)
  
  all_dates <- data.frame(date = seq.Date(as_date(min(training_df$date)), as_date(max(training_df$date)), 'day'))
  
  # set up checks
  if (length(variable) != 1) {
    stop('need unique variable and inflow_name')
  }
  
  if (sum(str_detect(colnames(training_df), 'inflow_name')) == 1) {
    if (length(inflow_name) != 1) {
      stop('need unique variable and inflow_name')
      
    }
  }
  
  if (!is.function(transform_function)) {
    message("using only data in training df, no lags etc. applied")
    transform_function  <- function(data) {data}
  }
  
  # Set up data
  all_data <- training_df |> 
    select(-tidyr::any_of(c('inflow_name', 'site_id', 'variable'))) |> 
    # mutate(observation = log(observation),
    #        precip = log(precip + 1)) |> 
    arrange(date) |> 
    full_join(all_dates, by = 'date') |> 
    ungroup() |> 
    future_frame(.length_out = horizon, 
                 .date_var = 'date',
                 .bind_data = T)
  
  # Apply transformations
  all_data_lags <- all_data |> 
    transform_function()
  
  # Drop any lines for training that contain NAs (includes future period)
  data_lags <- all_data_lags |> 
    drop_na()
  
  # Split into training and testing (including calibration)
  train_data <- data_lags[1:round((nrow(data_lags)*0.8)),]
  test_data <- data_lags[round(nrow(data_lags)*0.8):nrow(data_lags),]
  
  # Use a saved model fit?
  if (!refit_model) {
    xgboost_inflow_fit <- read_rds(file.path(save_loc, paste0('fitted_model_',variable, '_', inflow_name, '.rds')))
    message("using a saved prefitted model: ", file.path(save_loc, paste0('fitted_model_',variable, '_', inflow_name, '.rds')))
  } else {
    message("refitting model")
    # Set recipe
    if (is_character(use_recipe)) {
      if (use_recipe == 'all') {
        use_recipe <- recipe(observation ~ ., data = train_data) |> 
          step_rm('date')
        message("using default recipe")
        print(use_recipe$var_info)
        print(use_recipe$steps)
      }
      
    } else if (class(use_recipe) == "recipe") {
      message('using provided recipe')
      print(use_recipe$var_info)
      print(use_recipe$steps)
    }
    
    
    ## define folds in training data 
    folds <- vfold_cv(data_lags, v = 5) # orginally set to 10
    
    ## define model and tunining parameters (tuning 2/8 parameters right now)
    xgboost_mod <- boost_tree(tree_depth = tune(), trees = tune(), mtry = tune()) |> #, learn_rate = 0.1) |> 
      set_mode("regression") |>  
      set_engine("xgboost")
    
    # define the model workflow
    xgboost_inflow_wkflow <- 
      workflow() %>% 
      add_model(xgboost_mod) %>% 
      add_recipe(use_recipe)
    
    # tune the hyper-parameters
    inflow_resample_fit <- xgboost_inflow_wkflow |> 
      tune_grid(resamples = folds, 
                grid = 25, 
                control = control_grid(save_pred = TRUE),
                metrics = metric_set(rmse))
    
    
    # select the best tuned hyper-parameters
    best_hyperparameters <- inflow_resample_fit %>%
      select_best(metric = "rmse")
    
    final_workflow <- xgboost_inflow_wkflow |> 
      finalize_workflow(best_hyperparameters)
    
    ## fit the model 
    xgboost_inflow_fit <- fit(final_workflow, data = train_data) |> 
      recursive(transform = transform_function,
                train_tail = tail(train_data, n = 10)) # NEEDS TO BE AT LEAST AS LONG AS THE LONGEST LAG
    
    if (save_model) {
      write_rds(xgboost_inflow_fit,file.path(save_loc, paste0('fitted_model_',variable, '_', inflow_name, '.rds')))
      message("Saving refitted model: ", file.path(save_loc, paste0('fitted_model_',variable, '_', inflow_name, '.rds')))
    } 
    
  }
  
  # Add to a modeltime table
  model_tbl <- modeltime_table(xgboost_inflow_fit)
  
  # # Calibrate model to testing set
  cal_tbl <- model_tbl |>
    modeltime_calibrate(new_data = test_data, quiet = FALSE)
  
  # make predictions for each ensemble member 
  #make empty dataframe to store predictions
  data_build <- NULL
  
  for (i in unique(forecast_df$parameter)){
    
    ens_df <- forecast_df |> 
      dplyr::filter(parameter == i) |> 
      dplyr::ungroup() |> 
      select(-parameter)
    
    future_data <- 
      all_data_lags |> 
      select(-any_of(c('precip', 'temperature', 'doy'))) |> 
      right_join(ens_df, by = 'date') |> 
      
      bind_rows(tail(test_data, n = 5)) |> 
      arrange(date) |> 
      transform_function() |> 
      filter(is.na(observation))
    
    ens_prediction <- cal_tbl |> # uses the calibrated model
      modeltime_forecast(new_data = future_data) |> 
      rename(prediction = .value,
             date = .index) |> 
      mutate(parameter = i)
    
    
    
    
    data_build <- bind_rows(data_build,ens_prediction) |> 
      arrange(date, parameter)
    
  }
  
  xgb_forecast <- data_build |> 
    rename(datetime = date) |> 
    select(any_of(c('model_id', 'site_id', 'reference_datetime', 'datetime', 'family', 'parameter', 'variable', 'prediction', 'flow_type', 'flow_number')))
  
  return(xgb_forecast)
}

#' Apply transofrmationss such as lags and rolling window sums to flow dataset
#'
#' @param data 
#' @return A dataframe with the transformation column
apply_lags_flow <- function(data) {
  
  data |> 
    tk_augment_lags(observation, .lags = 1) |> 
    tk_augment_lags(precip, .lags = 1) |> 
    tk_augment_slidify(precip, .align = 'right',
                       .f = ~sum(.x, na.rm = T),
                       .period = 3,
                       .partial = F) |> 
    # tk_augment_lags(QSA, .lags = 35) |> 
    ungroup()
}

#' Apply transformation such as lags and rolling window sums to salt dataset
#'
#' @param data 
#' @return A dataframe with the transformation column
apply_lags_salt <- function(data) {
  
  data |> 
    tk_augment_lags(observation, .lags = 1) |> 
    # tk_augment_lags(precip, .lags = 1) |> 
    # tk_augment_slidify(precip,
    #                    .f = ~sum(.x, na.rm = T),
    #                    .period = 3,
    #                    .partial = T) |> 
    ungroup()
}

#' Apply transformation such as lags and rolling window sums to salt dataset
#'
#' @param data 
#' @return A dataframe with the transformation column
apply_lags_temp <- function(data) {
  
  data |> 
    tk_augment_lags(observation, .lags = 1) |> 
    # tk_augment_lags(precip, .lags = 1) |> 
    # tk_augment_slidify(precip,
    #                    .f = ~sum(.x, na.rm = T),
    #                    .period = 3,
    #                    .partial = T) |> 
    ungroup()
}



#' Generate training_df for use in generate_xgb_flow
#'
#' @param config flare configuration
#' @param met_vars which NOAA variables are needed
#' @param flow_obs filepath of the location of historical flow targets
#' @param training_years how many years should be used in training
#' @return A dataframe.

generate_training_df <- function(config, 
                                 met_vars = c("precipitation_flux", "air_temperature"), 
                                 use_upstream = F, # obtain data from upstream locations for drivers
                                 upstream_lag = 35, 
                                 flow_obs, 
                                 training_years = 5) {
  # Set up
  reference_date <- as_datetime(config$run_config$forecast_start_datetime)
  start_training <- reference_date - lubridate::days(training_years * 365)
  site_id <- config$location$site_id
  
  # Get historic met ----------------
  met_s3_historical <- arrow::s3_bucket(paste0("bio230121-bucket01/flare/drivers/met/gefs-v12/stage3/site_id=",site_id),
                                        endpoint_override = config$s3$drivers$endpoint,
                                        anonymous = TRUE) |> 
    arrow::open_dataset() |> 
    dplyr::filter(variable %in% met_vars,
                  ((datetime <= reference_date  & variable == "precipitation_flux") |
                     datetime < reference_date  & variable == "air_temperature"),
                  datetime > start_training) |>
    collect() |> 
    mutate(variable = ifelse(variable == "precipitation_flux", "precipitation", variable),
           variable = ifelse(variable == "air_temperature", "temperature_2m", variable),
           prediction = ifelse(variable == "temperature_2m", prediction - 273.15, prediction)) |> 
    select(-reference_datetime)
  
  # Get historic flow observations ---------------
  inflow_targets <- read_csv(flow_obs, show_col_types = FALSE) |> 
    dplyr::filter(datetime < reference_date) |> 
    rename(date = datetime) |>
    # pivot_wider(names_from = variable,
    #             values_from = observation) |> 
    group_by(site_id, inflow_name, variable) |> 
    mutate(observation = imputeTS::na_interpolation(observation))
  
  # Get upstream observations ---------------
  if (use_upstream) {
    message('Using upstream data')
    upstream <- read_csv(paste0("https://water.data.sa.gov.au/Export/BulkExport?DateRange=Custom&StartTime=2020-01-01%2000%3A00&EndTime=", Sys.Date(), "%2000%3A00&TimeZone=0&Calendar=CALENDARYEAR&Interval=Daily&Step=1&ExportFormat=csv&TimeAligned=True&RoundData=True&IncludeGradeCodes=False&IncludeApprovalLevels=False&IncludeQualifiers=False&IncludeInterpolationTypes=False&Datasets[0].DatasetName=Discharge.Master--Daily%20Calculation--ML%2Fday%40A4261001&Datasets[0].Calculation=Minimum&Datasets[0].UnitId=239&_=1731560927692"),
             skip = 5, col_names = c('date', 'end', 'QSA'), show_col_types = F) |> 
      select(date, QSA) |> 
      mutate(QSA = log(QSA), 
             QSA_lag := lag(QSA, n = upstream_lag))  |> 
      dplyr::filter(date < reference_date) |> 
      dplyr::slice(upstream_lag+1:n()) |> # removes the rows without a lag value
      mutate('QSA_lag_{upstream_lag}' := imputeTS::na_interpolation(QSA_lag)) |> 
      select(date, glue::glue('QSA_lag_{upstream_lag}'))
  } else {
    upstream <- data.frame(date = unique(inflow_targets$date))
  }
  
  
  # Generate training df -----------
  training_df <- met_s3_historical |> 
    filter(datetime <= reference_date) |> 
    reframe(pred_hourly = median(prediction, na.rm = T), .by = c("datetime", "variable")) |> # only want a median from the ensemble members
    pivot_wider(names_from = variable, values_from = pred_hourly) |> 
    mutate(date = lubridate::as_date(datetime)) |> 
    reframe(precip = sum(precipitation, na.rm = TRUE), # what is the total per day
            temperature = median(temperature_2m, na.rm = TRUE), # what is the average temperature per day
            .by = c("date")) |> 
    mutate(#fifteen_precip = RcppRoll::roll_sum(precip, n = 3, fill = NA,align = "right"), # calculate a 15-day rolling sum
      #threeday_temp = RcppRoll::roll_sum(temperature, n = 3, fill = NA, align = "right"), # calculate a 3-day rolling sum
      doy = lubridate::yday(date)) |> 
    left_join(inflow_targets, by = c('date')) |> # combine with the inflow observations
    left_join(upstream, by = c('date')) |>  # combine with upstream
    dplyr::filter(date < reference_date)
  
  return(training_df)
}

#' Generate training_df for use in generate_xgb_flow
#'
#' @param config flare configuration
#' @param met_vars which NOAA variables are needed
#' @return A dataframe.
generate_forecast_df <- function(met_vars,
                                 use_upstream = F,
                                 upstream_lag = 35,
                                 config) {
  
  # Set up
  reference_date <- as_datetime(config$run_config$forecast_start_datetime)
  noaa_date <- reference_date - lubridate::days(1)
  site_id <- config$location$site_id
  
  horizon <- config$run_config$forecast_horizon
  end_date <-  config$run_config$end_datetime
  
  if (is.na(end_date) & !is.na(horizon)) {
    end_date <- reference_date + days(horizon)
  } else if (!is.na(end_date) & is.na(horizon)) {
    horizon <- as.numeric(end_date - reference_date)
  }
  
  met_s3_future <- arrow::s3_bucket(file.path("bio230121-bucket01/flare/drivers/met/gefs-v12/stage2", 
                                              paste0("reference_datetime=",
                                                     noaa_date),
                                              paste0("site_id=",
                                                     site_id)),
                                    endpoint_override = config$s3$drivers$endpoint,
                                    anonymous = TRUE) |> 
    arrow::open_dataset() |> 
    dplyr::filter(variable %in% met_vars) |> 
    collect() |> 
    mutate(variable = ifelse(variable == "precipitation_flux", "precipitation", variable),
           variable = ifelse(variable == "air_temperature", "temperature_2m", variable),
           prediction = ifelse(variable == "temperature_2m", prediction - 273.15, prediction)) |> 
    filter(datetime > reference_date - days(16)) |> # need up to 15 days before to calculate the rolling window
    pivot_wider(names_from = variable, values_from = prediction) |> 
    mutate(date = lubridate::as_date(datetime)) |> 
    reframe(precip = sum(precipitation, na.rm = TRUE), # what is the total per day
            temperature = median(temperature_2m, na.rm = TRUE), # what is the average temperature per day
            .by = c("date", "parameter")) # retain the ensemble members
  
  # Get upstream observations ---------------
  if (use_upstream) {
    if (upstream_lag >= horizon) {
      upstream <- read_csv(paste0("https://water.data.sa.gov.au/Export/BulkExport?DateRange=Custom&StartTime=2020-01-01%2000%3A00&EndTime=", Sys.Date(), "%2000%3A00&TimeZone=0&Calendar=CALENDARYEAR&Interval=Daily&Step=1&ExportFormat=csv&TimeAligned=True&RoundData=True&IncludeGradeCodes=False&IncludeApprovalLevels=False&IncludeQualifiers=False&IncludeInterpolationTypes=False&Datasets[0].DatasetName=Discharge.Master--Daily%20Calculation--ML%2Fday%40A4261001&Datasets[0].Calculation=Minimum&Datasets[0].UnitId=239&_=1731560927692"),
                           skip = 5, col_names = c('date', 'end', 'QSA'), show_col_types = F) |> 
        select(date, QSA) |> 
        mutate(QSA = log(QSA), 
               QSA_lag := lag(QSA, n = upstream_lag)) |> # gernate the lag (upstream - downstream)
        dplyr::filter(as_date(date) >= as_date(reference_date)) |> 
        mutate('QSA_lag_{upstream_lag}' := imputeTS::na_interpolation(QSA_lag)) |> 
        select(date, glue::glue('QSA_lag_{upstream_lag}'))
    } else {
      stop('Horizon is longer than the lag from the upstream data. Use a longer lag or a shorter horizon.')
    }
  } else {
    upstream <- data.frame(date = unique(met_s3_future$date))
  }
  
  # Generate forecast df ------------
  forecast_df <- met_s3_future |> 
    mutate(#fifteen_precip = RcppRoll::roll_sum(precip, n = 3, fill = NA,align = "right"), # calculate a 15-day rolling sum
            #threeday_temp = RcppRoll::roll_sum(temperature, n = 3, fill = NA, align = "right"), # calculate a 3-day rolling sum
            doy = lubridate::yday(date)) |> 
    left_join(upstream, by = c('date')) |>  # combine with upstream
    filter(date >= reference_date, 
           date <= end_date) 
  
  return(forecast_df)
}
