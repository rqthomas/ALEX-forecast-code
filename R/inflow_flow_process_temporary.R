# This script is a simple "process" model that assumes a travel time (T) and a rate of loss (L) 
# Therefore the calculation of flow at W at time t is
# Qdown ~ Qup(t-T) - L
# L ~ Qup(t-T) + month (as per model output from DEW)
# Travel times are estimated based on observed outpyt from Q at SA border (QSA) and L1 
# plus some estimated time between L1 and Wellington (6 days)


library(fable)

#' model_losses
#' @description model the loss function based on QSA and month (the monthly loss in GL/m depending on the QSA in GL/d)
#' @returns dataframe with lagged predictors, model fitted on ML/d
#' @param T travel time or lag to be applied
#' @param data dataframe with the column to be lagged
#' @param upstream_col column name to be used as the upstream predictor
#' @param L_mod fitted loss model
#' @examples 

#'model_losses(model_dat = 'DEW_data/modelled_losses_DEW.csv',
#'             formula_use = "x ~ y + group", 
#'             x = 'loss', y = 'QSA', group = 'month')

model_losses <- function(model_dat = 'data_raw/modelled_losses_DEW.csv', 
                         # data used to fit model, in GL/m
                         formula_use = 'x ~ y + group',
                         x = 'loss', y = 'QSA', group = 'month') {

  model_loss <- 
    read_csv(model_dat, show_col_types = F) |> 
    mutate(month = match(month, month)) |> 
    pivot_longer(starts_with('GLd_'), 
                 names_to = y,
                 values_to = x,
                 names_prefix = 'GLd_') |> 
    mutate(days_in_month = lubridate::days_in_month(month),
           {{x}} := (as.numeric(.data[[x]])/days_in_month) * 1000, # convert to ML/d from GL/m
           {{y}} := (as.numeric(.data[[y]]))*1000, # convert to ML/d from GL/d
           {{group}} := as.factor(.data[[group]])) %>% 
    select(-days_in_month)
  
  # model_loss |> 
  #   ggplot(aes(x = qsa_flow, y = loss,colour = as.factor(month))) +
  #   geom_point() +
  #   geom_smooth(method = 'lm')
  
  # # fit model with and without interaction
  # anova(lm(loss ~ qsa_flow * month, data = model_loss),
  #       lm(loss ~ qsa_flow + month, data = model_loss))
  # 
  # no interaction better
  formula_updated <- gsub(pattern = "x", replacement = x, 
                          x = gsub(pattern = "y", replacement = y, 
                                   x = gsub(pattern = "group", replacement = group, x = formula_use)))
  
  L_mod <- lm(as.formula(formula_updated), data = model_loss)
  
  return(L_mod)
}

#' predict_downstream
#'
#' @returns dataframe with lagged predictors
#' @param lag_t travel time or lag to be applied (T)
#' @param data dataframe with the column to be lagged and a datetime column, requires explicit gaps
#' @param upstream_col column name to be used as the upstream predictor
#' @param L_mod fitted loss model
#' @examples
predict_downstream <- function(lag_t,
                               data, # needs a datatime column, data in ML/d, or specify units
                               upstream_col = 'QSA',
                               L_mod = L_mod) {
  
  
    new_dat <- data |>
    mutate(#"{upstream_col}" := data1[upstream_col],
      month = as.factor(month(datetime)))
  
  predicted_loss <- predict(L_mod, newdata = new_dat)
  
  
  upstream_lagged <- data |> 
    mutate(upstream_lag = lag(.data[[upstream_col]], lag_t)) # lagged_upstream (Qup(t-T)) 
  
  prediction <- data |> 
    select(datetime) |> 
    mutate(prediction = upstream_lagged$upstream_lag - predicted_loss)  # Qdown ~ Qup(t-T) - L
  
  return(prediction)
}


#' Title
#'
#' @param config
#' @param lag_t number of days to lag the upstream value 
#' @param use_QSA use QSA as the upstream location, only option at the moment
#' @param L_mod loss model object generated using model_losses()
#'
#' @returns forecast of inflow in MLd
#'
generate_flow_inflow_fc <- function(config,
                                    lag_t,
                                    upstream_unit = 'MLd',
                                    upstream_location = 'QSA',
                                    L_mod) {
  # Set up
  reference_date <- as_date(config$run_config$forecast_start_datetime)
  site_id <- config$location$site_id
  start_training <- reference_date - years(5)
  horizon <- config$run_config$forecast_horizon
  end_date <- reference_date + days(horizon)
  upstream_start <- reference_date - days(lag_t + 5) # give buffer
  
  # which upstream location to use
  if (!upstream_location %in% c('L1', 'QSA')) {
    stop('must use QSA or L1')
  } else if (upstream_location == 'QSA') {
    # read in recent QSA data - this is in MLd!!!
    download_WaterDataSA <- paste0("https://water.data.sa.gov.au/Export/BulkExport?DateRange=Custom&StartTime=",
                                   upstream_start, "%2000%3A00&EndTime=", reference_date, "%2000%3A00&TimeZone=9.5&Calendar=CALENDARYEAR&Interval=PointsAsRecorded&Step=1&ExportFormat=csv&TimeAligned=True&RoundData=True&IncludeGradeCodes=True&IncludeApprovalLevels=False&IncludeQualifiers=False&IncludeInterpolationTypes=False&Datasets[0].DatasetName=Discharge.Master--Daily%20Calculation--ML%2Fday%40A4261001&Datasets[0].Calculation=Instantaneous&Datasets[0].UnitId=241&_=1738250581759")
    download.file(download_WaterDataSA, destfile = 'data_raw/upstream.csv')
    upstream_MLd <- read_csv('data_raw/upstream.csv', show_col_types = F,
                             skip = 5, col_names = c('datetime', 'flow')) |> 
      mutate(datetime = ymd(format(datetime, "%Y-%m-%d")))
  } else {
    # read in recent L1 data - this is in MLd!!!
    download_WaterDataSA <- paste0("https://water.data.sa.gov.au/Export/BulkExport?DateRange=Custom&StartTime=",
                                   upstream_start, "%2000%3A00&EndTime=", reference_date, "%2000%3A00&TimeZone=9.5&Calendar=CALENDARYEAR&Interval=PointsAsRecorded&Step=1&ExportFormat=csv&TimeAligned=True&RoundData=True&IncludeGradeCodes=False&IncludeApprovalLevels=False&IncludeQualifiers=False&IncludeInterpolationTypes=False&Datasets[0].DatasetName=Discharge.Master--Daily%20Read--ML%2Fday%40A4260903&Datasets[0].Calculation=Instantaneous&Datasets[0].UnitId=241&_=1738251451037")
    download.file(download_WaterDataSA, destfile = 'data_raw/upstream.csv')
    upstream_MLd <- read_csv('data_raw/upstream.csv', show_col_types = F,
                             skip = 5, col_names = c('datetime', 'flow')) |> 
      mutate(datetime = ymd(format(datetime, "%Y-%m-%d")))
  }
  
  # convert units
  if (upstream_unit %in% c('m3s', 'MLd', 'GLd')) {
    if (upstream_unit == 'm3s') {
      # convert from m3/s to ML/d
      data <- data |>
        mutate("{upstream_col}" := (data[[upstream_col]] * 86.4))
    } else if (upstream_unit == 'GL/d') {
      # convert from GL/d to ML/d
      data <- data |>
        mutate("{upstream_col}" := data[[upstream_col]] * 1000)
    }
  } else {
    stop('units must be m3/s, ML/d or GL/d')
  }
    
    # Make sure the upstream_data extends as long as the forecast window - use persistence
    forecast_dates <- data.frame(datetime = seq.Date(reference_date, end_date, 'day'))
    all_upstream <- full_join(forecast_dates, upstream_MLd, by = 'datetime') |>
      arrange(datetime) |> 
      mutate(flow = zoo::na.locf(flow))
    
    # if (beyond_lag == 'ARIMA') {
    #   
    #   ARIMA_mod <- all_upstream |>
    #     mutate(flow = zoo::na.approx(flow, na.rm = F, rule = 1:2, maxgap = 5)) |>
    #     as_tsibble(index = 'datetime') |>
    #     na.omit() |> 
    #     model(ARIMA = ARIMA(flow))
    #   
    #   # calculate how long the horizon will be to estimate using the arima model
    #   arima_horizon <- horizon - lag_t
    #   
    #   ARIMA_fc <- ARIMA_mod |>
    #     forecast(h = arima_horizon) |>
    #     mutate(parameter = 1) |> 
    #     rename(flow = .mean) |>
    #     as_tibble()
    # }
    
    predictions <- predict_downstream(lag_t = lag_t, # need data to extend back at least this days before forecast date
                                      data = all_upstream,
                                      upstream_col = 'flow',
                                      L_mod = L_mod) |>
      filter(datetime %in% forecast_dates$datetime) |> 
      mutate(reference_date = as_date(reference_date),
             datetime = as_date(datetime),
             model_id = 'process_flow',
             variable = 'FLOW',
             flow_number = 1) |> 
      select(any_of(c('datetime', 'prediction', 'reference_date', 'model_id', 
                      'variable', 'flow_number', 'parameter'))) 
    
    
    return(predictions)
}
