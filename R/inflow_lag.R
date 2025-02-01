generate_flow_lag <- function(config, 
                              upstream_lag,
                              upstream_data = 'QSA') {
  
  if (upstream_lag > 10 & upstream_data == 'QSA') {
    message('You are using a lag greater than 10 days between QSA and the observatio, which is not recommended')
    
  } 
  
  if (upstream_lag > config$run_config$forecast_horizon) {
    stop('Cannot use a lag greater than horizon')  
  } else {
    
    if (upstream_data == 'QSA') {
      upstream <- read_csv(paste0("https://water.data.sa.gov.au/Export/BulkExport?DateRange=Custom&StartTime=2020-01-01%2000%3A00&EndTime=", Sys.Date(), "%2000%3A00&TimeZone=0&Calendar=CALENDARYEAR&Interval=Daily&Step=1&ExportFormat=csv&TimeAligned=True&RoundData=True&IncludeGradeCodes=False&IncludeApprovalLevels=False&IncludeQualifiers=False&IncludeInterpolationTypes=False&Datasets[0].DatasetName=Discharge.Master--Daily%20Calculation--ML%2Fday%40A4261001&Datasets[0].Calculation=Minimum&Datasets[0].UnitId=239&_=1731560927692"),
                           skip = 5, col_names = c('datetime', 'end', 'QSA'), show_col_types = F) |> 
        select(datetime, QSA) |> 
        mutate(QSA = log(QSA), 
               QSA_lag := lag(QSA, n = upstream_lag))  |> 
        # dplyr::filter(datetime < reference_date) |> 
        dplyr::slice(upstream_lag+1:n()) |> # removes the rows without a lag value
        mutate(QSA_lag = imputeTS::na_interpolation(QSA_lag)) |> 
        select(datetime, QSA_lag, QSA)
      
      lag_prediction <- upstream |> 
        rename(prediction = QSA_lag) |> 
        select(datetime, prediction)
      
      return(lag_prediction)
    }
    
  }
 
}
