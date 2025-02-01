set.seed(100)
readRenviron("~/.Renviron") # MUST come first
library(tidyverse)
library(lubridate)
lake_directory <- here::here()
setwd(lake_directory)
forecast_site <- "ALEX"
configure_run_file <- "configure_run.yml"
config_set_name <- "glm_flare_v3"

fresh_run <- TRUE

Sys.setenv("AWS_DEFAULT_REGION" = "renc",
           "AWS_S3_ENDPOINT" = "osn.xsede.org",
           "USE_HTTPS" = TRUE,
           'GLM_PATH' = 'GLM3r')

message("Checking for NOAA forecasts")

config <- FLAREr::set_up_simulation(configure_run_file,lake_directory, config_set_name = config_set_name,
                                    clean_start = fresh_run)

# if(fresh_run) unlink(file.path(lake_directory, "restart", "ALEX", config$run_config$sim_name, configure_run_file))

# Generate targets
source(file.path('workflows', config_set_name, 'generate_targets.R'))

message("Successfully generated targets")

FLAREr:::put_targets(site_id =  config$location$site_id,
                     cleaned_insitu_file,
                     cleaned_met_file = NA,
                     cleaned_inflow_file,
                     use_s3 = config$run_config$use_s3,
                     config = config)

if(config$run_config$use_s3){
  message("Successfully moved targets to s3 bucket")
}
noaa_ready <- TRUE



while(noaa_ready){
  
  config <- FLAREr::set_up_simulation(configure_run_file,lake_directory, config_set_name = config_set_name)
  
  # Generate inflow/outflows
  source(file.path('workflows', config_set_name,'new_baseline_inflow_workflow.R')) 
  # combined flow drivers - assuming inflows lagged from upstream and persistence outflow

  # run FLARE forecast
  output <- FLAREr::run_flare(lake_directory = lake_directory,
                              configure_run_file = configure_run_file,
                              config_set_name = config_set_name)
  
  forecast_start_datetime <- lubridate::as_datetime(config$run_config$forecast_start_datetime) + lubridate::days(1)
  start_datetime <- lubridate::as_datetime(config$run_config$forecast_start_datetime) - lubridate::days(5) ## SET LONGER LOOK BACK FOR DATA
  restart_file <- paste0(config$location$site_id,"-", (lubridate::as_date(forecast_start_datetime)- days(1)), "-",config$run_config$sim_name ,".nc")
  
  FLAREr::update_run_config(lake_directory = lake_directory,
                            configure_run_file = configure_run_file,
                            restart_file = basename(output$restart_file),
                            start_datetime = start_datetime,
                            end_datetime = NA,
                            forecast_start_datetime = forecast_start_datetime,
                            forecast_horizon = config$run_config$forecast_horizon,
                            sim_name = config$run_config$sim_name,
                            site_id = config$location$site_id,
                            configure_flare = config$run_config$configure_flare,
                            configure_obs = config$run_config$configure_obs,
                            use_s3 = config$run_config$use_s3,
                            bucket = config$s3$restart$bucket,
                            endpoint = config$s3$restart$endpoint,
                            use_https = TRUE)
  
  #RCurl::url.exists("https://hc-ping.com/31c3e142-8f8c-42ae-9edc-d277adb94b31", timeout = 5)
  
  noaa_ready <- FLAREr::check_noaa_present(lake_directory,
                                           configure_run_file,
                                           config_set_name = config_set_name)
  
  
}
