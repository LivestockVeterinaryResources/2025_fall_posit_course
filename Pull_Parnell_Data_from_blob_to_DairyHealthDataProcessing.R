

library(tidyverse)
library(jsonlite)
library(AzureStor)
library(dtplyr)
library(odbc)
library(DBI)

#****WARNING: clear the "data/event_files" folder prior to running this******
source('../ParnellFunctions/utils.R')

# load environment variables from .env file
tryCatch({
  log_msg("###### Attempting to load .env locally ###### ")
  dotenv::load_dot_env(file = '../.env')
  log_msg("###### Successfully loaded .env locally ###### ")
}, error = function(e) {
  log_msg("###### No local .env file found, proceeding without it. ###### ")
})

# Connect to database ----------------------
sql_conn_str<-Sys.getenv("mySYNCHSqlDataConnection") 
con <- dbConnect(odbc::odbc(), .connection_string = sql_conn_str) 



# get the herd table
herd <- dbGetQuery(con, "SELECT * FROM Herd")|>
  tibble() |> 
  mutate(HerdName = Name)|>
  select(HerdKey, HerdId, FarmId, HerdName, HerdMgmtSoftware)

#get the farm table
farm <- dbGetQuery(con, "SELECT * FROM Farm")|>
  tibble() |> 
  mutate(FarmName = Name)|>
  select(IsTestFarm, IsExtinct, Status, FarmId, FarmName, contains('Address'), City, State, PostalCode, Country)

herd_farm_list<<-herd%>%
  left_join(farm)%>%
  filter(IsTestFarm<1)%>%
  filter(IsExtinct<1)%>%
  filter(Status < 2)


#authenticate - Prod--------------------------

#keys
# acess<-read_json('../mysynchstorage.json')
storage_account_name <- Sys.getenv("AZURE_V1_STORAGE_ACCOUNT_NAME")
storage_account_key <- Sys.getenv("AZURE_V1_STORAGE_ACCOUNT_KEY")

# various endpoints for an account: blob, file, ADLS2
# bl_endp_key <- storage_endpoint("https://mysynchstorage.blob.core.windows.net", key=acess$mysynchstorage$key[[1]])
bl_endp_key <- storage_endpoint(
  paste0("https://", storage_account_name, ".blob.core.windows.net"),
  key = storage_account_key
)



#listing containers-------------------------------

# example of working with containers (blob storage)
list_containers<-list_storage_containers(bl_endp_key)

set_container<-'clientfile'


#get herd keys and add them to date fetch list------------------------------

## choose herds -----------------
#Make this the list of herds you want
selected_herd<-herd_farm_list%>%
  filter(HerdId %in% c('725', #Pagels
                       '726' #Dairy Dreams
                       )
         )%>%
  mutate(file_prefix = str_to_lower(HerdKey))
  


## choose farm data -----------------
farms_to_get<-c(selected_herd$file_prefix)

#Loop to get files-------------------------
#k=1
for (k in seq_along(farms_to_get)){
  
  #file_path_herd_key<- '4cfbbdc2-d892-424b-b3e9-cff09e1c3ee8/202411042230-Absorb-Less' #prefix in azure storage stage/clientfile 
  
  file_path_herd_key<- farms_to_get[[k]]  #prefix in azure storage stage/clientfile 
  
  
  find_date<-tibble(path = list_blobs(container = list_containers[['clientfile']], 
                                      dir = "/",
                                      info = 'name',
                                      #info = 'all',
                                      
                                      prefix = file_path_herd_key)
  )%>%
    mutate(get_stringdate = str_sub(path, 38, 49), 
           HerdKey_lower = str_sub(path, 1, 36))%>%
    arrange(HerdKey_lower, get_stringdate)%>%
    group_by(HerdKey_lower)%>%
    mutate(flag = max(get_stringdate))%>%
    ungroup()%>%
    filter(flag %in% get_stringdate)%>%
    
    #check<-find_date%>%
      select(HerdKey_lower, get_stringdate)%>%
      distinct()%>%
      arrange(HerdKey_lower, desc(get_stringdate))%>%
      mutate(flag_first = first(get_stringdate))%>%
      filter(flag_first == get_stringdate)%>%
      mutate(file_path_herd_key = paste0(HerdKey_lower, '/', flag_first))
  
  test_list<-tibble(path = list_blobs(container = list_containers[['clientfile']], 
                                      dir = "/",
                                      info = 'name',
                                      #info = 'all',
                                      
                                      prefix = find_date$file_path_herd_key[[1]])
  )%>%
    filter(str_detect(path, '.myce\\d'))
  
  #i=1
  
  for (i in seq_along(test_list$path)){
    
    fetch_file<-tibble(fetch_file_name = paste0(test_list$path[[i]]))%>%
      mutate(dest_file_name = str_replace_all(fetch_file_name, '/', '_'))
    
    
    download_blob(list_containers[["clientfile"]], 
                  paste0(fetch_file$fetch_file_name[[1]]),
                  dest=paste0('data/event_files/',  fetch_file$dest_file_name[[1]]), overwrite = TRUE)
                  #dest=paste0('../DairyDataHealthProcessing/data/event_files/',  fetch_file$dest_file_name[[1]]), overwrite = TRUE)
}
  
  
}


#check files to see if there are 26 for each herd-------------------------
check_files<-list.files('data/event_files')

