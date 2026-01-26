# SETUP-----------------------------
#**** Modify This Section***

get_EXAMPLE_herds <- 2 #(0-8)
#number of Parnell Example herds you want to process.
#if this is set to 0, you need to put your own data in the event_files folder

milk_data_exists <- FALSE #are there files in the milk_files folder that you want to process?
auto_de_duplicate <- TRUE #do you want to de-duplicate rows in the event files? 
#(choose FALSE if there are treatments that happen more than once daily that you want to capture)

## day of phase parameters-----------------------------------
#set the parameters for grouping by DIM or heifers by days of age
set_cut_by_days = 100 #number of days in each group
set_top_cut = 400 #the final group for cow DIM with be this number and anything higher
set_top_cut_hfr = 700 #the final group for heifer days of age with be this number and anything higher

## denominator granularity-----------------------
#Create a list of time periods (number of days) by which denominators will be created.  
#The standard options are 21 and 365.  However any number works.
#You can add or delete as you wish, except for yearly. Yearly needs to stay
#the more time periods you add to this list the longer it will take to process files
denominator_time_periods<-c(#21, 
                            365) #do NOT delete the yearly option or you will break the data_dictionary


# PROCESS FILES--------------------------
## read in functions -------------------
source('functions/fxn_pacman.R')
source("functions/fxn_delete_files.R")

## process files ----------
source('functions/fxn_process_files.R')

# GENERATE REPORTS ----------------
source('functions/fxn_pacman.R')
fxn_pacman_all()

## Gerard's lameness report ---------------------------
# quarto::quarto_render("step3_create_denominators_lact_dim_season.qmd") # denominators for lameness report
# quarto::quarto_render("report_explore_lame.qmd")

## "HOW TO" reports ---------------------------
 quarto::quarto_render("report_how_to_use_denominators.qmd")

## quick check data reports--------------------------------
 quarto::quarto_render("report_explore_event_types.qmd")
 quarto::quarto_render("report_data_dictionary.qmd")


#******************************************************************************
#*******************************************************************************
#CLEAN SLATE --------------
#  source("functions/fxn_delete_files.R")
# fxn_delete_files_clean_slate() #delete ALL original event data and  processed data
#****************************************************************************
#***************************************************************************


# FUTURE STUFF ---------------------------

# quarto::quarto_render('step3_report_disease_template.qmd')
# quarto::quarto_render('animal_counts.qmd')
# cohort disease incidence (Location, Lactation, Breed, etc)
# timing of disease (DIM (or Age) and calendar time distributions, Kaplan Meier)
# perfomrance and disease (milk, gain, repro)

# old stuff
# source('step2disease_create_intermediate_files.R') #under development #disease files


# TODO List --------------------------------------------
# add milk data for example farms
