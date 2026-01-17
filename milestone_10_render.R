# code to render the 4 documents


library(tidyverse)
library(quarto)
library(janitor)

# 1. Define your inputs
years <- c(2024, 2025)
farms <- c("Herd 2108e", "Herd 2755f")

# 2. Create a "Job Table"
# This ensures every year is paired with every farm automatically
render_jobs <- expand_grid(year = years, farm = farms)

# 3. Use pwalk to execute the renders
pwalk(render_jobs, function(year, farm) {
  
  # Prepare a clean filename
  # make_clean_names converts "Herd 2108e" to "herd_2108e"
  clean_farm <- make_clean_names(farm)
  file_name <- str_glue("farm_report_{year}_{clean_farm}.html")
  
  message(str_glue("Currently rendering: {file_name}..."))
  
  quarto_render(
    input = "milestones_dairy/milestone_week_10_publish.qmd",
    output_file = file_name,
    execute_params = list(
      year = year, 
      herd = farm
    )
  )
})