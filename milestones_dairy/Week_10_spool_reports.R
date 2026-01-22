library(quarto)
library(purrr)

reports <- c("2108e", "2755f")

walk(
  reports,
  ~ quarto_render(
    input = "C:/GitHub/2025_fall_posit_course/milestones_dairy/milestone_week_10_publish_mark.qmd",
    execute_params = list(herd = .x),
    output_file = paste0("report_", .x, ".html")
  )
)
