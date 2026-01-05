# solution for milestone 8
# this script pulls data and saves having to share a big rds file
# 
# 

  all_lesions <- c("dd", "footrot", 'wld', "sole_ulcer","injury", 
                   "cork", "other", "hemorrhage",
                   "sole_fracture", "toe_ulcer", "thin",
                   "inf", "noninf", "toe", "axial", "lesion")
  
  diseases <- c("inf", "noninf", "lesion")
  
  solution <- events_formatted |> 
    select(id_animal, date_birth, date_fresh,lact_number, lact_group, 
           date_archived, date_event,
           event, 
           remark_letters1, protocols_letters1, location_event) |> 
    filter(event %in% c("LAME", "TRIM")) |>
    fxn_code_lesions(protocol_var = protocols_letters1,
                     remark_var = remark_letters1) |> 
    # fix ab and NA issues
    filter(!is.na(trimonly)) |>
    mutate(noninf = case_when(
      other == 1 & str_detect(protocols_letters1, "Ab") ~ 1,
      .default = noninf),
      # remove other catergory if abscess is found
      other = case_when(
        other == 1 & str_detect(protocols_letters1, "Ab") ~ 0,
        .default = other)
    ) |> 
    fxn_collapse_lesions(lesions = all_lesions) |> 
    fxn_trim_vars(location_event, id_animal,
                  date_var = date_event,
                  trimonly_var = trimonly) |> 
    fxn_dz_status(disease_cols = diseases,
                  event_filter = trimonly == 0) |> 
    # join with animals to get left date
    left_join(animals |> 
                select(id_animal, date_sold, date_died, date_left),
              by = "id_animal") |> 
    # add cull data
    mutate(
      # create culling variables
      culled_ever = if_else(is.na(date_left), 0, 1),
      days_to_cull = date_left - date_event,
      # temp censor date need to adjust
      date_censor = ymd(date_max_pull) 
    ) |> 
    mutate(days_to_censor_cull = case_when(culled_ever == 1 ~
                                             days_to_cull,
                                           # cows not culled
                                           culled_ever == 0 ~
                                             date_censor - 
                                             date_event,
                                           .default =  NA
    ),
    across(starts_with("days"), as.numeric)
    ) |> 
    group_by(location_event, id_animal) |> 
    mutate(culled_last_trim = 
             if_else(culled_ever == 1 & !is.na(days_to_next_trim), 0, 
                     culled_ever),
           culled_30 = if_else(culled_ever == 1 & 
                                 days_to_cull <=30, 1, 0
           )
    )|>
    ungroup() |> 
    select(location_event, id_animal, date_birth, lact_number, lact_group, 
           date_event,
           date_left, date_died, date_sold,
           lesion, inf, noninf, trimonly, times_trimmed, date_censor, 
           days_to_cull, days_to_censor_cull,culled_ever, culled_last_trim,
           culled_30, date_prev_trim, days_to_next_trim, 
           starts_with("life_"), starts_with("status_")
    )
  
