# Meta --------------------------------------------------------------------

## Author:        Ian McCarthy
## Date Created:  2026-03-10
## Date Edited:   2026-03-10
## Description:   Parse NBER legacy Hospital Compare archives (nber-YYYYMM/).
##                Handles three naming eras plus zipped archives:
##                  Era 1 (2005-2014):  vwhqi_hosp.csv / dbo_vwhqi_hosp.csv
##                  Era 2 (2014-2016):  hqi_hosp.csv
##                  Era 3 (2015-2016):  hospital_general_information.csv (CMS-style)
##                  Zipped (2017-2025): hospitals_compare_YYYYMM.zip -> CMS-style names
##                Outputs: one CSV per topic area, stacked across all vintages.


# Paths -------------------------------------------------------------------

nber_dirs <- list.dirs("data/input/hospitals", recursive = FALSE)
nber_dirs <- nber_dirs[grepl("nber-\\d{6}$", nber_dirs)]
nber_dirs <- sort(nber_dirs)

if (length(nber_dirs) == 0) stop("No nber-YYYYMM folders found in data/input/hospitals/")

message(length(nber_dirs), " NBER vintage folders found")


# =========================================================================
# Step 0: Unzip any folders that only contain a .zip file
# =========================================================================

message("\n=== Checking for zipped vintages ===")
n_unzipped <- 0

for (d in nber_dirs) {
  zip_files <- list.files(d, pattern = "\\.zip$", full.names = TRUE)
  csv_files <- list.files(d, pattern = "\\.csv$", full.names = TRUE)

  # Only unzip if there are zip(s) but no CSVs yet

  if (length(zip_files) > 0 && length(csv_files) == 0) {
    for (zf in zip_files) {
      message("  Extracting: ", basename(d), "/", basename(zf))
      unzip(zf, exdir = d, overwrite = FALSE)
      n_unzipped <- n_unzipped + 1
    }
  }
}

message("Extracted ", n_unzipped, " zip archives")


# Helper: find file by pattern --------------------------------------------

find_file <- function(dir, patterns) {
  files <- list.files(dir, full.names = TRUE, ignore.case = TRUE)
  for (pat in patterns) {
    match <- files[grepl(pat, basename(files), ignore.case = TRUE)]
    if (length(match) >= 1) return(match[1])
  }
  return(NULL)
}


# Helper: safe read -------------------------------------------------------

safe_read <- function(file) {
  df <- tryCatch(
    fread(file, colClasses = "character", encoding = "UTF-8"),
    error = function(e) {
      tryCatch(
        fread(file, colClasses = "character", encoding = "Latin-1"),
        error = function(e2) {
          message("    Failed to read: ", basename(file), " -- ", e2$message)
          return(NULL)
        }
      )
    }
  )
  if (!is.null(df)) df <- scrub_encoding(df)
  df
}


# Helper: standardize column names ----------------------------------------

standardize_names <- function(df) {
  nms <- tolower(names(df))
  nms <- gsub("[^a-z0-9]+", "_", nms)
  nms <- gsub("^_|_$", "", nms)
  names(df) <- nms
  df
}


# Helper: scrub invalid UTF-8 from character columns ----------------------

scrub_encoding <- function(df) {
  chr_cols <- names(df)[sapply(df, is.character)]
  for (col in chr_cols) {
    df[[col]] <- iconv(df[[col]], from = "", to = "UTF-8", sub = "")
  }
  df
}


# Helper: rename columns via map, respecting existing targets -------------

apply_col_map <- function(df, col_map) {
  for (old_name in names(col_map)) {
    if (old_name %in% names(df)) {
      target <- col_map[old_name]
      if (!(target %in% names(df)) || old_name == target) {
        names(df)[names(df) == old_name] <- target
      }
    }
  }
  df
}


# =========================================================================
# 1. Hospital General Information
# =========================================================================

message("\n=== Parsing hospital general info ===")

hosp_info_list <- list()

for (d in nber_dirs) {
  vintage <- str_extract(basename(d), "\\d{6}")

  f <- find_file(d, c(
    "^dbo_vwhqi_hosp\\.csv$",
    "^vwhqi_hosp\\.csv$",
    "^hqi_hosp\\.csv$",
    "^hospital_general_information\\.csv$",
    "^Hospital General Information\\.csv$"
  ))

  if (is.null(f)) next

  df <- safe_read(f)
  if (is.null(df) || nrow(df) == 0) next

  df <- standardize_names(df)

  col_map <- c(
    "provider" = "facility_id",
    "provider_id" = "facility_id",
    "facility_id" = "facility_id",
    "prvdr_id" = "facility_id",
    "hospname" = "facility_name",
    "hospital_name" = "facility_name",
    "facility_name" = "facility_name",
    "state" = "state",
    "city" = "city",
    "city_town" = "city",
    "zipcode" = "zip_code",
    "zip_code" = "zip_code",
    "countyname" = "county",
    "county_parish" = "county",
    "county_name" = "county",
    "county" = "county",
    "hospitaltype" = "hospital_type",
    "hospital_type" = "hospital_type",
    "hospitalowner" = "hospital_ownership",
    "hospitalownership" = "hospital_ownership",
    "hospital_ownership" = "hospital_ownership",
    "ownership" = "hospital_ownership",
    "emergencyservice" = "emergency_services",
    "emergency_services" = "emergency_services",
    "hospital_overall_rating" = "overall_rating"
  )

  df <- apply_col_map(df, col_map)

  keep_cols <- c("facility_id", "facility_name", "state", "city", "zip_code",
                 "county", "hospital_type", "hospital_ownership",
                 "emergency_services", "overall_rating")
  keep_cols <- intersect(keep_cols, names(df))

  df <- df[, ..keep_cols]
  df$vintage <- vintage

  hosp_info_list[[vintage]] <- df
}

if (length(hosp_info_list) > 0) {
  hosp_info <- rbindlist(hosp_info_list, fill = TRUE)
  chr_cols <- names(hosp_info)[sapply(hosp_info, is.character)]
  hosp_info[, (chr_cols) := lapply(.SD, trimws), .SDcols = chr_cols]
  fwrite(hosp_info, "data/output/nber_hospital_info.csv")
  message("Wrote nber_hospital_info.csv: ", nrow(hosp_info), " rows, ",
          uniqueN(hosp_info$vintage), " vintages, ",
          uniqueN(hosp_info$facility_id), " unique facilities")
} else {
  message("WARNING: No hospital info files found across NBER vintages")
}


# =========================================================================
# 2. Quality Measures (process/outcome measures)
# =========================================================================

message("\n=== Parsing quality measures ===")

quality_list <- list()

for (d in nber_dirs) {
  vintage <- str_extract(basename(d), "\\d{6}")

  f <- find_file(d, c(
    "^dbo_vwhqi_hosp_msr_xwlk\\.csv$",
    "^vwhqi_hosp_msr_xwlk\\.csv$",
    "^hqi_hosp_timelyeffectivecare\\.csv$",
    "^Timely_and_Effective_Care-Hospital",
    "^Timely and Effective Care - Hospital"
  ))

  if (is.null(f)) next

  df <- safe_read(f)
  if (is.null(df) || nrow(df) == 0) next

  df <- standardize_names(df)

  col_map <- c(
    "provider" = "facility_id",
    "provider_id" = "facility_id",
    "facility_id" = "facility_id",
    "prvdr_id" = "facility_id",
    "hospname" = "facility_name",
    "hospital_name" = "facility_name",
    "facility_name" = "facility_name",
    "state" = "state",
    "condition" = "condition",
    "measurecode" = "measure_id",
    "measure_id" = "measure_id",
    "msr_cd" = "measure_id",
    "measurename" = "measure_name",
    "measure_name" = "measure_name",
    "score" = "score",
    "scorestr" = "score",
    "sample" = "sample",
    "samplestr" = "sample",
    "footnote" = "footnote",
    "start_date" = "start_date",
    "end_date" = "end_date"
  )

  df <- apply_col_map(df, col_map)

  keep_cols <- c("facility_id", "facility_name", "state", "condition",
                 "measure_id", "measure_name", "score", "sample",
                 "footnote", "start_date", "end_date")
  keep_cols <- intersect(keep_cols, names(df))

  df <- df[, ..keep_cols]
  df$vintage <- vintage

  quality_list[[vintage]] <- df
}

if (length(quality_list) > 0) {
  quality <- rbindlist(quality_list, fill = TRUE)
  chr_cols <- names(quality)[sapply(quality, is.character)]
  quality[, (chr_cols) := lapply(.SD, trimws), .SDcols = chr_cols]
  fwrite(quality, "data/output/nber_quality_measures.csv")
  message("Wrote nber_quality_measures.csv: ", nrow(quality), " rows, ",
          uniqueN(quality$vintage), " vintages")
} else {
  message("WARNING: No quality measure files found across NBER vintages")
}


# =========================================================================
# 3. HCAHPS (Patient Experience)
# =========================================================================

message("\n=== Parsing HCAHPS ===")

hcahps_list <- list()

for (d in nber_dirs) {
  vintage <- str_extract(basename(d), "\\d{6}")

  f <- find_file(d, c(
    "^dbo_vwhqi_hosp_hcahps_msr\\.csv$",
    "^vwhqi_hosp_hcahps_msr\\.csv$",
    "^hqi_hosp_hcahps\\.csv$",
    "^hcahps___hospital\\.csv$",
    "^HCAHPS-Hospital",
    "^HCAHPS - Hospital"
  ))

  if (is.null(f)) next

  df <- safe_read(f)
  if (is.null(df) || nrow(df) == 0) next

  df <- standardize_names(df)

  col_map <- c(
    "provider" = "facility_id",
    "provider_id" = "facility_id",
    "facility_id" = "facility_id",
    "hospname" = "facility_name",
    "hospital_name" = "facility_name",
    "facility_name" = "facility_name",
    "state" = "state",
    "hcahpsmsr" = "hcahps_measure_id",
    "hcahps_measure_id" = "hcahps_measure_id",
    "hcahpsq" = "hcahps_question",
    "hcahps_question" = "hcahps_question",
    "hcahpsa" = "hcahps_answer_description",
    "hcahps_answer_description" = "hcahps_answer_description",
    "hcahpsapct" = "hcahps_answer_percent",
    "hcahps_answer_percent" = "hcahps_answer_percent",
    "hcahps_linear_mean_value" = "hcahps_linear_mean_value",
    "patient_survey_star_rating" = "patient_survey_star_rating",
    "numsurv" = "number_of_completed_surveys",
    "number_of_completed_surveys" = "number_of_completed_surveys",
    "survrr" = "survey_response_rate_percent",
    "survey_response_rate_percent" = "survey_response_rate_percent",
    "footnote" = "footnote",
    "start_date" = "start_date",
    "end_date" = "end_date"
  )

  df <- apply_col_map(df, col_map)

  keep_cols <- c("facility_id", "facility_name", "state",
                 "hcahps_measure_id", "hcahps_question",
                 "hcahps_answer_description", "hcahps_answer_percent",
                 "hcahps_linear_mean_value", "patient_survey_star_rating",
                 "number_of_completed_surveys", "survey_response_rate_percent",
                 "footnote", "start_date", "end_date")
  keep_cols <- intersect(keep_cols, names(df))

  df <- df[, ..keep_cols]
  df$vintage <- vintage

  hcahps_list[[vintage]] <- df
}

if (length(hcahps_list) > 0) {
  hcahps <- rbindlist(hcahps_list, fill = TRUE)
  chr_cols <- names(hcahps)[sapply(hcahps, is.character)]
  hcahps[, (chr_cols) := lapply(.SD, trimws), .SDcols = chr_cols]
  fwrite(hcahps, "data/output/nber_hcahps.csv")
  message("Wrote nber_hcahps.csv: ", nrow(hcahps), " rows, ",
          uniqueN(hcahps$vintage), " vintages")
} else {
  message("WARNING: No HCAHPS files found across NBER vintages")
}


# =========================================================================
# 4. Mortality and Readmissions
# =========================================================================

message("\n=== Parsing mortality/readmissions ===")

mort_readm_list <- list()

for (d in nber_dirs) {
  vintage <- str_extract(basename(d), "\\d{6}")

  f <- find_file(d, c(
    "^dbo_vwhqi_hosp_mortality_readm_xwlk\\.csv$",
    "^dbo_vwhqi_hosp_mortality_xwlk\\.csv$",
    "^vwhqi_hosp_mortality_readm_xwlk\\.csv$",
    "^hqi_hosp_readmcompdeath\\.csv$",
    "^hqi_hosp_readmdeath\\.csv$",
    "^hqi_hosp_mv\\.csv$",
    "^complications___hospital\\.csv$",
    "^Complications_and_Deaths-Hospital",
    "^Complications and Deaths - Hospital"
  ))

  if (is.null(f)) next

  df <- safe_read(f)
  if (is.null(df) || nrow(df) == 0) next

  df <- standardize_names(df)

  col_map <- c(
    "provider" = "facility_id",
    "provider_id" = "facility_id",
    "facility_id" = "facility_id",
    "prvdr_id" = "facility_id",
    "hospname" = "facility_name",
    "hospital_name" = "facility_name",
    "facility_name" = "facility_name",
    "state" = "state",
    "condition" = "condition",
    "measurecode" = "measure_id",
    "measure_id" = "measure_id",
    "msr_cd" = "measure_id",
    "measurename" = "measure_name",
    "measure_name" = "measure_name",
    "compared_to_national" = "compared_to_national",
    "denominator" = "denominator",
    "score" = "score",
    "scr" = "score",
    "lower_estimate" = "lower_estimate",
    "higher_estimate" = "higher_estimate",
    "footnote" = "footnote",
    "start_date" = "start_date",
    "end_date" = "end_date"
  )

  df <- apply_col_map(df, col_map)

  keep_cols <- c("facility_id", "facility_name", "state", "condition",
                 "measure_id", "measure_name", "compared_to_national",
                 "denominator", "score", "lower_estimate", "higher_estimate",
                 "footnote", "start_date", "end_date")
  keep_cols <- intersect(keep_cols, names(df))

  df <- df[, ..keep_cols]
  df$vintage <- vintage

  mort_readm_list[[vintage]] <- df
}

if (length(mort_readm_list) > 0) {
  mort_readm <- rbindlist(mort_readm_list, fill = TRUE)
  chr_cols <- names(mort_readm)[sapply(mort_readm, is.character)]
  mort_readm[, (chr_cols) := lapply(.SD, trimws), .SDcols = chr_cols]
  fwrite(mort_readm, "data/output/nber_mortality_readmissions.csv")
  message("Wrote nber_mortality_readmissions.csv: ", nrow(mort_readm), " rows, ",
          uniqueN(mort_readm$vintage), " vintages")
} else {
  message("WARNING: No mortality/readmission files found across NBER vintages")
}


# =========================================================================
# 5. Healthcare-Associated Infections
# =========================================================================

message("\n=== Parsing HAI ===")

hai_list <- list()

for (d in nber_dirs) {
  vintage <- str_extract(basename(d), "\\d{6}")

  f <- find_file(d, c(
    "^dbo_vwhqi_hosp_hai\\.csv$",
    "^vwhqi_hosp_hai\\.csv$",
    "^hqi_hosp_hai\\.csv$",
    "^healthcare_associated_infections___hospital\\.csv$",
    "^Healthcare_Associated_Infections-Hospital",
    "^Healthcare Associated Infections - Hospital"
  ))

  if (is.null(f)) next

  df <- safe_read(f)
  if (is.null(df) || nrow(df) == 0) next

  df <- standardize_names(df)

  col_map <- c(
    "provider" = "facility_id",
    "provider_id" = "facility_id",
    "facility_id" = "facility_id",
    "prvdr_id" = "facility_id",
    "hospname" = "facility_name",
    "hospital_name" = "facility_name",
    "facility_name" = "facility_name",
    "state" = "state",
    "measurecode" = "measure_id",
    "measure_id" = "measure_id",
    "msr_cd" = "measure_id",
    "measurename" = "measure_name",
    "measure_name" = "measure_name",
    "compared_to_national" = "compared_to_national",
    "score" = "score",
    "scr" = "score",
    "footnote" = "footnote",
    "start_date" = "start_date",
    "end_date" = "end_date"
  )

  df <- apply_col_map(df, col_map)

  keep_cols <- c("facility_id", "facility_name", "state",
                 "measure_id", "measure_name", "compared_to_national",
                 "score", "footnote", "start_date", "end_date")
  keep_cols <- intersect(keep_cols, names(df))

  df <- df[, ..keep_cols]
  df$vintage <- vintage

  hai_list[[vintage]] <- df
}

if (length(hai_list) > 0) {
  hai <- rbindlist(hai_list, fill = TRUE)
  chr_cols <- names(hai)[sapply(hai, is.character)]
  hai[, (chr_cols) := lapply(.SD, trimws), .SDcols = chr_cols]
  fwrite(hai, "data/output/nber_hai.csv")
  message("Wrote nber_hai.csv: ", nrow(hai), " rows, ",
          uniqueN(hai$vintage), " vintages")
} else {
  message("WARNING: No HAI files found across NBER vintages")
}


message("\nNBER legacy parsing complete.")
