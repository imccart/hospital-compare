# Meta --------------------------------------------------------------------

## Author:        Ian McCarthy
## Date Created:  2026-03-10
## Description:   Combine API and NBER outputs into final panel datasets.
##                Deduplicates overlapping vintages, standardizes IDs.


# =========================================================================
# 1. Hospital General Info Panel
# =========================================================================

message("=== Building hospital info panel ===")

# NBER legacy
nber_info <- NULL
if (file.exists("data/output/nber_hospital_info.csv")) {
  nber_info <- fread("data/output/nber_hospital_info.csv", colClasses = "character")
  nber_info$source <- "nber"
}

# API current
api_info_files <- list.files("data/output/api", pattern = "^general_info_", full.names = TRUE)
api_info <- NULL
if (length(api_info_files) > 0) {
  api_info <- rbindlist(lapply(api_info_files, function(f) {
    df <- fread(f, colClasses = "character")

    # Standardize API column names to match NBER panel
    name_map <- c(
      "facility_id" = "facility_id",
      "facility_name" = "facility_name",
      "state" = "state",
      "city_town" = "city",
      "city" = "city",
      "zip_code" = "zip_code",
      "county_parish" = "county",
      "county" = "county",
      "hospital_type" = "hospital_type",
      "hospital_ownership" = "hospital_ownership",
      "ownership" = "hospital_ownership",
      "emergency_services" = "emergency_services",
      "hospital_overall_rating" = "overall_rating",
      "vintage" = "vintage"
    )

    existing <- intersect(names(df), names(name_map))
    for (col in existing) {
      if (name_map[col] != col) {
        names(df)[names(df) == col] <- name_map[col]
      }
    }

    keep <- intersect(c("facility_id", "facility_name", "state", "city",
                         "zip_code", "county", "hospital_type",
                         "hospital_ownership", "emergency_services",
                         "overall_rating", "vintage"), names(df))
    df <- df[, ..keep]
    df$source <- "api"
    df
  }), fill = TRUE)
}

# Stack
info_parts <- Filter(Negate(is.null), list(nber_info, api_info))
if (length(info_parts) > 0) {
  hospital_info <- rbindlist(info_parts, fill = TRUE)

  # Pad facility_id to 6 characters (leading zeros)
  hospital_info[, facility_id := str_pad(facility_id, 6, pad = "0")]

  # Deduplicate: if same facility_id + vintage appears in both sources, keep API
  hospital_info[, source_rank := ifelse(source == "api", 1, 2)]
  hospital_info <- hospital_info[order(facility_id, vintage, source_rank)]
  hospital_info <- hospital_info[!duplicated(hospital_info[, .(facility_id, vintage)])]
  hospital_info[, source_rank := NULL]

  fwrite(hospital_info, "data/output/hospital_info.csv")
  message("Wrote hospital_info.csv: ", nrow(hospital_info), " rows, ",
          uniqueN(hospital_info$facility_id), " facilities, ",
          uniqueN(hospital_info$vintage), " vintages")
} else {
  message("WARNING: No hospital info data to merge")
}


# =========================================================================
# 2. Quality Measures Panel
# =========================================================================

message("\n=== Building quality measures panel ===")

nber_quality <- NULL
if (file.exists("data/output/nber_quality_measures.csv")) {
  nber_quality <- fread("data/output/nber_quality_measures.csv", colClasses = "character")
  nber_quality$source <- "nber"
}

api_te_files <- list.files("data/output/api", pattern = "^timely_effective_", full.names = TRUE)
api_quality <- NULL
if (length(api_te_files) > 0) {
  api_quality <- rbindlist(lapply(api_te_files, function(f) {
    df <- fread(f, colClasses = "character")
    keep <- intersect(c("facility_id", "facility_name", "state", "condition",
                         "measure_id", "measure_name", "score", "sample",
                         "footnote", "start_date", "end_date", "vintage"), names(df))
    df <- df[, ..keep]
    df$source <- "api"
    df
  }), fill = TRUE)
}

quality_parts <- Filter(Negate(is.null), list(nber_quality, api_quality))
if (length(quality_parts) > 0) {
  quality <- rbindlist(quality_parts, fill = TRUE)
  quality[, facility_id := str_pad(facility_id, 6, pad = "0")]
  fwrite(quality, "data/output/hospital_quality_measures.csv")
  message("Wrote hospital_quality_measures.csv: ", nrow(quality), " rows, ",
          uniqueN(quality$vintage), " vintages")
}


# =========================================================================
# 3. HCAHPS Panel
# =========================================================================

message("\n=== Building HCAHPS panel ===")

nber_hcahps <- NULL
if (file.exists("data/output/nber_hcahps.csv")) {
  nber_hcahps <- fread("data/output/nber_hcahps.csv", colClasses = "character")
  nber_hcahps$source <- "nber"
}

api_hcahps_files <- list.files("data/output/api", pattern = "^hcahps_", full.names = TRUE)
api_hcahps <- NULL
if (length(api_hcahps_files) > 0) {
  api_hcahps <- rbindlist(lapply(api_hcahps_files, function(f) {
    df <- fread(f, colClasses = "character")
    keep <- intersect(c("facility_id", "facility_name", "state",
                         "hcahps_measure_id", "hcahps_question",
                         "hcahps_answer_description", "hcahps_answer_percent",
                         "hcahps_linear_mean_value", "patient_survey_star_rating",
                         "number_of_completed_surveys", "survey_response_rate_percent",
                         "footnote", "start_date", "end_date", "vintage"), names(df))
    df <- df[, ..keep]
    df$source <- "api"
    df
  }), fill = TRUE)
}

hcahps_parts <- Filter(Negate(is.null), list(nber_hcahps, api_hcahps))
if (length(hcahps_parts) > 0) {
  hcahps <- rbindlist(hcahps_parts, fill = TRUE)
  hcahps[, facility_id := str_pad(facility_id, 6, pad = "0")]
  fwrite(hcahps, "data/output/hospital_hcahps.csv")
  message("Wrote hospital_hcahps.csv: ", nrow(hcahps), " rows, ",
          uniqueN(hcahps$vintage), " vintages")
}


# =========================================================================
# 4. Mortality & Readmissions Panel
# =========================================================================

message("\n=== Building mortality/readmissions panel ===")

nber_mr <- NULL
if (file.exists("data/output/nber_mortality_readmissions.csv")) {
  nber_mr <- fread("data/output/nber_mortality_readmissions.csv", colClasses = "character")
  nber_mr$source <- "nber"
}

# API complications (mortality)
api_comp_files <- list.files("data/output/api", pattern = "^complications_", full.names = TRUE)
api_readm_files <- list.files("data/output/api", pattern = "^readmissions_", full.names = TRUE)
api_mr <- NULL
if (length(c(api_comp_files, api_readm_files)) > 0) {
  api_mr <- rbindlist(lapply(c(api_comp_files, api_readm_files), function(f) {
    df <- fread(f, colClasses = "character")
    keep <- intersect(c("facility_id", "facility_name", "state", "condition",
                         "measure_id", "measure_name", "compared_to_national",
                         "denominator", "score", "lower_estimate", "higher_estimate",
                         "footnote", "start_date", "end_date", "vintage"), names(df))
    df <- df[, ..keep]
    df$source <- "api"
    df
  }), fill = TRUE)
}

mr_parts <- Filter(Negate(is.null), list(nber_mr, api_mr))
if (length(mr_parts) > 0) {
  mort_readm <- rbindlist(mr_parts, fill = TRUE)
  mort_readm[, facility_id := str_pad(facility_id, 6, pad = "0")]
  fwrite(mort_readm, "data/output/hospital_mortality_readmissions.csv")
  message("Wrote hospital_mortality_readmissions.csv: ", nrow(mort_readm), " rows, ",
          uniqueN(mort_readm$vintage), " vintages")
}


# =========================================================================
# 5. HAI Panel
# =========================================================================

message("\n=== Building HAI panel ===")

nber_hai <- NULL
if (file.exists("data/output/nber_hai.csv")) {
  nber_hai <- fread("data/output/nber_hai.csv", colClasses = "character")
  nber_hai$source <- "nber"
}

api_hai_files <- list.files("data/output/api", pattern = "^hai_", full.names = TRUE)
api_hai <- NULL
if (length(api_hai_files) > 0) {
  api_hai <- rbindlist(lapply(api_hai_files, function(f) {
    df <- fread(f, colClasses = "character")
    keep <- intersect(c("facility_id", "facility_name", "state",
                         "measure_id", "measure_name", "compared_to_national",
                         "score", "footnote", "start_date", "end_date", "vintage"), names(df))
    df <- df[, ..keep]
    df$source <- "api"
    df
  }), fill = TRUE)
}

hai_parts <- Filter(Negate(is.null), list(nber_hai, api_hai))
if (length(hai_parts) > 0) {
  hai <- rbindlist(hai_parts, fill = TRUE)
  hai[, facility_id := str_pad(facility_id, 6, pad = "0")]
  fwrite(hai, "data/output/hospital_hai.csv")
  message("Wrote hospital_hai.csv: ", nrow(hai), " rows, ",
          uniqueN(hai$vintage), " vintages")
}


message("\n=== Panel build complete ===")
message("Final outputs in data/output/:")
message("  hospital_info.csv")
message("  hospital_quality_measures.csv")
message("  hospital_hcahps.csv")
message("  hospital_mortality_readmissions.csv")
message("  hospital_hai.csv")
