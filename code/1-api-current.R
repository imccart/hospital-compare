# Meta --------------------------------------------------------------------

## Author:        Ian McCarthy
## Date Created:  2026-03-10
## Description:   Parse current CMS Care Compare API downloads (api-YYYYMM/).
##                Reads hospital-level CSVs, cleans column names, writes to
##                data/output/api/ with vintage prefix.


# Paths -------------------------------------------------------------------

api_dirs <- list.dirs("data/input/hospitals", recursive = FALSE)
api_dirs <- api_dirs[grepl("api-\\d{6}$", api_dirs)]

if (length(api_dirs) == 0) stop("No api-YYYYMM folders found in data/input/hospitals/")


# Helper: clean column names to snake_case --------------------------------

clean_names <- function(x) {
  x %>%
    tolower() %>%
    str_replace_all("[^a-z0-9]+", "_") %>%
    str_replace_all("^_|_$", "")
}


# Helper: read and tag with vintage ---------------------------------------

read_api_file <- function(file, vintage) {
  if (!file.exists(file)) {
    message("  Skipping (not found): ", basename(file))
    return(NULL)
  }
  df <- fread(file, colClasses = "character", encoding = "UTF-8")
  names(df) <- clean_names(names(df))
  df$vintage <- vintage
  df
}


# Process each API vintage ------------------------------------------------

for (api_dir in api_dirs) {

  vintage <- str_extract(basename(api_dir), "\\d{6}")
  message("Processing API vintage: ", vintage)

  # --- Hospital General Information ---
  df <- read_api_file(file.path(api_dir, "Hospital_General_Information.csv"), vintage)
  if (!is.null(df)) {
    fwrite(df, sprintf("data/output/api/general_info_%s.csv", vintage))
    message("  general_info: ", nrow(df), " rows")
  }

  # --- HCAHPS (patient experience) ---
  df <- read_api_file(file.path(api_dir, "HCAHPS-Hospital.csv"), vintage)
  if (!is.null(df)) {
    fwrite(df, sprintf("data/output/api/hcahps_%s.csv", vintage))
    message("  hcahps: ", nrow(df), " rows")
  }

  # --- Complications and Deaths (mortality) ---
  df <- read_api_file(file.path(api_dir, "Complications_and_Deaths-Hospital.csv"), vintage)
  if (!is.null(df)) {
    fwrite(df, sprintf("data/output/api/complications_%s.csv", vintage))
    message("  complications: ", nrow(df), " rows")
  }

  # --- Unplanned Hospital Visits (readmissions) ---
  df <- read_api_file(file.path(api_dir, "Unplanned_Hospital_Visits-Hospital.csv"), vintage)
  if (!is.null(df)) {
    fwrite(df, sprintf("data/output/api/readmissions_%s.csv", vintage))
    message("  readmissions: ", nrow(df), " rows")
  }

  # --- Healthcare-Associated Infections ---
  df <- read_api_file(file.path(api_dir, "Healthcare_Associated_Infections-Hospital.csv"), vintage)
  if (!is.null(df)) {
    fwrite(df, sprintf("data/output/api/hai_%s.csv", vintage))
    message("  hai: ", nrow(df), " rows")
  }

  # --- Timely and Effective Care ---
  df <- read_api_file(file.path(api_dir, "Timely_and_Effective_Care-Hospital.csv"), vintage)
  if (!is.null(df)) {
    fwrite(df, sprintf("data/output/api/timely_effective_%s.csv", vintage))
    message("  timely_effective: ", nrow(df), " rows")
  }

  # --- Patient Safety Indicators (PSI) ---
  df <- read_api_file(file.path(api_dir, "CMS_PSI_6_decimal_file.csv"), vintage)
  if (!is.null(df)) {
    fwrite(df, sprintf("data/output/api/psi_%s.csv", vintage))
    message("  psi: ", nrow(df), " rows")
  }

  # --- Outpatient Imaging Efficiency ---
  df <- read_api_file(file.path(api_dir, "Outpatient_Imaging_Efficiency-Hospital.csv"), vintage)
  if (!is.null(df)) {
    fwrite(df, sprintf("data/output/api/imaging_%s.csv", vintage))
    message("  imaging: ", nrow(df), " rows")
  }

  # --- Medicare Spending Per Patient ---
  df <- read_api_file(file.path(api_dir, "Medicare_Hospital_Spending_Per_Patient-Hospital.csv"), vintage)
  if (!is.null(df)) {
    fwrite(df, sprintf("data/output/api/spending_%s.csv", vintage))
    message("  spending: ", nrow(df), " rows")
  }

  # --- HAC Reduction Program ---
  hac_file <- list.files(api_dir, pattern = "HAC_Reduction_Program", full.names = TRUE)
  if (length(hac_file) == 1) {
    df <- read_api_file(hac_file, vintage)
    if (!is.null(df)) {
      fwrite(df, sprintf("data/output/api/hac_%s.csv", vintage))
      message("  hac: ", nrow(df), " rows")
    }
  }

  # --- HVBP (value-based purchasing) — merge all domain files ---
  hvbp_files <- list.files(api_dir, pattern = "^hvbp_", full.names = TRUE)
  if (length(hvbp_files) > 0) {
    hvbp_list <- lapply(hvbp_files, function(f) {
      df <- fread(f, colClasses = "character", encoding = "UTF-8")
      names(df) <- clean_names(names(df))
      df$domain_file <- basename(f)
      df
    })

    # TPS file has the summary; domain files have measure detail.
    # Write each domain separately since column structures differ.
    for (i in seq_along(hvbp_files)) {
      domain <- str_replace(basename(hvbp_files[i]), "\\.csv$", "")
      hvbp_list[[i]]$vintage <- vintage
      fwrite(hvbp_list[[i]], sprintf("data/output/api/%s_%s.csv", domain, vintage))
      message("  ", domain, ": ", nrow(hvbp_list[[i]]), " rows")
    }
  }

  # --- Hospital Readmissions Reduction Program ---
  hrrp_file <- list.files(api_dir, pattern = "Readmissions_Reduction_Program", full.names = TRUE)
  if (length(hrrp_file) == 1) {
    df <- read_api_file(hrrp_file, vintage)
    if (!is.null(df)) {
      fwrite(df, sprintf("data/output/api/hrrp_%s.csv", vintage))
      message("  hrrp: ", nrow(df), " rows")
    }
  }

  # --- Maternal Health ---
  df <- read_api_file(file.path(api_dir, "Maternal_Health-Hospital.csv"), vintage)
  if (!is.null(df)) {
    fwrite(df, sprintf("data/output/api/maternal_%s.csv", vintage))
    message("  maternal: ", nrow(df), " rows")
  }

  # --- Measure Dates (reference) ---
  df <- read_api_file(file.path(api_dir, "Measure_Dates.csv"), vintage)
  if (!is.null(df)) {
    fwrite(df, sprintf("data/output/api/measure_dates_%s.csv", vintage))
    message("  measure_dates: ", nrow(df), " rows")
  }

  # --- Footnote Crosswalk (reference) ---
  df <- read_api_file(file.path(api_dir, "Footnote_Crosswalk.csv"), vintage)
  if (!is.null(df)) {
    fwrite(df, sprintf("data/output/api/footnote_crosswalk_%s.csv", vintage))
    message("  footnote_crosswalk: ", nrow(df), " rows")
  }

  message("Done with vintage ", vintage, "\n")
}

message("API parsing complete.")
