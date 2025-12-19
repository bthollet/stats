#!/usr/bin/env Rscript

# Pseudonymisation des extractions « montées_descentes.xlsx ».
#
# Le script lit un classeur Excel, pseudonymise les numéros de pacage tout en
# conservant leurs trois premiers chiffres, supprime les dénominations et
# laisse les départements en clair. Les champs quantitatifs et les dates sont
# normalisés.
#
# Usage :
#   Rscript pseudonymisation_montees_descentes.R input.xlsx output.csv [salt]
#
#   - input.xlsx : fichier source (Excel)
#   - output.csv : chemin du CSV pseudonymisé en sortie
#   - salt       : optionnel, sinon la variable d'environnement
#                  PSEUDONYMISATION_SALT sera utilisée

ensure_package <- function(pkg) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    repos <- getOption("repos")
    if (is.null(repos) || identical(repos["CRAN"], "@CRAN@") || identical(repos["CRAN"], "")) {
      options(repos = c(CRAN = "https://cloud.r-project.org"))
    }
    install.packages(pkg, quiet = TRUE)
  }

  if (!requireNamespace(pkg, quietly = TRUE)) {
    stop(
      sprintf("Le package '%s' est requis et n'a pas pu être installé automatiquement.", pkg),
      call. = FALSE
    )
  }
}

ensure_dependencies <- function() {
  pkgs <- c("stringr", "digest", "readxl", "readr", "tibble")
  lapply(pkgs, ensure_package)
}

normalize_columns <- function(df) {
  names(df) <- names(df) |>
    stringr::str_replace_all("\\s+", " ") |>
    stringr::str_trim()
  df
}

normalize_decimal <- function(x) {
  cleaned <- gsub("\\s+", "", as.character(x), useBytes = TRUE)
  cleaned <- gsub("\u00a0", "", cleaned, fixed = TRUE)
  cleaned <- sub(",", ".", cleaned, fixed = TRUE)
  suppressWarnings(as.numeric(cleaned))
}

parse_french_date <- function(x) {
  as.Date(as.character(x), format = "%d/%m/%Y")
}

pseudonymize_pacage <- function(values, salt, prefix = "") {
  vapply(
    values,
    function(value) {
      if (is.na(value) || trimws(value) == "") {
        return(NA_character_)
      }
      cleaned <- gsub("\\s+", "", as.character(value), useBytes = TRUE)
      first_three <- substr(cleaned, 1, 3)
      hashed <- digest::digest(paste0(salt, "|", cleaned), algo = "sha256")
      paste0(prefix, first_three, "_", substr(hashed, 1, 9))
    },
    character(1),
    USE.NAMES = FALSE
  )
}

sanitize_montees_descentes <- function(df, salt) {
  ensure_dependencies()
  df <- normalize_columns(df)

  required_cols <- c(
    "Dept de l'utilisateur",
    "Dept du gestionnaire",
    "Numéro pacage gestionnaire",
    "Numéro pacage utilisateur",
    "Bovins de moins de 6 mois",
    "Bovins de 6 mois à 2 ans",
    "Bovins de plus de 2 ans",
    "Ovins",
    "Caprins",
    "Equidés",
    "Lamas",
    "Alpagas",
    "Cerfs et biches",
    "Daims et daines",
    "Nombre de jours de présence",
    "Date montée",
    "Date descente",
    "Nombre UGB",
    "Nombre UGB temps plein"
  )

  missing_cols <- setdiff(required_cols, names(df))
  if (length(missing_cols) > 0) {
    stop(
      sprintf("Colonnes manquantes : %s", paste(missing_cols, collapse = ", ")),
      call. = FALSE
    )
  }

  to_integer <- function(x) as.integer(normalize_decimal(x))

  tibble::tibble(
    dept_utilisateur = stringr::str_trim(as.character(df[["Dept de l'utilisateur"]])),
    dept_gestionnaire = stringr::str_trim(as.character(df[["Dept du gestionnaire"]])),
    gestionnaire_pacage_id = pseudonymize_pacage(df[["Numéro pacage gestionnaire"]], salt, "GEST_"),
    utilisateur_pacage_id = pseudonymize_pacage(df[["Numéro pacage utilisateur"]], salt, "UTIL_"),
    bovins_moins_6_mois = to_integer(df[["Bovins de moins de 6 mois"]]),
    bovins_6_mois_2_ans = to_integer(df[["Bovins de 6 mois à 2 ans"]]),
    bovins_plus_2_ans = to_integer(df[["Bovins de plus de 2 ans"]]),
    ovins = to_integer(df[["Ovins"]]),
    caprins = to_integer(df[["Caprins"]]),
    equides = to_integer(df[["Equidés"]]),
    lamas = to_integer(df[["Lamas"]]),
    alpagas = to_integer(df[["Alpagas"]]),
    cerfs_biches = to_integer(df[["Cerfs et biches"]]),
    daims_daines = to_integer(df[["Daims et daines"]]),
    jours_presence = to_integer(df[["Nombre de jours de présence"]]),
    date_montee = parse_french_date(df[["Date montée"]]),
    date_descente = parse_french_date(df[["Date descente"]]),
    nombre_ugb = normalize_decimal(df[["Nombre UGB"]]),
    nombre_ugb_temps_plein = normalize_decimal(df[["Nombre UGB temps plein"]])
  )
}

pseudonymise_montees_descentes <- function(input_path,
                                           output_path,
                                           salt = Sys.getenv("PSEUDONYMISATION_SALT"),
                                           sheet = 1) {
  if (is.null(salt) || identical(salt, "")) {
    stop("Un salt non vide est requis pour la pseudonymisation.", call. = FALSE)
  }

  ensure_dependencies()

  raw_data <- readxl::read_excel(
    path = input_path,
    sheet = sheet,
    col_types = "text"
  )

  sanitized <- sanitize_montees_descentes(raw_data, salt)
  readr::write_csv(sanitized, output_path, na = "")
  invisible(sanitized)
}

args <- commandArgs(trailingOnly = TRUE)

if (length(args) %in% c(2, 3, 4)) {
  input_path <- args[[1]]
  output_path <- args[[2]]
  salt <- if (length(args) >= 3) args[[3]] else Sys.getenv("PSEUDONYMISATION_SALT")
  sheet <- if (length(args) == 4) as.integer(args[[4]]) else 1

  if (is.na(sheet)) {
    stop("Le paramètre de feuille doit être un entier valide.", call. = FALSE)
  }

  pseudonymise_montees_descentes(
    input_path = input_path,
    output_path = output_path,
    salt = salt,
    sheet = sheet
  )
  message("Fichier pseudonymisé écrit dans : ", output_path)
} else if (length(args) > 0) {
  stop(
    "Usage : Rscript pseudonymisation_montees_descentes.R <input.xlsx> <output.csv> [salt] [sheet]",
    call. = FALSE
  )
}
