#!/usr/bin/env Rscript

# Pseudonymisation d'un second type d'extraction de données d'élevage.
# Le script lit un fichier tabulaire, pseudonymise uniquement les numéros de
# pacage en conservant leurs trois premiers chiffres, supprime les
# dénominations, laisse les départements en clair et normalise les champs
# numériques et dates pour conserver l'utilisabilité analytique.
#
# Utilisation minimale :
#   Rscript pseudonymisation_type2.R input.csv output.csv "mon_salt"
# ou en important les fonctions dans une session R :
#   source("pseudonymisation_type2.R")
#   pseudonymise_extraction_type2("input.csv", "output.csv", salt = "mon_salt")

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
      sprintf(
        "Le package '%s' est requis et n'a pas pu être installé automatiquement.",
        pkg
      ),
      call. = FALSE
    )
  }
}

normalize_decimal <- function(x) {
  # Nettoie les espacements (y compris espaces insécables) et convertit les
  # virgules décimales en points.
  cleaned <- gsub("\\s+", "", x, useBytes = TRUE)
  cleaned <- gsub("\u00a0", "", cleaned, fixed = TRUE)
  cleaned <- sub(",", ".", cleaned, fixed = TRUE)
  suppressWarnings(as.numeric(cleaned))
}

normalize_date <- function(x) {
  # Convertit au format Date en considérant le format français JJ/MM/AAAA.
  as.Date(x, format = "%d/%m/%Y")
}

coerce_numeric_columns <- function(data, columns) {
  for (col in columns) {
    if (col %in% names(data)) {
      data[[col]] <- normalize_decimal(data[[col]])
    }
  }
  data
}

coerce_date_columns <- function(data, columns) {
  for (col in columns) {
    if (col %in% names(data)) {
      data[[col]] <- normalize_date(data[[col]])
    }
  }
  data
}

pseudonymize_pacage <- function(x, salt, prefix = "") {
  ensure_package("digest")
  vapply(
    x,
    function(val) {
      if (is.na(val) || trimws(val) == "") {
        return(NA_character_)
      }
      cleaned <- gsub("\\s+", "", as.character(val), useBytes = TRUE)
      first_three <- substr(cleaned, 1, 3)
      hashed <- digest::digest(paste0(salt, cleaned), algo = "sha256")
      paste0(prefix, first_three, "_", substr(hashed, 1, 9))
    },
    character(1),
    USE.NAMES = FALSE
  )
}

pseudonymise_extraction_type2 <- function(input_path,
                                          output_path,
                                          salt = Sys.getenv("PSEUDONYMISATION_SALT"),
                                          delim = ";",
                                          encoding = "UTF-8") {
  if (is.null(salt) || identical(salt, "")) {
    stop("Un salt non vide est requis pour la pseudonymisation.", call. = FALSE)
  }

  ensure_package("readr")

  raw_data <- readr::read_delim(
    file = input_path,
    delim = delim,
    col_types = readr::cols(.default = readr::col_character()),
    locale = readr::locale(encoding = encoding),
    trim_ws = TRUE,
    show_col_types = FALSE
  )

  numeric_columns <- c(
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
    "Nombre UGB",
    "Nombre UGB temps plein"
  )

  date_columns <- c("Date montée", "Date descente")

  pacage_columns <- c(
    "Numéro pacage gestionnaire",
    "Numéro pacage utilisateur"
  )

  denomination_columns <- c(
    "Dénomination gestionnaire",
    "Dénomination utilisateur"
  )

  dept_columns <- c(
    "Dept du gestionnaire",
    "Dept de l'utilisateurs"
  )

  transformed <- raw_data

  for (col in pacage_columns) {
    if (col %in% names(transformed)) {
      prefix <- if (grepl("gestionnaire", col, ignore.case = TRUE)) "GEST_" else "UTIL_"
      transformed[[col]] <- pseudonymize_pacage(transformed[[col]], salt, prefix)
    }
  }

  for (col in denomination_columns) {
    if (col %in% names(transformed)) {
      transformed[[col]] <- NULL
    }
  }

  for (col in dept_columns) {
    if (col %in% names(transformed)) {
      transformed[[col]] <- trimws(as.character(transformed[[col]]))
    }
  }

  transformed <- coerce_numeric_columns(transformed, numeric_columns)
  transformed <- coerce_date_columns(transformed, date_columns)

  readr::write_csv(transformed, output_path, na = "")
  invisible(transformed)
}

if (identical(environment(), globalenv()) && !length(commandArgs(trailingOnly = TRUE))) {
  message("Script chargé. Utilisez pseudonymise_extraction_type2() pour traiter vos fichiers.")
}

args <- commandArgs(trailingOnly = TRUE)

if (length(args) %in% c(2, 3)) {
  input_path <- args[[1]]
  output_path <- args[[2]]
  salt <- if (length(args) == 3) args[[3]] else Sys.getenv("PSEUDONYMISATION_SALT")

  pseudonymise_extraction_type2(
    input_path = input_path,
    output_path = output_path,
    salt = salt
  )
  message("Fichier pseudonymisé écrit dans : ", output_path)
} else if (length(args) > 0) {
  stop(
    "Usage : Rscript pseudonymisation_type2.R <input_path> <output_path> [salt]",
    call. = FALSE
  )
}
