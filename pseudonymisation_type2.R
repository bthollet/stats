#!/usr/bin/env Rscript

# Pseudonymisation d'un second type d'extraction de données d'élevage.
# Le script lit un fichier tabulaire, pseudonymise les identifiants
# (départements, numéros de pacage, dénominations) et normalise les champs
# numériques et dates pour conserver l'utilisabilité analytique.
#
# Utilisation minimale :
#   Rscript pseudonymisation_type2.R input.csv output.csv "mon_salt"
# ou en important les fonctions dans une session R :
#   source("pseudonymisation_type2.R")
#   pseudonymise_extraction_type2("input.csv", "output.csv", salt = "mon_salt")

ensure_package <- function(pkg) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    stop(
      sprintf(
        "Le package '%s' est requis. Installez-le avec install.packages('%s').",
        pkg, pkg
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

hash_values <- function(x, salt) {
  ensure_package("digest")
  vapply(
    x,
    function(val) {
      if (is.na(val) || val == "") {
        return(NA_character_)
      }
      digest::digest(paste0(salt, val), algo = "sha256")
    },
    character(1),
    USE.NAMES = FALSE
  )
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

pseudonymise_identifiers <- function(data, salt, columns) {
  for (col in columns) {
    if (col %in% names(data)) {
      data[[col]] <- hash_values(data[[col]], salt)
    }
  }
  data
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

  id_columns <- c(
    "Dept du gestionnaire",
    "Numéro pacage gestionnaire",
    "Dénomination gestionnaire",
    "Dept de l'utilisateurs",
    "Numéro pacage utilisateur",
    "Dénomination utilisateur"
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

  transformed <- raw_data
  transformed <- pseudonymise_identifiers(transformed, salt, id_columns)
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
