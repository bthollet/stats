
#' Anonymisation des extractions "données estives collectives"
#'
#' Ce script lit un fichier d'extraction, pseudonymise les identifiants
#' (numéros de pacage et dénominations) et exporte un fichier épuré qui ne
#' contient plus d'informations directement identifiantes.
#'
#' Utilisation en ligne de commande :
#'   Rscript anon.R input.csv output.csv
#'
#' Arguments :
#'   - input.csv  : fichier source (séparateur auto-détecté parmi ; , ou tab)
#'   - output.csv : chemin du fichier anonymisé en sortie
#'
#' Vous devez fournir un sel via la variable d'environnement ANON_SALT pour
#' garantir la reproductibilité des hachages.

# ---- Fonctions utilitaires -------------------------------------------------

require_pkg <- function(pkg) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    stop(
      sprintf("Le package '%s' est requis. Installez-le avec install.packages('%s').", pkg, pkg),
      call. = FALSE
    )
  }
}

ensure_dependencies <- function() {
  pkgs <- c("stringr", "digest", "tibble", "readr")
  lapply(pkgs, require_pkg)
}

guess_separator <- function(path) {
  first_line <- readLines(path, n = 1, warn = FALSE)
  separators <- c("," = ",", ";" = ";", "\t" = "\t")
  counts <- vapply(separators, function(sep) stringr::str_count(first_line, sep), integer(1))
  names(which.max(counts))
}

normalize_columns <- function(df) {
  names(df) <- names(df) |>
    stringr::str_replace_all("\\s+", " ") |>
    stringr::str_trim()
  df
}

parse_french_date <- function(x) {
  as.Date(x, format = "%d/%m/%Y")
}

hash_with_salt <- function(values, salt, prefix = "") {
  require_pkg("digest")
  vapply(
    values,
    function(value) {
      if (is.na(value) || trimws(value) == "") return(NA_character_)
      hash <- digest::digest(paste0(salt, "|", value), algo = "sha256")
      paste0(prefix, substr(hash, 1, 12))
    },
    character(1),
    USE.NAMES = FALSE
  )
}

# ---- Pipeline principal ----------------------------------------------------

sanitize_estives_collectives <- function(df, salt) {
  ensure_dependencies()
  df <- normalize_columns(df)

  required_cols <- c(
    "Numéro pacage gestionnaire",
    "Dénomination gestionnaire",
    "Numéro pacage utilisateur",
    "Dénomination utilisateur",
    "Ovins",
    "Caprins",
    "Equidés",
    "Lamas",
    "Alpagas",
    "Cerfs et biches",
    "Daims et daines",
    "Nombre de jours de présence",
    "Date montée",
    "Date descente"
  )

  missing_cols <- setdiff(required_cols, names(df))
  if (length(missing_cols) > 0) {
    stop(
      sprintf(
        "Colonnes manquantes : %s",
        paste(missing_cols, collapse = ", ")
      ),
      call. = FALSE
    )
  }

  tibble::tibble(
    gestionnaire_id = hash_with_salt(df[["Numéro pacage gestionnaire"]], salt, "GEST_"),
    utilisateur_id = hash_with_salt(df[["Numéro pacage utilisateur"]], salt, "UTIL_"),
    gestionnaire_nom_hash = hash_with_salt(df[["Dénomination gestionnaire"]], salt, "GNOM_"),
    utilisateur_nom_hash = hash_with_salt(df[["Dénomination utilisateur"]], salt, "UNOM_"),
    ovins = as.integer(df[["Ovins"]]),
    caprins = as.integer(df[["Caprins"]]),
    equides = as.integer(df[["Equidés"]]),
    lamas = as.integer(df[["Lamas"]]),
    alpagas = as.integer(df[["Alpagas"]]),
    cerfs_biches = as.integer(df[["Cerfs et biches"]]),
    daims_daines = as.integer(df[["Daims et daines"]]),
    jours_presence = as.integer(df[["Nombre de jours de présence"]]),
    date_montee = parse_french_date(df[["Date montée"]]),
    date_descente = parse_french_date(df[["Date descente"]])
  )
}

anonymize_estives_file <- function(input_path, output_path, salt = Sys.getenv("ANON_SALT"), sep = NULL) {
  ensure_dependencies()
  if (salt == "") {
    stop("Définissez la variable d'environnement ANON_SALT pour pseudonymiser les identifiants.", call. = FALSE)
  }

  if (is.null(sep)) {
    sep <- guess_separator(input_path)
  }

  raw <- utils::read.delim(
    input_path,
    sep = sep,
    dec = ".",
    stringsAsFactors = FALSE,
    check.names = FALSE,
    encoding = "UTF-8"
  )

  sanitized <- sanitize_estives_collectives(raw, salt)
  readr::write_csv(sanitized, file = output_path, na = "")
  invisible(sanitized)
}

# ---- Exécution CLI ---------------------------------------------------------

run_from_cli <- function() {
  args <- commandArgs(trailingOnly = TRUE)
  if (length(args) == 0) {
    return(invisible(NULL))
  }

  if (length(args) != 2) {
    stop("Usage : Rscript anon.R input.csv output.csv", call. = FALSE)
  }

  anonymize_estives_file(args[1], args[2])
  message("Fichier anonymisé écrit dans : ", args[2])
}

run_from_cli()

# ---- Exemple minimal -------------------------------------------------------

# Exemple reproductible pour tester rapidement le pipeline :
# demo_df <- data.frame(
#   "Numéro pacage gestionnaire" = c("064018100", "064018100"),
#   "Dénomination gestionnaire" = c("CS DU PAYS DE SOULE", "CS DU PAYS DE SOULE"),
#   "Numéro pacage utilisateur" = c("064174454", "064174465"),
#   "Dénomination utilisateur" = c("GAEC OIER", "Monsieur IRITCITY ETCHART Allande"),
#   "Ovins" = c(140, 180),
#   "Caprins" = 0,
#   "Equidés" = 0,
#   "Lamas" = 0,
#   "Alpagas" = 0,
#   "Cerfs et biches" = 0,
#   "Daims et daines" = 0,
#   "Nombre de jours de présence" = c(148, 121),
#   "Date montée" = c("10/05/2022", "01/06/2022"),
#   "Date descente" = c("05/10/2022", "30/09/2022"),
#   check.names = FALSE
# )
# Sys.setenv(ANON_SALT = "change-me")
# sanitize_estives_collectives(demo_df, Sys.getenv("ANON_SALT"))
