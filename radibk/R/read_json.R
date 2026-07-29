

#' @param x character, name/path of one single json file (or multiple files when calling
#'        [ri_read_jsons()]) with the expected content.
#'        The file names, as its first integer, must contain a timestamp (seconds UTC) which
#'        is extracted as temporal information.
#' @param city_name character of length one, defaults to `"Innsbruck"`. Used to
#'        subset the data to one specific city.
#' @param cores `NULL` (default) or integer, only used by [ri_read_jsons()].
#'        Number of cores to use with `parallel::mclapply`. If `NULL` takes all but
#'        two available cores (maximum number of files to be processed).
#' @param returnclass one of "`data.frame"` (tibble data frames, default)
#'        or `"list"` (only for [ri_read_json()]).
#' @param verbose Logical, defaults to `FALSE`.
#'
#' @return A named list is returned which by default contains three
#' tibble data frames, namely `cities`, `places`, and `bikes`. The
#' date and time information extracted from the file name(s) `x` is
#' added as a new variable `datetime`.
#'
#' Calling [ri_read_json()] with `returnclass = "list"` the return is also
#' a named list but with four elements containing the raw data from the json
#' file subsetted to the city requested, plus one additional element
#' `datetime` with the timestamp (POSIXt).
#'
#' @importFrom tibble as_tibble
#' @importFrom jsonlite read_json
#'
#' @author Reto
#' @importFrom parallel mclapply
#' @export
ri_read_jsons <- function(x, city_name = "Innsbruck", cores = NULL, verbose = FALSE) {

    if (!is.null(cores)) cores <- as.integer(cores)[[1L]]
    stopifnot(
        "argument `cores` must be NULL or integer" =
            is.null(cores) || (is.integer(cores) && length(cores) == 1L)
    )
    if (!is.null(cores) && cores < 1L) stop("`cores` must be positive")
    if (is.null(cores)) cores <- pmin(length(x), pmax(1L, detectCores()))

    if (verbose)
        message("Processing ", length(x), " files on ", cores, " cores")

    # Parsing files
    x <- lapply(x, ri_read_json, city_name = city_name, verbose = verbose)

    # Combining data
    x <- list(cities  = bind_rows(lapply(x, function(x) x$cities)),
              places  = bind_rows(lapply(x, function(x) x$places)),
              bikes   = bind_rows(lapply(x, function(x) x$bikes)))
    return(x)
}


#' @author Reto
#' @rdname ri_read_jsons
#' @export
ri_read_json <- function(x, city_name = "Innsbruck",
                         returnclass = c("data.frame", "list"), verbose = FALSE) {
    verbose     <- as.logical(verbose)[[1L]]
    returnclass <- match.arg(returnclass)

    stopifnot(
        "argument `x` not pointing to single existing file" = isTRUE(file.exists(x)),
        "argument `x` must end on json" = grepl("\\.json$", x, ignore.case = TRUE),
        "argument `city_name` must be single character" =
            is.character(city_name) && length(city_name) == 1L,
        "argument `verbose` must evaluate to TRUE or FALSE" =
            isTRUE(verbose) || isFALSE(verbose)
    )

    # Extracting time information
    ts <- regmatches(basename(x), regexpr("[0-9]+", basename(x)))
    if (length(ts) == 0L) stop("File name (`x`) does not contain integer time stamp")
    ts <- as.POSIXct(as.integer(ts), tz = "UTC")
    if (verbose) message("Data set valid, data for \"", format(ts, "%Y-%m-%d %H:%M:%S %Z"), "\"")

    # Reading the file
    x <- read_json(x)
    if (!all(c("cities", "places", "bikes") %in% names(x)))
        stop("Unexpected json content, expected cities, places, and bikes")

    idx <- sapply(x$cities, function(x) x$name == city_name) |> which()
    if (length(idx) == 0L)
        stop("Could not find city \"", city_name, "\" in data")
    city_id <- x$cities[[idx]]$uid
    x$cities <- x$cities[idx]

    if (verbose) message(" - City \"", city_name, "\" with city_id = ", city_id)

    # Extracting places and bikes in that city
    x$places  <- x$places[sapply(x$places, function(x) x$city_id == city_id)]
    place_ids <- sapply(x$places, function(x) x$uid)
    x$bikes   <- x$bikes[sapply(x$bikes, function(x) x$place_id == place_ids)]

    if (verbose)
        message(" - Found ", length(x$places), " places and ",
                length(x$bikes), " bikes in city")

    # If returnclass is list, return as is
    if (returnclass == "list") {
        x$datetime <- ts
        return(x)
    }

    # Else converting to tibble data frames
    x$countries <- NULL # Not needed
    x <- lapply(x, bind_rows)

    # Adding time stamp to all elements
    x <- lapply(x, function(x) { x$datetime <- ts; x })
    return(x)

}






