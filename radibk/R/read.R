
#' Reading Stadtrad Data
#'
#' The REST API returns a single JSON file at the time of the request.
#' Individual JSON files can be processed using `ri_read_json`. Note that
#' the name of the file must include an integer, the timestamp of the request.
#' A series of convenience functions exist to read not only one, but multiple
#' files at a time. `ri_read_jsons` imports a series of JSON files and returns
#' a combined data set. If archived in a ZIP file, `ri_read_zip` will read all
#' available JSON files from that archive. Multiple archive files can be read
#' using the `ri_read_zips` function.
#'
#' @param x character, name/path of the file(s) to be read. See Section
#'        Details for more information.
#' @param city_name character of length one, defaults to `"Innsbruck"`. Used to
#'        subset the data to one specific city.
#' @param cores `NULL` (default) or integer, not used by `ri_read_json`.
#'        Number of cores to used with `parallel::mclapply`. If `NULL` it takes all but
#'        two available cores (topped of by the maximum number of json files to be
#'        processed).
#' @param returnclass only used by `ri_read_json`, one of "`data.frame"`
#'        (tibble data frames, default) or `"list"` (only for `ri_read_json`).
#' @param tz defaults to `NULL`, forwarded to [ri_timezone()].
#' @param verbose integer, defaults to `0L` (`FALSE`; silent). If it evaluates to
#'        `1L` (`TRUE`) it is verbose on function level, `2L` makes the interfaced
#'        functions verbose, too.
#'
#' @return Object of class `ri_data`, a named list with three tibble data
#' frames: `cities`, `places`, and `bikes`. The date and time information
#' extracted from the file name(s) `x` is added as a new variable `datetime`.
#' All data frames are sorted by datetime (univariate).
#'
#' There is one exception: Calling `ri_read_json` allows to set `returnclass = "list"`
#' in which case the return is a named list of length four with mostly raw data,
#' with the elements `cities`, `places`, and `bikes` as for all the other functions,
#' except they do not include the datetime information. Instead, the datetime information
#' is stored on the fourth element `datetime`.
#'
#' @examples
#' \dontrun{
#' ## Reading JSON files:
#' ## Find available json files
#' jsonfiles <- list.jsonfiles(file.path("archive", "foo"), full.names = TRUE)
#' jsonfiles <- jsonfiles[grepl("^[0-9]+_si.json$", basename(jsonfiles))]
#'
#' ## Reading single json file
#' x <- ri_read_json(jsonfiles[1], verbose = TRUE)
#'
#' ## Reading set of json files
#' y <- ri_read_jsons(jsonfiles[1:3], verbose = TRUE)
#'
#' ## Quick visual inspection
#' library("tinyplot")
#' tinyplot(available_bikes ~ datetime, data = y$cities)
#' tinyplot(bikes ~ datetime | name, type = "l",
#'          data = subset(y$places, !grepl("^BIKE", name)))
#'
#' ## Reading ZIP archive files
#' ## Find available ZIP files
#' zipfiles <- list.files("archive", full.names = TRUE)
#' zipfiles <- zipfiles[grepl("_si\\.zip$", zipfiles)]
#'
#' #' ## Reading single ZIP file
#' x <- ri_read_zip(zipfiles[1], verbose = TRUE)
#'
#' #' ## Reading set of ZIP files
#' y <- ri_read_zips(zipfiles[1:5], verbose = TRUE)
#'
#' ## Quick visual inspection
#' tinyplot(available_bikes ~ datetime | as.Date(datetime),
#'          data = y$cities, type = "l")
#' }
#'
#' @importFrom parallel detectCores mclapply
#' @importFrom tibble as_tibble
#' @importFrom jsonlite read_json
#' @importFrom dplyr bind_rows
#'
#' @rdname ri_read_json
#' @author Reto
#' @export
ri_read_jsons <- function(x, city_name = "Innsbruck", cores = NULL, tz = NULL, verbose = FALSE) {

    verbose <- as.integer(verbose)[[1L]]
    if (!is.null(cores)) cores <- as.integer(cores)[[1L]]
    stopifnot(
        "argument `x` points to at least one non-existing file" = all(file.exists(x)),
        "argument `x` must all end on .json" = all(grepl("\\.json$", x, ignore.case = TRUE)),
        "argument `cores` must be NULL or integer" =
            is.null(cores) || (is.integer(cores) && length(cores) == 1L),
        "argument `verbose` must evaluate to 0L, 1L, or 2L" = 
            is.integer(verbose) && (verbose >= 0L && verbose <= 2L)
    )

    # Setting number of cores
    if (!is.null(cores) && cores < 1L) stop("`cores` must be positive")
    if (is.null(cores)) cores <- pmin(length(x), pmax(1L, detectCores()))

    if (verbose) message("Processing ", length(x), " files on ", cores, " cores")

    # Parsing files
    x <- mclapply(x, ri_read_json, city_name = city_name, mc.cores = cores,
                  verbose = verbose >= 2L, tz = tz)

    # Combining data
    x <- list(cities  = bind_rows(lapply(x, function(x) x$cities)),
              places  = bind_rows(lapply(x, function(x) x$places)),
              bikes   = bind_rows(lapply(x, function(x) x$bikes)))

    # Ensure correct order of rows (only based on datetime) and return
    return(lapply(x, function(x) x[order(x$datetime, decreasing = TRUE), ]) |>
                  structure(class = "ri_data"))
}


#' @importFrom dplyr bind_rows
#'
#' @author Reto
#' @rdname ri_read_json
#' @export
ri_read_json <- function(x, city_name = "Innsbruck", returnclass = c("data.frame", "list"),
                         tz = NULL, verbose = FALSE) {
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
    ts <- as.POSIXct(as.integer(ts), tz = ri_timezone(tz))
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

    if (verbose)
        message(" - Found ", length(x$places), " places and ",
                length(x$bikes), " bikes in city")

    # If returnclass is list, return as is
    if (returnclass == "list") {
        x$bikes    <- x$bikes[sapply(x$bikes, function(x) x$place_id %in% place_ids), ]
        x$datetime <- ts
        return(x)
    }

    # Else converting to tibble data frames
    x$countries <- NULL # Not needed
    x <- lapply(x, bind_rows)

    x$bikes <- x$bikes[x$bikes$place_id %in% place_ids, ]

    # Make numeric (integer)
    x$places$place_type <- as.integer(x$places$place_type)


    # Adding time stamp to all elements
    x <- lapply(x, function(x) { x$datetime <- ts; x })
    return(x |> structure(class = "ri_data"))
}


#' @author Reto
#' @importFrom utils unzip
#' @rdname ri_read_json
#' @export
ri_read_zip <- function(x, city_name = "Innsbruck", cores = NULL, tz = NULL, verbose = FALSE) {
    verbose     <- as.logical(verbose)[[1L]]

    stopifnot(
        "argument `x` not pointing to single existing zip file" = isTRUE(file.exists(x)),
        "argument `x` must end on zip" = grepl("\\.zip$", x, ignore.case = TRUE),
        "argument `city_name` must be single character" =
            is.character(city_name) && length(city_name) == 1L,
        "argument `verbose` must evaluate to TRUE or FALSE" =
            isTRUE(verbose) || isFALSE(verbose)
    )

    if (verbose) message("Extracting json files from \"", x, "\"")

    # Temporary directory to extract the zip file
    dir.create(tmpdir <- tempfile(pattern = "radibk_"))
    on.exit(unlink(tmpdir, recursive = TRUE))

    # Unzipping
    files <- unzip(x, exdir = tmpdir)
    files <- files[grepl("\\.json$", files, ignore.case = TRUE)]

    return(ri_read_jsons(files, cores = cores, verbose = verbose))
}


#' @importFrom dplyr bind_rows
#'
#' @author Reto
#' @rdname ri_read_json
#' @export
ri_read_zips <- function(x, city_name = "Innsbruck", cores = NULL, tz = NULL, verbose = FALSE) {
    verbose     <- as.integer(verbose)[[1L]]

    stopifnot(
        "argument `x` points to at least one non-existing file" = all(file.exists(x)),
        "argument `x` must all end on .zip" = all(grepl("\\.zip$", x, ignore.case = TRUE)),
        "argument `city_name` must be single character" =
            is.character(city_name) && length(city_name) == 1L,
        "argument `verbose` must evaluate to 0L, 1L, or 2L" = 
            is.integer(verbose) && (verbose >= 0L && verbose <= 2L)
    )

    x <- lapply(x, ri_read_zip, city_name = city_name,
                cores = cores, tz = tz, verbose = verbose >= 2L)

    res <- list()
    for (n in names(x[[1L]]))
        res[[n]] <- bind_rows(lapply(x, function(x) x[[n]]))

    # Ensure correct order of rows (only based on datetime) and return
    return(lapply(res, function(x) x[order(x$datetime, decreasing = TRUE), ]) |>
           structure(class = "ri_data"))
}



#' @param what what to plot.
#' @param ... currently unused.
#'
#' @importFrom graphics abline axis barplot par
#' @rdname ri_read_json
#' @exportS3Method plot ri_data
#' @importFrom tinyplot tinyplot
plot.ri_data <- function(x, what = c("cities", "places"), ...) {

    what <- match.arg(what, several.ok = TRUE)
    hold <- par(no.readonly = TRUE); on.exit(par(hold))
    if (length(what) > 1L) par(ask = TRUE)

    ## Overall avaliable bikes over the entire city
    if ("cities" %in% what) {
        dt  <- as.Date(x$cities$datetime)
        idx <- as.integer(factor(dt))
        tinyplot(available_bikes ~ datetime | as.Date(datetime), data = x$cities,
                 xaxt = "n", main = "Citywide total number of available bikes",
                 theme = "clean2")

        xat  <- if (max(idx) < 5) unique(dt) else pretty(dt)
        axis(side = 1, padj = 1, at = as.POSIXct(xat, tz = attr(x$cities$datetime, "tzone")),
             labels = format(xat, "%a\n%y-%m-%d"), lwd = 0)
        abline(v = as.POSIXct(unique(dt), tz = attr(x$cities$datetime, "tzone")),
               col = "gray", lty = 2)
    }

    ## Available bikes at stations, using only known stations
    if ("places" %in% what) {
        tmp <- subset(x$places, !grepl("^BIKE", name))
        tinyplot(bikes_available_to_rent ~ datetime | name, data = tmp,
                 type = "l", theme = "clean2")
    }

    invisible(NULL)
}

