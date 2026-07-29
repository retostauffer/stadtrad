
#' Parsing JSON Data
#'
#' Extracting all bike numbers seen.
#'
#' @param files can be a ZIP file containing a series of JSON files, or 
#'        a character vector with one or multiple json files. If ZIP,
#'        the ZIP file is unpacked and all JSON files found within are
#'        processed.
#' @param mc.cores number of cores, defaults to all but two cores if not
#'        specified otherwise.
#'
#' @return Returns an integer vector with all unique bike IDs seen
#' in the file/files.
#'
#' @importFrom jsonlite read_json
#' @export
ri_get_bike_numbers <- function(files, mc.cores = pmax(1L, detectCores() - 2L)) {

    mc.cores <- as.integer(mc.cores)[[1L]]
    stopifnot(
        "argument 'mc.cores' must be positive integer" =
            is.integer(mc.cores) && length(mc.cores) == 1L && mc.cores > 0L,
        "argument 'files' must be character of length > 0" =
            is.character(files) && length(files) > 0L,
        "not all files in 'files' exist" = all(file.exists(files))
    )

    # If ZIP, there must only be one files.
    is_zip <- sum(grepl(".zip$", files, ignore.case = TRUE))
    if (is_zip > 1L)
        stop("only one ZIP file can be processed at a time")
    if (is_zip) {
        dir.create(tmpdir <- tempfile(), showWarnings = FALSE)
        files <- unzip(files, exdir = tmpdir)
    }

    fn <- function(f) {
        x <- tryCatch(read_json(f),
            error = function(e) { warning("Issues reading \"", f, "\", returning NULL"); NULL })
        if (is.null(x)) return(NULL)
        return(as.integer(sapply(x$bikes, function(y) y$number)))
    }
    res <- mclapply(files, fn, mc.cores = mc.cores)
    res <- unique(unlist(res))
    if (is_zip) unlink(tmpdir)
    return(res)
}


#' Loading Bike Positions
#'
#' @param files can be a ZIP file containing a series of JSON files, or 
#'        a character vector with one or multiple json files. If ZIP,
#'        the ZIP file is unpacked and all JSON files found within are
#'        processed.
#' @param combine logical, defaults to `TRUE`.
#' @param tz time zone used to convert the time stamps to POSIXct. Defaults
#'        to "Europe/Vienna" (see [ri_timezone()]).
#' @param mc.cores number of cores, defaults to all but two cores if not
#'        specified otherwise.
#'
#' @return If combine is `TRUE` (default), data frame is returned with
#' combined bike and place information. This data set is reduced to the
#' required information for further processing. When `combine = FALSE`
#' a list of two data frame with all data found is returned.
#'
#' @importFrom jsonlite read_json
#' @importFrom dplyr bind_rows
fi_get_bike_positions <- function(files, combine = TRUE, tz = ri_timezone(), mc.cores = pmax(1L, detectCores() - 2L)) {

    mc.cores <- as.integer(mc.cores)[[1L]]
    combine  <- as.logical(combine)[[1L]]
    stopifnot(
        "argument 'mc.cores' must be positive integer" =
            is.integer(mc.cores) && length(mc.cores) == 1L && mc.cores > 0L,
        "argument 'files' must be character of length > 0" =
            is.character(files) && length(files) > 0L,
        "argument 'combine' must be TRUE or FALSE" =
            isTRUE(combine) || isFALSE(combine),
        "not all files in 'files' exist" = all(file.exists(files))
    )

    # If ZIP, there must only be one files.
    is_zip <- sum(grepl(".zip$", files, ignore.case = TRUE))
    if (is_zip > 1L)
        stop("only one ZIP file can be processed at a time")
    if (is_zip) {
        dir.create(tmpdir <- tempfile(), showWarnings = FALSE)
        files <- unzip(files, exdir = tmpdir)
    }


    # Scopes 'combine' and 'tz'
    fn <- function(f) {
        x <- tryCatch(read_json(f),
            error = function(e) { warning("Issues reading \"", f, "\", returning NULL"); NULL })
        if (is.null(x)) return(NULL)
        ts  <- as.integer(regmatches(basename(f), regexpr("^[0-9]+", basename(f))))
        res <- list(places = bind_rows(x$places), bikes  = bind_rows(x$bikes))
        res$bikes$timestamp <- res$places$timestamp <- as.POSIXct(ts, tz = tz)
        if (combine) {
            res <- merge(res$bikes[c("number", "place_id", "timestamp")],
                         res$places[c("uid", "lat", "lng", "name")],
                         by.x = "place_id", by.y = "uid",
                         all.x = TRUE, all.y = FALSE)
        }
        return(res)
    }
    tmp <- mclapply(files, fn, mc.cores = mc.cores)

    # Delete unzipped data if needed
    if (is_zip) unlink(tmpdir)

    # When 'combine' is false, we row-bind places and bikes individually.
    # Else 'tmp' is a list of alrady combined data frames.
    if (!combine) {
        list(places = bind_rows(lapply(tmp, function(z) z$places)),
                    bikes  = bind_rows(lapply(tmp, function(z) z$bikes)))
    } else {
        bind_rows(tmp)
    }
}



