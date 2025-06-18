

#' Parsing JSON Files
#'
#' Helper function to parse the original JSON files, or ZIP archives
#' with these JSON files.
#'
#' @param x character name of the file(s) to be parsed.
#' @param domain character, defaults to `"si"`. Ensures the input
#'        files are for a specific domain (`"si"` = Stadtrad Innsbruck).
#' @param verbose logical. If `TRUE` some messages will be printed.
#'
#' @details If `x` is a ZIP archive it will be unpacked and all JSON files
#' inside will be parsed. The file names must match the expected pattern
#' as we extract the time stamp from the file name.
#'
#' @return Named list with two elements containing data.frames with
#' the `places` information and the `bikes` information in a raw format.
#' `bikes$datetime` is extracted from the JSON file name.
#'
#' @importFrom jsonlite read_json
#' @importFrom dplyr bind_rows
#' @author Reto
#' @export
ri_parse_json <- function(x, domain = "si", verbose = TRUE) {
    stopifnot(
        "'x' must be character" = is.character(x) && length(x) > 0L,
        "'x' must be files ending in json or zip" =
            all(grepl(".*\\.(zip|json)$", x, ignore.case = TRUE)),
        "'verbose' must be TRUE or FALSE" = isTRUE(verbose) || isFALSE(verbose)
    )

    # check if files even exist
    if (!all(file.exists(x)))
        stop("Not all files exist, missing: ", paste(x[!file.exists(x)], collapse = ", "))

    # Unpacking zip files if needed
    idx <- grep(".*\\.zip$", x, ignore.case = TRUE)
    tmpdir <- tempdir()

    files <- x[grepl(".*\\.json$", x, ignore.case = TRUE)]
    if (length(idx) > 0) {
        for (f in x[idx]) {
            files <- c(files, unzip(f, exdir = tmpdir, overwrite = TRUE))
        }
    }

    # Checking file name format
    pat <- sprintf("[0-9]+_%s.json", domain)
    idx <- which(!grepl(pat, basename(files), ignore.case = TRUE))
    if (length(idx) > 0L)
        stop("Not all files following the pattern \"", pat, "\": ",
             paste(files[idx], collapse = ", "), sep = "")

    # Parsing the files
    files <- sort(files)
    if (verbose) message("Number of json files to read: ", length(files))

    fn <- function(x) {
        d <- regmatches(basename(x), regexpr("[0-9]+", basename(x)))
        d <- as.POSIXct(as.integer(d), tz = ri_timezone())
        tryCatch(content <- read_json(x), error = function(x) stop(e))
        list(places = as.data.frame(bind_rows(content$places)),
             bikes  = cbind(data.frame(datetime = d, as.data.frame(bind_rows(content$bikes)))))
    }
    res <- lapply(files, fn)

    return(list(places = do.call(rbind, lapply(res, function(x) x$places)),
                bikes  = do.call(rbind, lapply(res, function(x) x$bikes))))
}




