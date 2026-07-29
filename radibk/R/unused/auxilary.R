

#' An Index Class for Hourly Data
#'
#' @param x object of class POSIXct
#' @param tz character, time zone. Defaults to `ri_timezone()`.
#'
#' @return POSIXct object of same length as `x` but with
#' ourly data (used for indexing in aggregation).
#'
#' @export
as.hourly <- function(x, tz = ri_timezone()) {
    stopifnot(
        "'x' must be POSIXt" = inherits(x, "POSIXt"),
        "'tz' must be character" = is.character(tz) && length(tz) == 1L,
        "'tz' not found in OlsonNames()" = tz %in% OlsonNames()
    )

    # Convert
    x <- ceiling(as.integer(x) / 3600) * 3600
    return(as.POSIXct(x, tz = tz))
}
