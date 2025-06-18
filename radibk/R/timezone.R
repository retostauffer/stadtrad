


#' Get and Set Default Time Zone
#'
#' The package will handle times in local time zone, defaulting to
#' 'Europe/Vienna'. This function allows to set the time zone
#' and/or retrieve the time zone.
#'
#' @param x `NULL` or character (see `?OlsonNames`). If set, the
#'        default (`"Europe/London"`) will be overwritten and used for the
#'        current session.
#'
#' @return Character with current time zone used by the package.
ri_timezone <- function(x = NULL) {
    stopifnot("'x' must be NULL or character of length 1" =
            is.null(x) || (is.character(x) && length(x) == 1L))
    if (is.character(x))
        stopifnot("'x' not found in OlsonNames()" = x %in% OlsonNames())

    # Set new
    if (!is.null(x)) {
        message("Setting radibk_timezone to ", x)
        Sys.setenv("radibk_timezone" = x)
    }
    # Return user-specified time zone or default value
    return(Sys.getenv("radibk_timezone", "Europe/Vienna"))
}
