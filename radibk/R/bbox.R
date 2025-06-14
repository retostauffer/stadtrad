
#' Stadtrad Innsbruck Bounding Box
#'
#' Bounding box used to delete locations (places) outside range.
#' Defaults to $11.3-11.5$E, $47.2-47.3$N but can be changed if needed.
#'
#' @param xmin,xmax,ymin,ymax single numeric, defining limits for longitude
#'        (`x`) and latitude (`y`). Minimum must be smaller than the maximum.
#'
#' @return Object of class `bbox` with CRS $4326$.
#'
#' @importFrom sf st_bbox st_crs
#' @author Reto
#' @export
si_bbox <- function(xmin = 11.3, xmax = 11.5, ymin = 47.2, ymax = 47.3) {
    stopifnot(
        "'xmin' must be single numeric" = is.numeric(xmin) && length(xmin) == 1L,
        "'xmax' must be single numeric" = is.numeric(xmax) && length(xmax) == 1L,
        "'ymin' must be single numeric" = is.numeric(ymin) && length(ymin) == 1L,
        "'ymax' must be single numeric" = is.numeric(ymax) && length(ymax) == 1L,
        "'xmin' must be smaller than 'xmax'" = xmin < xmax,
        "'ymin' must be smaller than 'ymax'" = ymin < ymax
    )
    return(st_bbox(c(xmin = xmin, xmax = xmax, ymin = ymin, ymax = ymax),
                   crs = st_crs(4326)))
}

