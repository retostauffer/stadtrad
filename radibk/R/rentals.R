

#' Availability of Rentals
#'
#' Every few minutes the number of available bikes per station
#' is recorded. This is mainly useful for official stations, thus
#' `random locations' are excluded by default.
#'
#' @param con database connection. Typically an `SQLiteConnection`
#'        but should be possible to easily extend to MariaDB/MySQL (untested).
#' @param officials_only logical, defaults to `TRUE`. If `FALSE`, random
#'        parking locations are also included.
#'
#' @examples
#' \dontrun{
#' library('RSQLite')
#' con <- dbConnect(SQLite(), "../stadtrad.db")
#' x <- ri_rentals(con)
#' head(x)
#' plot(x)
#'}
#'
#' @importFrom sf st_as_sf
#' @author Reto
#' @export
#' @rdname ri_rentals
ri_rentals <- function(con, officials_only = TRUE) {
    stopifnot(
        "'con' must be an SQLiteConnection (for now)" = inherits(con, "SQLiteConnection")
    )
    officials_only <- as.logical(officials_only)[1L]
    stopifnot("'officials_only' must evaluate to TRUE or FALSE" =
              isTRUE(officials_only) || isFALSE(officials_only))

    # Retrieving data
    sql <- "SELECT p.id, p.name, p.lat, p.lon,
                   r.timestamp, r.bikes, r.available
            FROM places AS p
            LEFT JOIN rentals AS r
            ON p.id = r.place_id
            ## ORDER BY p.id ASC
    "
    if (!officials_only) {
        sql <- gsub("##", "", sql)
    } else {
        sql <- gsub("##", "WHERE p.timestamp IS NULL", sql)
    }
    res <- dbGetQuery(con, sql)

    # Converting to sf
    res$timestamp <- as.POSIXct(res$timestamp, ri_timezone())
    res <- st_as_sf(res, coords = c("lon", "lat"), crs = st_crs(4326))

    # Transform and return
    structure(res, class = c("ri_rentals", class(res)))
}


#' @param x object of class `ri_rentals`.
#' @param \dots forwarded to plot method.
#' @param bbox object of class `bbox`.
#'
#' @importFrom dplyr group_by summarise `%>%`
#' @importFrom sf st_crop
#' @exportS3Method plot ri_rentals
#' @rdname ri_rentals
plot.ri_rentals <- function(x, bbox = ri_bbox(), ...) {
    stopifnot("'bbox' must be of class `bbox`" = inherits(bbox, "bbox"))
    class(x) <- class(x)[-1]
    x <- st_crop(x, bbox)
    x <- x %>% group_by(geometry) %>% summarise(average = mean(available, na.rm = TRUE))

    args <- list(...)
    if (!"main" %in% names(args)) args$main <- "Average number of bikes"
    args$x   <- x["average"]
    if (!"pch" %in% names(args)) args$pch <- 19
    if (!"cex" %in% names(args))
        args$cex <- 0.5 + (x$average - min(x$average)) / max(x$average) * 4

    do.call(plot, args)
    invisible(x)
}

