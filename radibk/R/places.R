

#' Retrieve Places (Locations)
#'
#' The API reports where (how many) bikes are located when parked.
#' Besides around 50 official stations (as of 2025) bikes can also
#' be parked outside official stations.
#'
#' @param con database connection. Typically an `SQLiteConnection`
#'        but should be possible to easily extend to MariaDB/MySQL (untested).
#'
#' @examples
#' \dontrun{
#' library('RSQLite')
#' con <- dbConnect(SQLite(), "../stadtrad.db")
#' x <- ri_places(con)
#' head(x)
#' plot(x)
#'}
#'
#' @importFrom sf st_as_sf
#' @author Reto
#' @export
#' @rdname ri_places
ri_places <- function(con) {
    stopifnot(
        "'con' must be an SQLiteConnection (for now)" = inherits(con, "SQLiteConnection")
    )

    # Retrieving data
    res <- dbGetQuery(con, "SELECT * FROM places ORDER BY id ASC")

    # Converting to sf
    res <- st_as_sf(res, coords = c("lon", "lat"), crs = st_crs(4326))

    # Transform and return
    structure(transform(res, official_station = !grepl("^BIKE", name)),
              class = c("ri_places", class(res)))
}


#' @param x object of class `ri_places`.
#' @param \dots forwarded to plot method.
#' @param bbox object of class `bbox`.
#'
#' @importFrom sf st_crop
#' @exportS3Method plot ri_places
#' @rdname ri_places
plot.ri_places <- function(x, bbox = ri_bbox(), ...) {
    stopifnot("'bbox' must be of class `bbox`" = inherits(bbox, "bbox"))
    class(x) <- class(x)[-1]
    x <- st_crop(x, bbox)

    args <- list(...)
    if (!"main" %in% names(args))
        args$main <- sprintf("Bike Places (official: %d, random: %d)",
                             sum(x$official_station), sum(!x$official_station))
    args$x   <- x["official_station"]
    args$pch <- ifelse(x$official_station, 19, 1)
    args$col <- ifelse(x$official_station, 1, "gray60")
    args$cex <- ifelse(x$official_station, 1, 1 / 2)

    do.call(plot, args)
    invisible(x)
}

