

#' Bikes with Rudimentary Stats
#'
#' Returns all bikes (by `number`) ever seen with some rudimentary stats.
#'
#' @param con database connection. Typically an `SQLiteConnection`
#'        but should be possible to easily extend to MariaDB/MySQL (untested).
#'
#' @return Spatial feature data.frame containing all ever recorded bikes
#' (identified by bike number), number of records in different places,
#' and the average position of the bike.
#'
#' * `number`: Bike identifier (bike number).
#' * `records`: Number of stored records the bike has been seen in
#'   different locations (i.e., moved places).
#' * `avg_time_h`: Average parking time in hours.
#' * `geometry`: Unweighted average longitude and latitude of all
#'   the places the bike has been recorded.
#'
#' @examples
#' \dontrun{
#' library('RSQLite')
#' con <- dbConnect(SQLite(), "../stadtrad.db")
#' x <- ri_bikes(con)
#' head(x)
#' plot(x)
#'}
#'
#' @importFrom sf st_as_sf
#' @author Reto
#' @export
#' @rdname ri_bikes
ri_bikes <- function(con) {
    stopifnot(
        "'con' must be an SQLiteConnection (for now)" = inherits(con, "SQLiteConnection")
    )

    # Retrieving data
    sql <- "SELECT b.number, COUNT(b.number) AS records,
            AVG(p.lat) AS avg_lat, AVG(lon) AS avg_lon,
            AVG((CAST(last_seen AS float) - CAST(first_seen AS float)) / 3600) AS avg_time_h
            FROM bikes as b
            JOIN places AS p ON b.place_id = p.id
            GROUP BY b.number"
    res <- dbGetQuery(con, sql)

    # Converting to sf
    res <- st_as_sf(res, coords = c("avg_lon", "avg_lat"), crs = st_crs(4326))

    # Transform and return
    structure(res, class = c("ri_bikes", class(res)))
}


#' @param x object of class `ri_bikes`.
#' @param \dots forwarded to plot method.
#'
#' @exportS3Method plot ri_bikes
#' @rdname ri_bikes
plot.ri_bikes <- function(x, ...) {
    class(x) <- class(x)[-1]

    x <- x[order(x$records, decreasing = FALSE), ]

    args <- list(...)
    if (!"main" %in% names(args))
        args$main <- sprintf("Rudimentary bike stats (n = %d, avg location + record count)", nrow(x))
    args$x   <- x["records"]
    if (!"pch" %in% names(args)) args$pch <- 19
    if (!"cex" %in% names(args))
        args$cex <- .5 + (x$records - min(x$records)) / max(x$records) * 2

    do.call(plot, args)
    invisible(x)
}

