

#' Bikes with Rudimentary Stats
#'
#' Returns all bikes (by `number`) ever seen with some rudimentary stats.
#'
#' @param con database connection. Typically an `SQLiteConnection`
#'        but should be possible to easily extend to MariaDB/MySQL (untested).
#'
#' @examples
#' \dontrun{
#' library('RSQLite')
#' con <- dbConnect(SQLite(), "../stadtrad.db")
#' x <- si_bikes(con)
#' head(x)
#' plot(x)
#'}
#'
#' @importFrom sf st_as_sf
#' @author Reto
#' @export
#' @rdname si_bikes
si_bikes <- function(con) {
    stopifnot(
        "'con' must be an SQLiteConnection (for now)" = inherits(con, "SQLiteConnection")
    )

    # Retrieving data
    sql <- "SELECT b.number, COUNT(b.number) AS records,
            avg(p.lat) AS lat, avg(lon) AS lon FROM bikes as b
            JOIN places AS p ON b.place_id = p.id
            GROUP BY b.number"
    res <- dbGetQuery(con, sql)

    # Converting to sf
    res <- st_as_sf(res, coords = c("lon", "lat"), crs = st_crs(4326))

    # Transform and return
    structure(res, class = c("si_bikes", class(res)))
}


#' @param x object of class `si_bikes`.
#' @param \dots forwarded to plot method.
#'
#' @exportS3Method plot si_bikes
#' @rdname si_bikes
plot.si_bikes <- function(x, ...) {
    class(x) <- class(x)[-1]

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

