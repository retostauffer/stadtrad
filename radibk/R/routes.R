
#' Retrieve Routes
#'
#' Calculates routes (bikes moved from A to B).
#'
#' @param con database connection. Typically an `SQLiteConnection`
#'        but should be possible to easily extend to MariaDB/MySQL (untested).
#'
#' @examples
#' \dontrun{
#' library('RSQLite')
#' con <- dbConnect(SQLite(), "../stadtrad.db")
#' x <- si_routes(con)
#' head(x)
#' plot(x)
#'}
#'
#' @importFrom sf st_as_sf
#' @author Reto
#' @export
#' @rdname si_places
si_routes <- function(con) {
    stopifnot(
        "'con' must be an SQLiteConnection (for now)" = inherits(con, "SQLiteConnection")
    )

    # Retrieving data
    sql <- "SELECT b.first_seen, b.last_seen, b.number,
            b.bike_type, b.active, p.name, p.lon, p.lat
            FROM bikes AS b
            LEFT JOIN places AS p ON p.id = b.place_id
            ORDER BY b.number, b.first_seen"
    res <- dbGetQuery(con, sql)
    res <- calc_trip(res)
    return(res)
}

#' Calculate from-to geometry and time for the way the
#' bike has travelled between two points we've seen it.
#'
#' @importFrom sf st_set_geometry
#' @importFrom dplyr group_by group_modify
#' @author Reto
calc_trip <- function(x) {
    # TODO: add sanity checks?
    trips <- as.data.frame(x %>% group_by(number) %>% group_modify(~calc_way(.x)))

    tripstats <- merge(aggregate(time ~ route, data = trips, function(y) round(mean(y), 1)),
                       aggregate(count ~ route, data = transform(trips, count = 1), sum),
                       by = "route")

    # Taking unique trips to merge geometry to 'tripstats'
    uniq_trips <- unique(subset(trips, select = c(route, geometry)))

    res <- merge(uniq_trips, trips, by = c("route", "geometry"))
    res <- st_set_geometry(res, "geometry")
    return(structure(res, class = c("si_routes", class(res))))
}

# Heler function for calc_trip
calc_way <- function(x) {
    dummy <- data.frame(geometry = st_sfc(st_geometrycollection()),
                        route = NA_character_, time = NA_integer_)
    if (nrow(x) == 1) return(st_set_geometry(dummy, "geometry"))

    idx <- seq_len(nrow(x) - 1)
    fun <- function(i) {
        m <- rbind(x[i, c("lon", "lat")], x[i + 1, c("lon", "lat")])
        data.frame(geometry = st_sfc(st_linestring(as.matrix(m))),
                   route = paste(x$name[i], "-", x$name[i + 1]),
                   time  = round(as.numeric(x$first_seen[i + 1] - x$last_seen[i],
                                      units = "mins")))
    }
    st_set_geometry(do.call(rbind, lapply(seq_len(nrow(x) - 1), fun)), "geometry")
}

