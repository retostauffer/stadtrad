
#' Loading Bike Data
#'
#' @param con database connection. Typically an `SQLiteConnection`
#'        but should be possible to easily extend to MariaDB/MySQL (untested).
#' @param tz chracter, time zone. Defaults to `ri_timezone()`.
#'
#' @details
#' The object returned contains information about all recorded bike stops, i.e.,
#' when we have seen a certain bike at a specific place.
#'
#' * `first_seen`: Date and time he bike has first seen at the station.
#' * `last_seen`: Date and time the bike has last been seen at the station.
#' * `number`: Bike identifier.
#' * `bike_type`: Type of bike.
#' * `active`: Status.
#' * `name`: Name of the station the bike is located.
#' * `lon`/`lat`: Location of the bike (station/place).
#'
#' @return Data frame with details about bikes locations (see 'Details').
#'
#' @author reto
#' @export
ri_bikes <- function(con, tz = ri_timezone()) {
    stopifnot(
        "'con' must be an SQLiteConnection (for now)" = inherits(con, "SQLiteConnection"),
        "'tz' must be character" = is.character(tz) && length(tz) == 1L,
        "'tz' not found in OlsonNames()" = tz %in% OlsonNames()
    )

    sql <- "SELECT b.first_seen, b.last_seen, b.number,
            b.bike_type, b.active, p.name, p.lon, p.lat
            FROM bikes AS b
            LEFT JOIN places AS p ON p.id = b.place_id
            ORDER BY b.number, b.first_seen"
    res <- dbGetQuery(con, sql)
    res <- transform(res,
                     first_seen = as.POSIXct(first_seen, tz = tz),
                     last_seen  = as.POSIXct(last_seen, tz = tz),
                     active     = as.logical(active))

    res$time <- round(1 + with(res, as.numeric(last_seen - first_seen, units = "mins")))
    class(res) <- c("ri_bikes", class(res))
    return(res)
}


#' Calculate Bike Trips
#'
#' The object returned by `ri_bikes()` contains the stops the bikes
#' have been recorded.
#'
#' @param x object of class `ri_bikes`.
#'
#' @importFrom dplyr group_by `%>%` group_modify
#' @importFrom sf st_as_sf st_crs st_sfc st_geometrycollection
#' @author reto
#' @export
ri_biketrips <- function(x) {
    stopifnot(
        "'x' must be of class ri_bikes" = inherits(x, "ri_bikes")
    )

    x <- st_as_sf(x, coords = c("lon", "lat"), crs = st_crs(4326))

    fn <- function(x) {
        # Dummy return if we can't calculate trips
        dummy <- data.frame(trip = NA_character_,
                            line = st_sfc(st_geometrycollection()))
        if (nrow(x) < 2L) return(dummy)

        # Else calculating the trips
        i0 <- seq.int(1, nrow(x) - 1); i1 <- i0 + 1L
        co <- st_coordinates(st_geometry(x))
        line <- lapply(i0, function(i) st_linestring(rbind(co[i, ], co[i + 1, ])))
        x <- data.frame(trip = paste(x$name[i0], ">", x$name[i1]),
                        line = st_sfc(line),
                        start = x$last_seen[i0],
                        end   = x$first_seen[i1])
        x$duration_min <- with(x, round(as.numeric(end - start, unit = "mins")))
        return(x)
    }
    x <- x[order(x$first_seen), ] %>% group_by(number) %>%
                group_modify(~fn(.x)) %>% as.data.frame()
    st_geometry(x) <- "geometry"
    return(x)

}



