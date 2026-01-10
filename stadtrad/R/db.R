

#' Connecting to Existing Stadtrad Database
#'
#' Connects to sqlite3 database and checks if the database
#' is as expected before returning a database connection.
#'
#' @param file Path to the sqlite3 database.
#'
#' @return Returns an object of class `SQLiteConnection`.
#'
#' @author Reto
#' @export
#'
#' @importFrom RSQLite dbConnect SQLite dbListTables dbDisconnect
sr_connect <- function(file) {
    stopifnot(
        "argument 'file' must be single character (path)" =
            is.character(file) && length(file) == 1L,
        "file does not exist" = isTRUE(file.exists(file))
    )

    con <- tryCatch(dbConnect(SQLite(), file, extended_types = TRUE),
             error = function(e) stop("Problems connecting to sqlite3 db:", 3)
    )

    # Checking tables, if the tables do not match our
    # expectation we know there is something wrong.
    expected_tables <- c("bikes", "places", "rentals")
    available_tables <- dbListTables(con)

    if (!all(expected_tables %in% available_tables)) {
        dbDisconnect(con)
        stop("Could not find all expected tables:", paste(expected_tables, collapse = ", "))
    }

    # Returning database connection handler
    return(con)
}

#' Loading Data From Database
#'
#' @param con An SQLiteConnection object as returned by [sr_connect].
#' @param numbers If `NULL` (default), the information of all available bikes
#'        (by number; bike ID) in the defined period (see argument `start`/`end`) will
#'        be returned. Can be a numeric/integer vector of length `>0` to limit the
#'        return to specific bike number(s).
#' @param start `NULL` or an object which can be converted to POSIXct.
#'        If set, only data after this point in time are loaded.
#' @param end `NULL` or an object which can be converted to POSIXct.
#'        If set, only data before (or equal) to this time are returned.
#' @param tz time zone used to convert `start` and/or `end` to POSIXct.
#'        Used to (i) transform `start` and `end` to POSIXct if needed,
#'        as well as to properly convert the data returned.
#'
#' @examples
#' \dontrun{
#' ## Connecting to database
#' con <- sr_connect("stadtrad.db")
#'
#' ## Define date range
#' start <- as.POSIXct("2025-12-22 23:00", tz = "Europe/Vienna")
#' end   <- as.POSIXct("2025-12-24 01:00", tz = "Europe/Vienna")
#'
#' ## Loading all bike information in time period
#' d <- sr_load_data(con, start = start, end = end, tz = "CET") 
#'
#' ## Loading data for one specific bike by ID (number)
#' d <- sr_load_data(con, numbers = 340000, start = start, end = end, tz = "CET")
#' }
#'
#' @return If no data are found, `NULL` is returned. Else an object of class
#' `c("srbike", "sf", "data.frame")`, i.e., a simple features data frame
#' with an additional stadtrad bike position class to provide S3 support.
#'
#' @author Reto
#' @export
#'
#' @importFrom RSQLite dbGetQuery
#' @importFrom sf st_as_sf st_crs
sr_load_data <- function(con, numbers = NULL, start = NULL, end = NULL, tz = "") {

    stopifnot(
        "argument 'con' must be of class SQLiteConnection" = inherits(con, "SQLiteConnection"),
        "argument 'numbers' must be NULL or a numeric vector of length > 0" =
            is.null(numbers) | (is.numeric(numbers) & length(numbers) > 0)
    )
    if (!is.null(numbers)) numbers <- unique(as.integer(numbers))

    if (!is.null(start) & !inherits(start, "POSIXt"))
        start <- tryCatch(as.POSIXct(start[1L], tz = tz),
                          error = function(e) stop("Error converting 'start'": e))
    if (!is.null(end) & !inherits(end, "POSIXt"))
        end <- tryCatch(as.POSIXct(end[1L], tz = tz),
                          error = function(e) stop("Error converting 'end'": e))

    if (!is.null(start) && !is.null(end))
        stopifnot("'start' must be before 'end'" = start < end)

    # Auxilary function to create the required SQLite query
    get_query <- function(start, end) {
        sql <- "SELECT b.first_seen, b.last_seen, b.number,
                b.bike_type, b.active, p.name, p.lon, p.lat
                FROM bikes AS b
                LEFT JOIN places AS p ON p.id = b.place_id"
        # sql <- gsub("\\s{2,}", " ", sql)
        where <- list()
        if (!is.null(start))
            where <- c(where, sprintf("min(b.first_seen, b.last_seen) > %d", as.integer(start)))
        if (!is.null(end))
            where <- c(where, sprintf("max(b.first_seen, b.last_seen) <= %d", as.integer(end)))

        # Number search: ensure 'number' is handled as integer, not index
        if (!is.null(numbers))
            where <- c(where, paste0("b.number + 0 IN (", paste(numbers, collapse = ", "), ")"))

        # Gluing together the where clause
        if (length(where) > 0)
            sql <- paste(sql, "WHERE", paste(where, collapse = "\n   AND "))

        return(paste(sql, "\n     ORDER BY b.number, b.first_seen"))
    }

    sql <- get_query(start, end)
    ##cat("------------------\n", sql, "\n----------------\n")

    # Requesting data
    data <- tryCatch(dbGetQuery(con, sql),
                     error = function(e) stop("Error querying data:", e))
    if (nrow(data) == 0) return(NULL)

    # Transforming first/last seen
    data$first_seen <- as.POSIXct(data$first_seen, tz = tz)
    data$last_seen  <- as.POSIXct(data$last_seen, tz = tz)

    # Coerce to simple feature data.frame
    data <- st_as_sf(data, coords = c("lon", "lat"), crs = st_crs(4326))

    return(structure(data, class = c("srbike", class(data))))
}


#' @rdname sr_load_data
#'
#' @exportS3Method plot srbike
#'
#' @importFrom sf st_coordinates
#' @importFrom stats setNames
plot.srbike <- function(x, ...) {

    fn <- function(x) {
        # Ensuring order
        x <- x[order(x$first_seen), ]
        geom   <- st_geometry(x)
        coord  <- st_coordinates(geom)
        distkm <- 1/1000 * c(NA_real_, st_distance(geom[-1], geom[-length(geom)], by_element = TRUE))
        timeh  <- c(NA_integer_, as.numeric(x$first_seen[-1L] - x$last_seen[-nrow(x)], unit = "hours"))
        data.frame(number     = x$number,
                   fist_seen  = x$first_seen,
                   last_seen  = x$last_seen,
                   lon        = coord[, 1L],
                   lat        = coord[, 2L],
                   dist_km    = round(distkm, 3),
                   parked_min = as.numeric(x$last_seen - x$first_seen, unit = "mins"),
                   timediff_h = round(timeh, 3),
                   speed_kmh  = round(distkm / timeh, 3))
    }

    x <- setNames(lapply(split(x, x$number), fn), unique(x$number))
    return(x)

}

#' Converting Stadtrad Bike Positions to Dataframe (Long Format)
#'
#' @param x Object of class `srbike` as returned by [sr_load_data()].
#'
#' @author Reto
#' @export
#'
#' @importFrom sf st_coordinates
sr_as_df <- function(x, ...) {
    stopifnot("argument 'x' must be an object of class 'srbike'" = 
              inherits(x, "srbike"))

    df <- data.frame(number = rep(x$number, 4L),
                     coord  = rep(as.vector(st_coordinates(x)), each = 2L),
                     ctype  = rep(c("longitude", "latitude"), each = nrow(x) * 2L),
                     timestamp = rep(c(x$first_seen, x$last_seen), each = 2L))

    return(df[order(df$timestamp), ])
}


