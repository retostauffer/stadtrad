

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
#' @author Reto
#' @export
#'
#' @importFrom RSQLite dbGetQuery
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

    get_query <- function(start, end) {
        sql <- "SELECT b.first_seen, b.last_seen, b.number,
                b.bike_type, b.active, p.name, p.lon, p.lat
                FROM bikes AS b
                LEFT JOIN places AS p ON p.id = b.place_id"
        # sql <- gsub("\\s{2,}", " ", sql)
        where <- list()
        if (!is.null(start))
            where <- c(where, sprintf("min(b.first_seen, b.last_seen) < %d", as.integer(start)))
        if (!is.null(end))
            where <- c(where, sprintf("max(b.first_seen, b.last_seen) >= %d", as.integer(end)))

        # Number search: ensure 'number' is handled as integer, not index
        if (!is.null(numbers))
            where <- c(where, paste0("b.number + 0 IN (", paste(numbers, collapse = ", "), ")"))

        # Gluing together the where clause
        if (length(where > 0))
            sql <- paste(sql, "WHERE", paste(where, collapse = "\n   AND "))

        return(paste(sql, "\n     ORDER BY b.number, b.first_seen"))
    }

    sql <- get_query(start, end)
    cat("------------------\n", sql, "\n----------------\n")

    # Requesting data
    data <- tryCatch(dbGetQuery(con, sql),
                     error = function(e) stop("Error querying data:", e))

    # Transforming first/last seen
    data$first_seen <- as.POSIXct(data$first_seen, tz = tz)
    data$last_seen  <- as.POSIXct(data$last_seen, tz = tz)

    # Coerce to simple feature data.frame
    data <- st_as_sf(data, coords = c("lon", "lat"), crs = st_crs(4326))

    return(data)
}

