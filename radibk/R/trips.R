


#' Calculating Bike Trips
#'
#' Uses the raw data to calculate bike trips, i.e., calculate
#' when a bike disapeared at a certain location and then popped up
#' at another one (considered a rented trip).
#'
#' @param x object of class `ri_data` as returned by
#'        [ri_read_jsons()], [ri_read_zip()], or [ri_read_zips()].
#' @param cores `NULL` (default) or integer, not used by `ri_read_json`.
#'        Number of cores to used with `parallel::mclapply`. If `NULL` it takes all but
#'        two available cores (topped of by the maximum number of json files to be
#'        processed).
#' @param verbose defaults to `FALSE`.
#' @param ... currently unused.
#'
#' @return Object of class `c("ri_trips", "tbl", "data.frame")`
#' containing bike trips as well as 'open trips' (see below).
#' Besides the bike `number` (the identifier) the data frame
#' contains `*_place_id`, `*_daetetime`, `*_name`, `*_lng`, and `*_lat`
#' for both the start and the end of the trip. In addition, the trip
#' duration is reported in minutes (rounded to closest minutes).
#'
#' Besides closed trips (where we know the start and end) the data frame
#' also contains "open trips". These are always added at the start of
#' the data period (with respect to 'datetime') and end of the data period
#' and are required to capture potential bike trips which started before
#' the earliest observation into the period the data covers, as well
#' as trips starting within the data period but have not finished at
#' the end of the period.
#' These are later used when combining multiple `ri_trips` objects
#' (see S3 method `c.ri_trips`).
#'
#' @importFrom tibble as_tibble
#' @export
#' @author Reto
ri_trips <- function(x, cores = NULL, verbose = FALSE, ...) {

    if (!is.null(cores)) cores <- as.integer(cores)[[1L]]
    verbose <- as.logical(verbose)[1L]

    stopifnot(
        "argument `x` must be an object of class ri_data" = inherits(x, "ri_data"),
        "argument `cores` must be NULL or integer" =
            is.null(cores) || (is.integer(cores) && length(cores) == 1L),
        "argument `verbose` must evaluate to TRUE or FALSE" =
            isTRUE(verbose) || isFALSE(verbose)
    )

    # Setting number of cores
    if (!is.null(cores) && cores < 1L) stop("`cores` must be positive")
    if (is.null(cores)) cores <- pmin(length(x), pmax(1L, detectCores()))

    if (verbose) message("Calculating trips based on ", nrow(x$bikes), " observations")

    ## (1) Unique places
    places <- x$places[ c("lng", "lat", "uid", "name")] |> unique()

    ## (2) Splitting the data by bike
    tmp <- split(x$bikes[, c("number", "place_id", "datetime")], x$bikes$number)
    if (verbose) message(" - Found ", length(tmp), " different bikes in data set")

    ## (3) Calculating trips
    tmp <- do.call(rbind, mclapply(tmp, calculate_trip, mc.cores = cores))

    ## (4) Enriching with place details
    start_idx <- match(tmp$start_place_id, places$uid)
    end_idx   <- match(tmp$end_place_id,   places$uid)

    tmp$start_name <- places$name[start_idx]
    tmp$start_lng  <- places$lng[start_idx]
    tmp$start_lat  <- places$lat[start_idx]

    tmp$end_name   <- places$name[end_idx]
    tmp$end_lng    <- places$lng[end_idx]
    tmp$end_lat    <- places$lat[end_idx]

    ## (5) Calculating trip duration (rounded to full minutes)
    tmp$duration_min <- as.numeric(tmp$end_datetime - tmp$start_datetime,
                                   units = "mins") |> round()

    rownames(tmp) <- NULL
    tmp <- as_tibble(tmp)

    return(tmp |> structure(class = c("ri_trips", class(tmp))))
}

calculate_trip <- function(x) {

    ## Ensure unique data and order
    x <- unique(x)
    x <- x[order(x$datetime), ]

    ## Check when location changed
    idx <- which(!c(diff(x$place_id), NA) == 0L)
    if (length(idx) == 0L) return(NULL) # Never moved

    warning("I think I have to store first and last observation with NA as start and end to later on combine multiple 'blocks' (days) not to miss trips between archive sets")

    ## Generate new data frame with start and end trip location
    res   <- data.frame(number         = x$number[1L],
                        start_place_id = x$place_id[idx],
                        start_datetime = x$datetime[idx],
                        start_name     = NA_character_,
                        start_lng      = NA_real_,
                        start_lat      = NA_real_,
                        end_place_id   = x$place_id[idx + 1L],
                        end_datetime   = x$datetime[idx + 1L],
                        end_name       = NA_character_,
                        end_lng        = NA_real_,
                        end_lat        = NA_real_)

    ## Adding open trips
    start <- data.frame(number         = x$number[1L],
                        start_place_id = NA_integer_,
                        start_datetime = NA_real_,
                        start_name     = NA_character_,
                        start_lng      = NA_real_,
                        start_lat      = NA_real_,
                        end_place_id   = x$place_id[1L],
                        end_datetime   = x$datetime[1L],
                        end_name       = NA_character_,
                        end_lng        = NA_real_,
                        end_lat        = NA_real_)

    end   <- data.frame(number         = x$number[1L],
                        start_place_id = x$place_id[nrow(x)],
                        start_datetime = x$datetime[nrow(x)],
                        start_name     = NA_character_,
                        start_lng      = NA_real_,
                        start_lat      = NA_real_,
                        end_place_id   = NA_integer_,
                        end_datetime   = NA_real_,
                        end_name       = NA_character_,
                        end_lng        = NA_real_,
                        end_lat        = NA_real_)

    # Combine open and closed trips and return
    return(do.call(rbind, list(start, res, end)))

}
