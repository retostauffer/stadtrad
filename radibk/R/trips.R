


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
#'
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

    return(tmp)
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
    data.frame(number         = x$number[1L],
               start_place_id = x$place_id[idx],
               start_datetime = x$datetime[idx],
               start_name     = NA_character_,
               start_lng      = NA_real_,
               start_lat      = NA_real_,
               end_place_id   = x$place_id[idx + 1],
               end_datetime   = x$datetime[idx + 1],
               end_name       = NA_character_,
               end_lng        = NA_real_,
               end_lat        = NA_real_)

}
