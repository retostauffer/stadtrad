


#' Calculating Bike Trips
#'
#' Uses the raw data to calculate bike trips, i.e., calculate
#' when a bike disapeared at a certain location and then popped up
#' at another one (considered a rented trip).
#'
#' @param x object of class `ri_data` as returned by
#'        [ri_read_jsons()], [ri_read_zip()], or [ri_read_zips()].
#' @param boundary either `NULL` or a single `sfc_POLYGON` as returned by
#'        [ri_innsbruck()]. If set all positions not within the polygons
#'        surface are deleted.
#' @param cores `NULL` (default) or integer, not used by `ri_read_json`.
#'        Number of cores to used with `parallel::mclapply`. If `NULL` it takes all but
#'        two available cores (topped of by the maximum number of json files to be
#'        processed).
#' @param verbose defaults to `FALSE`.
#' @param ... S3 method `c.tp_trips`: one or multiple `ri_trip`
#'        objects to be combined. They must have the same time zone and must not
#'        overlap, else an error is thrown. For S3 method `plot.tp_trips`: forwarded
#'        to barplot function.
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
#' The `c.ri_trips` method allows to combine multiple objects of this class.
#' Will also resolve potential open trips.
#'
#' @importFrom tibble as_tibble
#' @export
#' @author Reto
ri_trips <- function(x, boundary = ri_innsbruck(), cores = NULL, verbose = FALSE, ...) {

    if (!is.null(cores)) cores <- as.integer(cores)[[1L]]
    verbose <- as.logical(verbose)[1L]

    stopifnot(
        "argument `x` must be an object of class ri_data" = inherits(x, "ri_data"),
        "argument `boundary` must be NULL or a single `sfc_POLYGON`" =
            is.null(boundary) || (inherits(boundary, "sfc_POLYGON") && length(boundary) == 1L),
        "argument `cores` must be NULL or integer" =
            is.null(cores) || (is.integer(cores) && length(cores) == 1L),
        "argument `verbose` must evaluate to TRUE or FALSE" =
            isTRUE(verbose) || isFALSE(verbose)
    )

    # Setting number of cores
    if (!is.null(cores) && cores < 1L) stop("`cores` must be positive")
    if (is.null(cores)) cores <- pmax(1L, detectCores())

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

    warning("Idea was to create dedicated sf columns here and then apply boundary filter")

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
    tz <- attr(x$datetime, "tzone")

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

    ## Adding open trips starting at current time period and
    ## at the end of the time period
    start <- data.frame(number         = x$number[1L],
                        start_place_id = NA_integer_,
                        start_datetime = as.POSIXct(NA, tz = tz),
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
                        end_datetime   = as.POSIXct(NA, tz = tz),
                        end_name       = NA_character_,
                        end_lng        = NA_real_,
                        end_lat        = NA_real_)

    # Combine open and closed trips and return
    return(do.call(rbind, list(start, res, end)))

}






#' @rdname ri_trips
#' @exportS3Method c ri_trips
c.ri_trips <- function(..., cores = NULL) {

    # Evaluating cores
    if (!is.null(cores)) cores <- as.integer(cores)[[1L]]
    stopifnot(
        "argument `cores` must be NULL or integer" =
            is.null(cores) || (is.integer(cores) && length(cores) == 1L)
    )

    # Setting number of cores
    if (!is.null(cores) && cores < 1L) stop("`cores` must be positive")
    if (is.null(cores)) cores <- pmax(1L, detectCores())

    ## Checking arguments to be combined
    args <- list(...)
    cls <- sapply(args, function(x) inherits(x, "ri_trips"))
    stopifnot(
        "all objects to be combined must be of class `ri_trips`" = all(cls)
    )
    if (length(args) == 1L) return(args[[1L]])

    ## (1) Make sure they have the same time zone
    tzs <- sapply(args, function(x) attr(x$start_datetime, "tzone"))
    stopifnot(
        "datetime information not all have same time zone" = all(tzs == tzs[[1L]])
    )

    ## (2) Make sure they do not overlap timewise; for that we check the
    ##     datetime range of the different objects and sort them accordingly first.
    dtcols <- c("start_datetime", "end_datetime")
    rng <- lapply(args, function(x) range(unlist(x[, dtcols]), na.rm = TRUE))
    rng <- lapply(rng, function(x) as.POSIXct(x, tz = tzs[[1L]]))

    # Sort timewise (oldest first etc)
    idx  <- order(sapply(rng, function(x) x[[1L]]))
    args <- args[idx]; rng <- rng[idx]

    for (i in seq.int(2L, length(args))) {
        if (rng[[i]][[1L]] <= rng[[i - 1]][[2L]]) stop("time periods overlapping (stopping)")
    }

    ## (3) We first combine all data (and delete args; no longer needed)
    res <- do.call(rbind, args)
    rm(args)

    ## (3) Check if there are any open trips. If so, we try to resolve them.
    if (sum(is.na(res[, dtcols]) > 0L)) {
        res <- split(res, res$number)
        res <- mclapply(res, fix_open_trips, dtcols = dtcols, mc.cores = cores)
        res <- do.call(rbind, res)
    }

    return(res)
}


# Auxilary function to resolve open trips. Will check for
# rows with 'open end' followed by rows with 'open start' to
# complete the trip by combining these two rows.
fix_open_trips <- function(x, dtcols) {
    if (sum(is.na(x[, dtcols])) == 0L) return(x)

    ## Find rows with open end trips and open start trips
    idx_e <- which(is.na(x$end_datetime))
    idx_s <- which(is.na(x$start_datetime))

    ## Columns with 'start_*' information
    scols <- grep("^start_*", names(x), value = TRUE)

    ## If we find two consecutive rows with open end and open start
    ## we can combine them into one closed trip
    idx <- idx_e[idx_e %in% (idx_s - 1L)]

    # Fill row x[i + 1,] with data from x[i, ] to complete the trip
    for (i in idx) x[i + 1, scols] <- x[i, scols]

    ## Now remove 'idx' rows (which we filled into idx + 1)
    return(x[-idx, ])
}


#' @param n positive integer, defaults to 15L.
#'
#' @importFrom graphics par barplot
#' @importFrom utils head
#' @exportS3Method plot ri_trips
#' @rdname ri_trips
plot.ri_trips <- function(x, n = 15L, ...) {
    n <- as.integer(n)[[1L]]
    stopifnot(
        "argument `n` must evaluate to positive integer" =
            (is.integer(n) && length(n) == 1L) && n > 0L
    )

    x <- x[!is.na(x$start_datetime) & !is.na(x$end_datetime), ]
    if (nrow(x) == 0L) stop("No finished trips in data set")
    n_total <- nrow(x)

    ## Evaluate additional arguments
    args <- list(...)

    ## Create trips and get n most used ones
    x <- interaction(x$start_name, x$end_name, sep = " - ")
    x <- table(x) |> sort(decreasing = TRUE) |> head(n = n)

    main <- if (!is.null(args$main)) {
        args$main
    } else {
        sprintf("Top %d trips\n%d out of %d total trips (%s)", n,
                sum(x), n_total, sprintf("%.1f percent", 100 * sum(x) / n_total))
    }

    # Plotting
    hold <- par(no.readonly = TRUE); on.exit(par(hold))

    par(mar = c(20, 4.1, 2.1, 1))
    bp <- barplot(x, las = 2, main = main, ...)

    invisible(bp)
}



