


#' Stadtteile Innsbruck
#'
#' Simple feature data frame with the districts of the city of Innsbruck.
#'
#' @data



#' Innsbruck Outline
#'
#' Citybikes in Innsbruck are only allowed to be used within the cities
#' boundaries. The `radibk` package is shipped with a data set including
#' the districts of the town. This function loads the data set and returns
#' a single polygon with the city outline to filter for out-of-bound
#' coordinates reported by the API (happens rarely, but happens).
#'
#' @param ... unused.
#'
#' @return Returns a single `sfc_POLYGON` used to subset bike positions
#' in [ri_trips()].
#'
#' @importFrom sf st_buffer st_union st_boundary
#' @export
ri_innsbruck <- function(...) {
    data("stadtteile", package = "radibk")
    return(stadtteile |> st_buffer(20) |> st_union())
}

