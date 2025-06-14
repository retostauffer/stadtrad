


library("RSQLite")
library("leaflet")
library("sf")

con <- dbConnect(SQLite(), "../stadtrad.db")

sql <- "SELECT b.first_seen, b.last_seen, b.number,
        b.bike_type, b.active, p.name, p.lon, p.lat
        FROM bikes AS b
        LEFT JOIN places AS p ON p.id = b.place_id
        ORDER BY b.number, b.first_seen"

data <- dbGetQuery(con, sql)
print(dim(data))
data <- transform(data,
                  first_seen = as.POSIXct(first_seen, tz = "utc"),
                  last_seen  = as.POSIXct(last_seen, tz = "utc"))


dbDisconnect(con)

tab <- sort(table(data$number))
#x <- subset(data, number == 340308)
#leaflet() %>% addTiles() %>% addPolylines(lng = ~lon, lat = ~lat, data = x)
#x <- subset(data, number == 340139)
#leaflet() %>% addTiles() %>% addPolylines(lng = ~lon, lat = ~lat, data = x)


x <- subset(data, number == 340139)
map <- leaflet() %>% addTiles()
numbers <- as.integer(names(tab)[tab > 5])

geoms <- list()
for (num in numbers) {
    tmp <- subset(data, number == num)
    geoms[[length(geoms) + 1]] <- st_linestring(as.matrix(tmp[, c("lon", "lat")]))
}
sf <- data.frame(geometry = st_sfc(geoms, crs = st_crs(4326)))
sf$color <- rainbow(nrow(sf))
#sf$color <- hcl.colors(nrow(sf), "Set 3")
st_geometry(sf) <- "geometry"

leaflet() %>% addTiles() %>% addPolylines(data = sf, color = ~color, opacity = 0.9)
plot(sf)


leaflet() %>% addTiles() %>% addPolylines(data = sf[sample(1:nrow(sf), 1),], color = ~color, opacity = 0.9)






