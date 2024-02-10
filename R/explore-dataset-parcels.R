
# SETUP -------------------------------------------------------------------

library(tidyverse)
library(sf)
library(mapview)
library(DBI)
library(RPostgres)
library(janitor) 
library(tictoc)
library(glue)
library(leafpop)
library(units)
library(extrafont)
library(skimr)

# font_import()  # You only need to run this once, or again if you add new fonts
# loadfonts(device = "win")
font_add_google("Roboto", "roboto")
showtext_auto()

# Define a function that creates your custom theme
theme_custom <- function(base_size = 20) {
  theme_grey(base_size = base_size) +  # Start with a predefined theme, optional
    theme(
      text = element_text(size = base_size),  # General text size
      axis.title = element_text(size = base_size * 1.2),  # Axes titles
      axis.text = element_text(size = base_size),  # Axes text (ticks)
      legend.title = element_text(size = base_size * 1.1),  # Legend title
      legend.text = element_text(size = base_size * 0.9),  # Legend text
      plot.title = element_text(size = base_size * 1.4,face = "bold"),  # Plot title
      plot.subtitle = element_text(size = base_size * 1.2),  # Plot subtitle
      plot.caption = element_text(size = base_size * 0.8),  # Plot caption
      plot.margin = margin(10, 10, 10, 10)  # Adjust plot margins (optional)
    )
}


# LOAD PARCEL DATA --------------------------------------------------------

p_new_max_far <- st_read("data_outputs/2024-tod-bill.gpkg",
                         layer = "tod-parcels")

# EXPLORE THE NEW MAX FAR DATA ----

p_new_max_far |> 
  st_drop_geometry() |> 
  select(zoning_new_max_far_diff,
         zoning_new_max_far_additional) |> 
  skim()

p_new_max_far |> 
  st_drop_geometry() |> 
  summarise(n = n(),
            n_addtl = scales::percent(
              sum(zoning_new_max_far_additional > 0,
                  na.rm = TRUE) / n),
            median = median(zoning_new_max_far_diff,
                            na.rm = TRUE),
            area_addtl = scales::percent(
              sum(parcel_area[zoning_new_max_far_additional > 0],
                  na.rm = TRUE) / sum(parcel_area, na.rm = TRUE)),
            wt_mean_far_diff = round(
              digits = 2,
              x =  weighted.mean(zoning_new_max_far_diff,
                                 w = parcel_area,
                                 na.rm = TRUE)),
            wt_mean_far_diff_addtl = round(
              digits = 2,
              x = weighted.mean(zoning_new_max_far_additional,
                            w = parcel_area,
                            na.rm = TRUE)),
            ) |> 
  glimpse()

p_new_max_far |> 
  st_drop_geometry() |> 
  group_by(zoning_new_max_far) |> 
  summarise(n = n(),
            area = sum(parcel_area_mi2, na.rm = TRUE),
            n_addtl = scales::percent(
              sum(zoning_new_max_far_additional > 0,
                  na.rm = TRUE) / n),
            median = median(zoning_new_max_far_diff,
                            na.rm = TRUE),
            area_addtl = scales::percent(
              sum(parcel_area[zoning_new_max_far_additional > 0],
                  na.rm = TRUE) / sum(parcel_area, na.rm = TRUE)),
            wt_mean_far_diff = round(
              digits = 2,
              x =  weighted.mean(zoning_new_max_far_diff,
                                 w = parcel_area,
                                 na.rm = TRUE)),
            wt_mean_far_diff_addtl = round(
              digits = 2,
              x = weighted.mean(zoning_new_max_far_additional,
                                w = parcel_area,
                                na.rm = TRUE)),
  ) |> 
  glimpse()


p_new_max_far |> 
  st_drop_geometry() |> 
  slice_min(zoning_new_max_far_diff) |> glimpse()


p_new_max_far |> 
  st_drop_geometry() |> 
  group_by(factor(zoning_new_max_far)) |> 
  summarise(mean = mean(zoning_new_max_far_diff,na.rm = TRUE),
            median = median(zoning_new_max_far_diff,na.rm = TRUE),
            n = n()) |> 
  mutate(label = paste("Mean:", round(mean, 2),
                       "\nMedian:", round(median, 2),
                       "\nN:", n))



# HISTOGRAM: PARCELS BY COUNT ----

hist_cnt <- ggplot(data = p_new_max_far) + 
  aes(x = zoning_new_max_far_diff) + 
  geom_histogram(binwidth = 0.5, color = "grey20", fill = "grey80") + 
  geom_vline(xintercept = 0, color = "tomato",linetype = 2) +
  scale_y_continuous(labels = scales::label_comma()) +
  xlim(-15,5) +
  labs(title = "Change in Maximum FAR",
       subtitle = glue("Histogram bars represent # of parcels,\nn = {scales::comma(nrow(p_new_max_far) - 14637)}"),
       y = "# of Parcels",
       x = "Change in Maximum FAR (0.5 increments)") +
  theme()

ggsave("plots/change-in-max-far-cnt.png",
       plot = hist_cnt,
       width = 6, height = 4, dpi = 300)

# HISTOGRAM: PARCEL AREA (MI^2) ----

hist_area <- ggplot(data = p_new_max_far) + 
  aes(x = zoning_new_max_far_diff,
      weight = parcel_area_mi2) + 
  geom_histogram(binwidth = 0.5, color = "grey20", fill = "grey80") + 
  geom_vline(xintercept = 0, color = "tomato",linetype = 2) +
  scale_y_continuous(labels = scales::label_comma()) +
  xlim(-15,5) +
  labs(title = "Change in Maximum FAR",
       subtitle = glue("Histogram bars represent area of land (sq. miles), n = {scales::comma(nrow(p_new_max_far) - 9439)}"),
       y = "Land Area (sq. miles)",
       x = "Change in Maximum FAR (0.5 increments)") 

ggsave("plots/change-in-max-far-area.png",
       plot = hist_area,
       width = 6, height = 4, dpi = 300)

# HISTOGRAM: PARCEL AREA (MI^2) BY FAR TYPE----

hist_area_by_far <- ggplot(data = p_new_max_far) + 
  aes(x = zoning_new_max_far_diff,
      group = factor(zoning_new_max_far),
      weight = parcel_area_mi2) + 
  geom_histogram(binwidth = 0.5, color = "grey20", fill = "grey80") + 
  geom_vline(xintercept = 0, color = "tomato",linetype = 2) +
  facet_wrap(~zoning_new_max_far,ncol = 1,scales = "fixed") + 
  scale_y_continuous(labels = scales::label_comma()) +
  xlim(-10,5) +
  labs(title = "Change in Maximum FAR by Station Area Type",
       subtitle = "Histogram bars represent area of land (sq. miles)",
       y = "Parcel Area (sq. miles)",
       x = "Change in Maximum FAR (0.5 increments)") 

ggsave("plots/change-in-max-far-area-by-far.png",
       plot = hist_area_by_far,
       width = 6, height = 4, dpi = 300)

ggplot(data = p_new_max_far) + 
  aes(x = zoning_new_max_far_diff, group = factor(zoning_new_max_far)) + 
  geom_histogram(binwidth = 0.5, color = "grey20", fill = "grey80") + 
  geom_vline(xintercept = 0, color = "tomato",linetype = 2) +
  facet_wrap(~zoning_new_max_far,ncol = 1) +
  scale_y_continuous(labels = scales::label_comma()) +
  xlim(-5,5) + 
  labs(title = "Change in Maximum FAR by Station Area Type",
       subtitle = "Histogram bars represent # of parcels",
       y = "# of Parcels",
       x = "Change in Maximum FAR (0.5 increments)")



ggplot(data = p_new_max_far) + 
  aes(x = zoning_new_max_far_diff, 
      color = factor(zoning_new_max_far),
      fill = factor(zoning_new_max_far)) + 
  geom_density(alpha = 0.5) +
  geom_vline(xintercept = 0, linetype = 2) +
  xlim(-5,5)

p_map_all <- p_new_max_far |> 
  select(zoning_juris,
         zoning_district_name,
         zoning_allow_resi,
         zoning_max_far,
         zoning_est_max_far,
         zoning_new_max_far,
         zoning_new_max_far_diff,
         zoning_new_max_far_additional,
         transit_within_lr_walkshed, 
         transit_within_sc_walkshed,
         transit_within_cr_walkshed,
         transit_within_brt_walkshed
  )

p_map <- p_map_all |> 
  filter(zoning_juris %in% "Everett")

mapview(p_map, 
        zcol = "zoning_new_max_far_additional", 
        popup = popupTable(p_map))

mapview(p_map_all, 
        zcol = "zoning_new_max_far_additional", 
        popup = FALSE)

p_map_all <- 
  p_new_max_far |> 
  select(zoning_new_max_far_additional)mapview()






# MAP: EVERETT ------------------------------------------------------------

p_bellevue <- p_new_max_far |> 
  filter(zoning_juris %in% "Bellevue")

p_bellevue_map <- p_bellevue |> 
  select(starts_with("transit"),
         zoning_district_name,
         zoning_new_max_far,
         zoning_new_max_far_diff,
         zoning_new_max_far_additional
         )

mapview(p_bellevue_map, 
        zcol = "zoning_new_max_far_additional")


# MAP: MERCER ISLAND ------------------------------------------------------

p_mercer_island <- p_new_max_far |> 
  filter(zoning_juris %in% "Mercer Island")

p_mercer_island_map <- p_mercer_island |> 
  select(starts_with("transit"),
         zoning_district_name,
         parcel_lu_code_category, 
         parcel_lu_code_use,
         zoning_est_max_lot_pct:zoning_new_max_far_additional
  )

mapview(p_mercer_island_map, 
        zcol = "zoning_new_max_far_additional")


# MAP: TACOMA ------------------------------------------------------

p_tacoma <- p_new_max_far |> 
  filter(zoning_juris %in% "Tacoma")

p_tacoma_map <- p_tacoma |> 
  select(starts_with("transit"),
         zoning_district_name,
         parcel_lu_code_category, 
         parcel_lu_code_use,
         zoning_est_max_lot_pct:zoning_new_max_far_additional
  )

mapview(p_tacoma_map, 
        zcol = "zoning_new_max_far_additional")
