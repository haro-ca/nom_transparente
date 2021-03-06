library(tidyverse)

# Init ====
params_frame <- read_csv("data/raw/entidades.csv") %>%
    janitor::clean_names() %>%
    select(sector_id, unidad_responsable, 
           nombre_corto)

url <- "https://dgti-ejc-ftp.k8s.funcionpublica.gob.mx/APF"

# Ciclo de descarga ====
pwalk(params_frame,
     ~ download.file(glue::glue("{url}/{..1}_{..2}.csv"),
              glue::glue("data/raw/nomina/{..3}.csv")))
