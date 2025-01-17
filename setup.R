if (!require("pacman")) install.packages("pacman")
pacman::p_load(
  haven,      # Para leer archivos SPSS
  dplyr,      # Manipulación de datos
  ggplot2,    # Visualización
  scales,     # Formateo de escalas
  kableExtra  # Tablas formateadas
)

# Rutas relativas a los datos
ruta_casen <- "data/casen_reducida.rds"
ruta_ebs <- "data/Base de datos EBS 2021 SPSS.sav"

# Función para verificar disponibilidad de datos
check_data <- function() {
  required_files <- c(ruta_casen, ruta_ebs)
  missing_files <- required_files[!file.exists(required_files)]
  
  if (length(missing_files) > 0) {
    stop("Faltan los siguientes archivos:\n", 
         paste(missing_files, collapse = "\n"))
  }
}