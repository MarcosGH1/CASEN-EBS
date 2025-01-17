#ARQUITECTURA DE DATOS ETL

#1. EXTRACCIÓN
#Se sacan obtienen las bases de datos a partir de las fuentes de información de CASEN
#Y EBS. Específicamente se ocuparán las bases de datos EBS2021 Y CASEN 2020 (en pandemia)

#2. TRANSFORMACIÓN
#En primer lugar, la limpieza. Se eliminan NA y se corrigen erorres de formato. En segundo
#lugar, se realiza la normalización, en donde se normalizan los datos a un formato consistene.
#En tercer lugar, se realiza la integración, uniendo datos de distintas fuentes (ya mencionadas)

#3. CARGA
#Finalmente, se realiza el proceso de carga a la plataforma de github, el cual se puede
#ver con más detalle en en el archivo README.md.




#MARCOS, ACÁ AGREGO LAS VARIABLES FALTANTES DE LA EBS PARA CREAR ENTIDADES
# Realizar el merge manteniendo todas las variables
merged_data <- merge(
  casen,  # Mantenemos todas las variables de CASEN
  ebs %>% select(folio_casen, id_persona_casen, fexp, region, j3a_1, j3a_4, a3_4, j3a_5, c1_4, c1_5, c3_2, c6),  # Seleccionamos solo las variables necesarias de EBS
  by = c("folio_casen", "id_persona_casen"),
  suffixes = c("_casen", "_ebs")  # Para diferenciar variables duplicadas
)




#CREACIÓN ENTIDADES 

# ENTIDAD: 10% HOGARES DE MAYOR INGRESO
hogares_mayor_ingreso <- merged_data %>%
  select(id_persona_casen, folio_casen, elite)

# ENTIDAD: IDENTIFICACIÓN PERSONAL
identificacion_personal <- merged_data %>%
  select(id_persona_casen, folio_casen, elite, e6a, sexo, region, zona, v13)

# ENTIDAD: BIENESTAR SOCIAL
bienestar_social <- merged_data %>%
  select(id_persona_casen, folio_casen, elite, j3a_1, j3a_4, a3_4, j3a_5, c1_4, c1_5, c3_2, c6)
         
# ENTIDAD: TRABAJO
trabajo <- merged_data %>%
  select(id_persona_casen, folio_casen, elite, o9a, o15, o32, o34)

#VER NAs
anyNA(identificacion_personal)
anyNA(trabajo)
anyNA(bienestar_social)
colSums(is.na(bienestar_social))
         
#BORRO NA DE TRABAJO
trabajo <- trabajo %>%
  filter(!is.na(o15), !is.na(o32), !is.na(o34))

#BORRO NA DE BIENSTAR SOCIAL
bienestar_social <- bienestar_social %>%
  filter(!is.na(j3a_1), !is.na(j3a_4), !is.na(a3_4), !is.na(j3a_5), !is.na(c3_2),
!is.na(c6))


#RECODIFICAR VARIABLES

#ENTIDAD INDENTIFICACION_PERSONAL: 
#Variable situación vivienda
identificacion_personal <- identificacion_personal %>%
  mutate(v13_recodificado = case_when(
    v13 == 1 ~ "Propia",
    v13 == 2 ~ "Arrendada",
    v13 == 3 ~ "Cedida",
    v13 == 9 ~ "Usufructo (solo uso y goce)",
    v13 == 10 ~ "Ocupación irregular (de hecho)",
    v13 == 11 ~ "Poseedor irregular",
    TRUE ~ "Otro"  # Caso en que el valor no coincida con ninguno
  ))


#Variable nivel educacional
identificacion_personal <- identificacion_personal %>%
  mutate(e6a_recodificado = case_when(
    e6a == 1 ~ "Nunca asistió",
    e6a == 2 ~ "Sala cuna",
    e6a == 3 ~ "Jardín Infantil (Medio menor y Medio mayor)",
    e6a == 4 ~ "Prekinder / Kinder (Transición menor y Transición mayor)",
    e6a == 5 ~ "Educación Especial (Diferencial)",
    e6a == 6 ~ "Primaria o Preparatoria (Sistema antiguo)",
    e6a == 7 ~ "Educación Básica",
    e6a == 8 ~ "Humanidades (Sistema Antiguo)",
    e6a == 9 ~ "Educación Media Científico-Humanista",
    e6a == 10 ~ "Técnica Comercial, Industrial o Normalista (Sistema Antiguo)",
    e6a == 11 ~ "Educación Media Técnica Profesional",
    e6a == 12 ~ "Técnico Nivel Superior Incompleto (Carreras 1 a 3 años)",
    e6a == 13 ~ "Técnico Nivel Superior Completo (Carreras 1 a 3 años)",
    e6a == 14 ~ "Profesional Incompleto (Carreras 4 o más años)",
    e6a == 15 ~ "Profesional Completo (Carreras 4 o más años)",
    e6a == 16 ~ "Postgrado Incompleto",
    e6a == 17 ~ "Postgrado Completo",
    TRUE ~ "Otro"  # Caso en que el valor no coincida con ninguno
  ))


#ENTIDAD TRABAJO
#RENOMBRAR VARIABLE o9a
trabajo <- trabajo %>%
  rename(`Ocupacion u oficio` = o9a)


#RECODIFICAR VARIABLE SOBRE EL TIPO DE TRABAJO. 
trabajo <- trabajo %>%
  mutate(o15_recodificado = case_when(
    o15 == 1 ~ "Patrón o empleador",
    o15 == 2 ~ "Trabajador por cuenta propia",
    o15 == 3 ~ "Empleado u obrero del sector público (Gobierno Central o Municipal)",
    o15 == 4 ~ "Empleado u obrero de empresas públicas",
    o15 == 5 ~ "Empleado u obrero del sector privado",
    o15 == 6 ~ "Servicio doméstico puertas adentro",
    o15 == 7 ~ "Servicio doméstico puertas afuera",
    o15 == 8 ~ "FF.AA. y del Orden",
    o15 == 9 ~ "Familiar no remunerado",
    TRUE ~ "Otro"  # Caso en que el valor no coincida con ninguno
  ))

#VARIABLE o32 SOBRE SI COTIZÓ EL MES PASADO
trabajo <- trabajo %>%
  mutate(o32_recodificado = case_when(
    o32 == 1 ~ "Si, AFP (Administradora de Fondos de Pensiones)",
    o32 == 2 ~ "Si, IPS ex INP [Caja Nacional de Empleados Públicos (CANAEMPU), Caja de Empleados Particulares (EMPART), Servicio de Salud]",
    o32 == 3 ~ "Si, Caja de Previsión de la Defensa Nacional (CAPREDENA)",
    o32 == 4 ~ "Si, Dirección de Previsión de Carabineros (DIPRECA)",
    o32 == 5 ~ "Si, otra. Especifique",
    o32 == 6 ~ "No está cotizando",
    o32 == 9 ~ "No sabe",
    TRUE ~ "Otro"  # Caso en que el valor no coincida con ninguno
  ))

#VARIABLE o34 SOBRE SI EMITIÓ BOLETA DE HONORARIOS
trabajo <- trabajo %>%
  mutate(o34_recodificado = case_when(
    o34 == 1 ~ "Sí",
    o34 == 2 ~ "No",
    o34 == 9 ~ "No sabe/No responde",
    TRUE ~ "Otro"  # Caso en que el valor no coincida con ninguno
  ))


#ENTIDAD BIENESTAR SOCIAL
#VARIABLE SOBRE SI PUDO CONTAR CON CONOCIDOS PARA MEJORAR EMPLEABILIDAD
bienestar_social <- bienestar_social %>%
  mutate(j3a_1_recodificado = case_when(
    j3a_1 == 1 ~ "Nada",
    j3a_1 == 2 ~ "Poco",
    j3a_1 == 3 ~ "Algo",
    j3a_1 == 4 ~ "Bastante",
    j3a_1 == 5 ~ "Mucho",
    TRUE ~ "Otro"  # Caso en que el valor no coincida con ninguno
  ))

#VARIABLE SOBRE SI PUDO COMPATIBILIZAR VIDA PERSONAL Y TRABAJO
bienestar_social <- bienestar_social %>%
  mutate(j3a_4_recodificado = case_when(
    j3a_4 == 1 ~ "Nada",
    j3a_4 == 2 ~ "Poco",
    j3a_4 == 3 ~ "Algo",
    j3a_4 == 4 ~ "Bastante",
    j3a_4 == 5 ~ "Mucho",
    TRUE ~ "Otro"  # Caso en que el valor no coincida con ninguno
  ))


#VARIABLE SOBRE SI ESTÁ SATISFECHO CON TIEMPO ENTRE VIDA PERSONAL Y TRABAJO
bienestar_social <- bienestar_social %>%
  mutate(a3_4_recodificado = case_when(
    a3_4 == 1 ~ "Totalmente insatisfecho",
    a3_4 == 2 ~ "Insatisfecho",
    a3_4 == 3 ~ "Indiferente",
    a3_4 == 4 ~ "Satisfecho",
    a3_4 == 5 ~ "Totalmente satisfecho",
    TRUE ~ "Otro"  # Caso en que el valor no coincida con ninguno
  ))

#VARIABLE SOBRE SI EN TRABAJO ACTUAL PUDO LOGRAR PROYECTOS CUMPLIR METAS
bienestar_social <- bienestar_social %>%
  mutate(j3a_5_recodificado = case_when(
    j3a_5 == 1 ~ "Nada",
    j3a_5 == 2 ~ "Poco",
    j3a_5 == 3 ~ "Algo",
    j3a_5 == 4 ~ "Bastante",
    j3a_5 == 5 ~ "Mucho",
    TRUE ~ "Otro"  # Caso en que el valor no coincida con ninguno
  ))


#VARIABLE TIEMPO TRASLADO AL TRABAJO
table(bienestar_social$c1_4)
#SE BORRA QUIENES RESPONDEN CERO EN TIEMPO, PUES PUEDE SER POR ESTAR CESANTES, JUBILADOS, ESTUDIANTES, ETC.
bienestar_social <- bienestar_social %>%
  filter(!(c1_4 %in% c("00:00", "88:88", "99:99")))
#LA CAMBIO DE NOMBRE
bienestar_social <- bienestar_social %>%
  rename("Tiempo traslado trabajo (en horas)" = c1_4)


#VARIABLE Horas al día para Ocio, vida social y pasatiempos
table(bienestar_social$c1_5)
#BORRO RESPUESTA NO SABE
bienestar_social <- bienestar_social %>%
  filter(!(c1_5 %in% c("88:88")))
#LA CAMBIO DE NOMBRE
bienestar_social <- bienestar_social %>%
  rename("Tiempo al dia para ocio (en horas))" = c1_5)


#VARIABLE SOBRE SI PIENSA EN TAREAS DOMESTICAS MIENTRA TRABAJA
bienestar_social <- bienestar_social %>%
  mutate(c6_recodificado = case_when(
    c6 == 1 ~ "Nada",
    c6 == 2 ~ "Poco",
    c6 == 3 ~ "Algo",
    c6 == 4 ~ "Bastante",
    c6 == 5 ~ "Mucho",
    TRUE ~ "Otro"  # Caso en que el valor no coincida con ninguno
  ))


#VARIABLE SOBRE SI PUEDE AUSENTARSE AL TRABAJO PARA ATENDER ASUNTOS PERSONALES
bienestar_social <- bienestar_social %>%
  mutate(c3_2_recodificado = case_when(
    c3_2 == 1 ~ "Sí",
    c3_2 == 2 ~ "No",
    TRUE ~ "Otro"  # Caso en que el valor no coincida con ninguno
  ))




#MODELO ENTIDAD RELACION
library(DiagrammeR)

diagram <- grViz(
  "digraph ER {
    
    graph [rankdir = LR]

    # Nodos para entidades
    node [shape = rectangle, style = filled, fillcolor = lightblue]
    hogares_mayor_ingreso [label = 'Hogares de Mayor Ingreso\n(id_persona_casen, folio_casen, elite)']
    identificacion_personal [label = 'Identificación Personal\n(id_persona_casen, folio_casen, elite,\ne6a, sexo, región, zona, v13)']
    trabajo [label = 'Trabajo\n(id_persona_casen, folio_casen, elite,\no9a, o15, o32, o34)']
    bienestar_social [label = 'Bienestar Social\n(id_persona_casen, folio_casen, elite,\nj3a_1, j3a_4, a3_4, j3a_5,\nc1_4, c1_5, c3_2, c6)']

    # Relaciones
    hogares_mayor_ingreso -> identificacion_personal [label = 'se despliega en']
    identificacion_personal -> trabajo [label = 'se representa en']
    identificacion_personal -> bienestar_social [label = 'se representa en']
  }"
)

print(diagram)


