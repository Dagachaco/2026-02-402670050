# La generación de datos va a realizarse con un script generado por ChatGPT


import csv
import os
import random
from datetime import date, timedelta

# ============================================================
# Configuración
# ============================================================

random.seed(42)

DATA_DIR = "data"

NUM_CICLISTAS = 500
NUM_RUTAS = 100
NUM_ACTIVIDADES = 500

os.makedirs(DATA_DIR, exist_ok=True)

# ============================================================
# Datos base
# ============================================================

# Distribución desigual.
# Limón debe tener exactamente 3 ciclistas.
PROVINCIAS_PESOS = [
    ("San José", 0.30),
    ("Alajuela", 0.22),
    ("Cartago", 0.15),
    ("Heredia", 0.14),
    ("Guanacaste", 0.08),
    ("Puntarenas", 0.105),
    ("Limón", 0.006)
]

NOMBRES = [
    "Alejandro", "Andrés", "Antonio", "Carlos", "Daniel",
    "David", "Diego", "Eduardo", "Esteban", "Felipe",
    "Fernando", "Francisco", "Gabriel", "Gustavo", "Héctor",
    "Isaac", "Javier", "Jorge", "José", "Juan",
    "Luis", "Manuel", "Marco", "Mario", "Martín",
    "Miguel", "Nicolás", "Óscar", "Pablo", "Pedro",
    "Rafael", "Ricardo", "Roberto", "Rodrigo", "Santiago",
    "Sebastián", "Sergio", "Tomás", "Víctor", "Adriana",
    "Alejandra", "Andrea", "Ángela", "Beatriz", "Camila",
    "Carolina", "Daniela", "Diana", "Elena", "Gabriela",
    "Isabel", "Jimena", "Laura", "Lucía", "María",
    "Mariana", "Natalia", "Nicole", "Patricia", "Paola",
    "Sofía", "Valentina", "Verónica"
]

APELLIDOS = [
    "Alvarado", "Araya", "Álvarez", "Brenes", "Calderón",
    "Campos", "Carmona", "Castillo", "Chacón", "Chaves",
    "Cordero", "Cruz", "Díaz", "Esquivel", "Fernández",
    "García", "Gómez", "González", "Hernández", "Jiménez",
    "León", "López", "Madrigal", "Méndez", "Molina",
    "Montero", "Mora", "Morales", "Navarro", "Núñez",
    "Pérez", "Quesada", "Ramírez", "Rojas", "Romero",
    "Rosales", "Salas", "Sánchez", "Solano", "Soto",
    "Ugalde", "Valverde", "Vargas", "Vega", "Vílchez",
    "Villalobos", "Zamora"
]

NOMBRES_RUTA = [
    "Sendero del Volcán",
    "Ruta del Valle Central",
    "Circuito de los Cafetales",
    "Camino de las Montañas",
    "Ruta del Pacífico",
    "Sendero de los Santos",
    "Circuito del Arenal",
    "Ruta de la Sabana",
    "Camino de las Flores",
    "Ruta del Caribe",
    "Sendero de la Cordillera",
    "Circuito de Orosi",
    "Ruta de Turrialba",
    "Camino de Guanacaste",
    "Sendero de Puntarenas",
    "Ruta de los Manglares",
    "Circuito de Escazú",
    "Camino de las Brumas",
    "Ruta de Monteverde",
    "Sendero del Atlántico",
    "Circuito de Sarapiquí",
    "Ruta de los Volcanes",
    "Camino del Pacífico",
    "Sendero de los Robles",
    "Ruta de los Pinos",
    "Circuito de Cartago",
    "Camino de Alajuela",
    "Ruta de Heredia",
    "Sendero de Limón",
    "Circuito de las Playas"
]


# ============================================================
# Funciones auxiliares
# ============================================================

def fecha_aleatoria():
    """Genera una fecha entre 2026-01-01 y 2026-06-30."""
    inicio = date(2026, 1, 1)
    fin = date(2026, 6, 30)

    dias = (fin - inicio).days
    fecha = inicio + timedelta(days=random.randint(0, dias))

    return fecha.isoformat()


def generar_nombre():
    """Genera un nombre con dos apellidos."""
    nombre = random.choice(NOMBRES)
    apellido1 = random.choice(APELLIDOS)
    apellido2 = random.choice(APELLIDOS)

    # Evita que los dos apellidos sean iguales.
    while apellido2 == apellido1:
        apellido2 = random.choice(APELLIDOS)

    return f"{nombre} {apellido1} {apellido2}"


def elegir_provincia():
    """Selecciona una provincia según la distribución definida."""
    numero = random.random()

    acumulado = 0

    for provincia, peso in PROVINCIAS_PESOS:
        acumulado += peso

        if numero <= acumulado:
            return provincia

    return "San José"


# ============================================================
# 1. Generar ciclista.csv
# ============================================================

ciclistas = []
cedulas_usadas = set()

# Primero creamos exactamente 3 ciclistas de Limón.
for _ in range(3):
    while True:
        cedula = random.randint(100000000, 799999999)

        if cedula not in cedulas_usadas:
            cedulas_usadas.add(cedula)
            break

    ciclistas.append({
        "cedula": cedula,
        "nombre_completo": generar_nombre(),
        "provincia": "Limón"
    })


# Crear los otros 497 ciclistas.
while len(ciclistas) < NUM_CICLISTAS:
    while True:
        cedula = random.randint(100000000, 799999999)

        if cedula not in cedulas_usadas:
            cedulas_usadas.add(cedula)
            break

    provincia = elegir_provincia()

    # Evitar agregar más ciclistas de Limón.
    if provincia == "Limón":
        provincia = random.choice([
            "San José",
            "Alajuela",
            "Cartago",
            "Heredia",
            "Guanacaste",
            "Puntarenas"
        ])

    ciclistas.append({
        "cedula": cedula,
        "nombre_completo": generar_nombre(),
        "provincia": provincia
    })


# ============================================================
# 2. Generar ruta.csv
# ============================================================

rutas = []

for codigo in range(1, NUM_RUTAS + 1):
    nombre_base = random.choice(NOMBRES_RUTA)

    # Agregamos el número para garantizar nombres únicos.
    nombre_ruta = f"{nombre_base} {codigo}"

    kilometros = round(random.uniform(5.0, 120.0), 1)

    rutas.append({
        "codigo_ruta": codigo,
        "nombre_ruta": nombre_ruta,
        "kilometros": f"{kilometros:.1f}"
    })


# ============================================================
# 3. Preparar actividades
# ============================================================

# Lista de cédulas.
cedulas = [c["cedula"] for c in ciclistas]

# Diccionario para obtener información de cada ruta.
ruta_por_codigo = {
    r["codigo_ruta"]: r
    for r in rutas
}

actividades = []

# Para controlar cuántas actividades tiene cada ciclista.
actividades_por_ciclista = {
    cedula: []
    for cedula in cedulas
}


# ------------------------------------------------------------
# Caso especial 1:
# Al menos 40 ciclistas sin ninguna actividad.
#
# Reservamos 50 ciclistas sin actividades para tener margen.
# ------------------------------------------------------------

ciclistas_sin_actividad = set(random.sample(cedulas, 50))

ciclistas_con_actividad = [
    cedula
    for cedula in cedulas
    if cedula not in ciclistas_sin_actividad
]


# ------------------------------------------------------------
# Caso especial 2:
# Un ciclista con actividades en un solo día.
# ------------------------------------------------------------

ciclista_un_dia = ciclistas_con_actividad[0]
fecha_unica = "2026-03-15"


# ------------------------------------------------------------
# Caso especial 3:
# Otro ciclista con actividades en muchos días distintos.
# ------------------------------------------------------------

ciclista_muchos_dias = ciclistas_con_actividad[1]

fechas_muchos_dias = [
    "2026-01-10",
    "2026-01-25",
    "2026-02-14",
    "2026-03-05",
    "2026-03-28",
    "2026-04-12",
    "2026-05-03",
    "2026-05-22",
    "2026-06-15"
]


# ------------------------------------------------------------
# Caso especial 4:
# Al menos 15 repeticiones exactas:
# mismo ciclista + misma ruta + mismo día.
# ------------------------------------------------------------

repeticiones = []
ciclistas_repetidos = []

for i in range(15):
    cedula = ciclistas_con_actividad[2 + i]
    ciclistas_repetidos.append(cedula)

    # La misma ruta para la primera y segunda aparición.
    codigo_ruta = (i % NUM_RUTAS) + 1

    fecha = fecha_aleatoria()

    fila = (codigo_ruta, cedula, fecha)

    # Agregamos exactamente la misma fila dos veces.
    repeticiones.append(fila)
    repeticiones.append(fila)


# Agregamos las repeticiones.
for codigo_ruta, cedula, fecha in repeticiones:
    actividades.append({
        "codigo_ruta": codigo_ruta,
        "cedula": cedula,
        "fecha": fecha
    })

    actividades_por_ciclista[cedula].append(
        (codigo_ruta, fecha)
    )


# ------------------------------------------------------------
# Actividades del ciclista de un solo día.
# ------------------------------------------------------------

for codigo_ruta in [10, 20, 30]:
    actividades.append({
        "codigo_ruta": codigo_ruta,
        "cedula": ciclista_un_dia,
        "fecha": fecha_unica
    })

    actividades_por_ciclista[ciclista_un_dia].append(
        (codigo_ruta, fecha_unica)
    )


# ------------------------------------------------------------
# Actividades del ciclista de muchos días.
# ------------------------------------------------------------

for i, fecha in enumerate(fechas_muchos_dias):
    codigo_ruta = 40 + i

    actividades.append({
        "codigo_ruta": codigo_ruta,
        "cedula": ciclista_muchos_dias,
        "fecha": fecha
    })

    actividades_por_ciclista[ciclista_muchos_dias].append(
        (codigo_ruta, fecha)
    )


# ------------------------------------------------------------
# Caso especial 5:
# Dos ciclistas de la misma provincia con exactamente
# el mismo total de kilómetros.
#
# Usamos San José y les asignamos la misma ruta.
# Se eligen entre ciclistas que NO sean de otros casos especiales,
# para que el relleno aleatorio no altere su total.
# ------------------------------------------------------------

# Cédulas que quedan fuera del relleno aleatorio (casos especiales).
cedulas_especiales = {ciclista_un_dia, ciclista_muchos_dias}
cedulas_especiales |= set(ciclistas_repetidos)

con_actividad_set = set(ciclistas_con_actividad)

ciclistas_sanjose = [
    c["cedula"]
    for c in ciclistas
    if c["provincia"] == "San José"
    and c["cedula"] in con_actividad_set
    and c["cedula"] not in cedulas_especiales
]

ciclista_empate_1 = ciclistas_sanjose[0]
ciclista_empate_2 = ciclistas_sanjose[1]

cedulas_especiales |= {ciclista_empate_1, ciclista_empate_2}

ruta_empate = 75

# Ambos hacen exactamente la misma ruta una vez.
for cedula in [ciclista_empate_1, ciclista_empate_2]:
    fecha = "2026-04-20"

    actividades.append({
        "codigo_ruta": ruta_empate,
        "cedula": cedula,
        "fecha": fecha
    })

    actividades_por_ciclista[cedula].append(
        (ruta_empate, fecha)
    )


# ------------------------------------------------------------
# Completar las actividades restantes hasta llegar a 500,
# usando solo ciclistas que no son casos especiales.
# ------------------------------------------------------------

pool_aleatorio = [
    cedula
    for cedula in ciclistas_con_actividad
    if cedula not in cedulas_especiales
]

while len(actividades) < NUM_ACTIVIDADES:
    cedula = random.choice(pool_aleatorio)
    codigo_ruta = random.randint(1, NUM_RUTAS)
    fecha = fecha_aleatoria()

    actividades.append({
        "codigo_ruta": codigo_ruta,
        "cedula": cedula,
        "fecha": fecha
    })

    actividades_por_ciclista[cedula].append(
        (codigo_ruta, fecha)
    )


# Mezclar para que los casos especiales no queden agrupados.
random.shuffle(actividades)


# ============================================================
# Validaciones
# ============================================================

# Validar cantidad de filas.
assert len(ciclistas) == 500
assert len(rutas) == 100
assert len(actividades) == 500

# Validar cédulas únicas.
assert len(set(c["cedula"] for c in ciclistas)) == 500

# Validar exactamente 3 ciclistas de Limón.
assert sum(
    1 for c in ciclistas
    if c["provincia"] == "Limón"
) == 3

# Validar que todas las rutas de actividad existan.
assert all(
    a["codigo_ruta"] in ruta_por_codigo
    for a in actividades
)

# Validar que todas las cédulas de actividad existan.
assert all(
    a["cedula"] in cedulas_usadas
    for a in actividades
)

# Validar fechas.
for actividad in actividades:
    fecha = date.fromisoformat(actividad["fecha"])

    assert date(2026, 1, 1) <= fecha <= date(2026, 6, 30)


# Validar al menos 40 ciclistas sin actividades.
sin_actividad = [
    cedula
    for cedula, lista in actividades_por_ciclista.items()
    if len(lista) == 0
]

assert len(sin_actividad) >= 40


# Validar que existen al menos 15 filas repetidas.
conteo_filas = {}

for actividad in actividades:
    clave = (
        actividad["codigo_ruta"],
        actividad["cedula"],
        actividad["fecha"]
    )

    conteo_filas[clave] = conteo_filas.get(clave, 0) + 1

filas_repetidas = sum(
    1
    for cantidad in conteo_filas.values()
    if cantidad > 1
)

assert filas_repetidas >= 15


# Validar ciclista con un solo día.
dias_un_ciclista = {
    a["fecha"]
    for a in actividades
    if a["cedula"] == ciclista_un_dia
}

assert len(dias_un_ciclista) == 1


# Validar ciclista con muchos días.
dias_muchos = {
    a["fecha"]
    for a in actividades
    if a["cedula"] == ciclista_muchos_dias
}

assert len(dias_muchos) >= 5


# Validar el empate: mismos kilómetros totales, misma provincia.
km_por_ruta = {r["codigo_ruta"]: float(r["kilometros"]) for r in rutas}


def total_km(cedula):
    return round(
        sum(
            km_por_ruta[a["codigo_ruta"]]
            for a in actividades
            if a["cedula"] == cedula
        ),
        1
    )


assert total_km(ciclista_empate_1) == total_km(ciclista_empate_2)


# ============================================================
# Escribir archivos CSV
# ============================================================

def escribir_csv(nombre_archivo, filas, columnas):
    ruta_archivo = os.path.join(DATA_DIR, nombre_archivo)

    with open(
        ruta_archivo,
        "w",
        encoding="utf-8",
        newline=""
    ) as archivo:

        escritor = csv.writer(
            archivo,
            delimiter=",",
            lineterminator="\n"
        )

        # IMPORTANTE:
        # No escribimos encabezado.
        for fila in filas:
            escritor.writerow(
                [fila[columna] for columna in columnas]
            )


escribir_csv(
    "ciclista.csv",
    ciclistas,
    ["cedula", "nombre_completo", "provincia"]
)

escribir_csv(
    "ruta.csv",
    rutas,
    ["codigo_ruta", "nombre_ruta", "kilometros"]
)

escribir_csv(
    "actividad.csv",
    actividades,
    ["codigo_ruta", "cedula", "fecha"]
)


# ============================================================
# Resumen final
# ============================================================

print("=" * 50)
print("ARCHIVOS GENERADOS")
print("=" * 50)

print(f"ciclista.csv   : {len(ciclistas)} filas")
print(f"ruta.csv       : {len(rutas)} filas")
print(f"actividad.csv  : {len(actividades)} filas")
print(f"Ciclistas sin actividades: {len(sin_actividad)}")

print()
print("Casos especiales:")
print("- Ciclistas de Limón: 3")
print(f"- Ciclistas sin actividades: {len(sin_actividad)}")
print(f"- Casos de filas repetidas: {filas_repetidas}")
print(f"- Ciclista con un solo día: {ciclista_un_dia}")
print(f"- Ciclista con muchos días: {ciclista_muchos_dias}")
print(f"- Empate de kilómetros: {ciclista_empate_1} y {ciclista_empate_2}")
print()
print(f"Archivos guardados en: {DATA_DIR}/")