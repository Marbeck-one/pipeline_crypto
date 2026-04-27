"""
limpieza.py — Limpieza y transformación del dataset de criptomonedas
Actividad 2.2 – Pipeline de Datos: Limpieza y transformación del dataset

Lee el CSV crudo desde data/raw/, aplica un flujo estructurado de
limpieza y transformación, y guarda el dataset procesado en data/processed/.

Transformaciones aplicadas:
    1. Eliminación de registros nulos o con campos críticos vacíos.
    2. Eliminación de duplicados por clave (id, last_updated).
    3. Conversión y estandarización de tipos numéricos (float).
    4. Estandarización del campo 'last_updated' al formato ISO 8601 sin milisegundos.
    5. Estandarización de 'symbol' y 'name' (strip + capitalización consistente).
    6. Filtrado de registros con precio <= 0 o market_cap <= 0 (fuera de rango).
    7. Columna derivada 'market_cap_category': clasifica la moneda por capitalización.
    8. Columna derivada 'price_change_label': etiqueta la variación 24h como
       'positiva', 'negativa' o 'estable'.

Uso:
    python limpieza.py
"""

import os
import csv
import logging
from datetime import datetime, timezone, timedelta

# ─────────────────────────────────────────────
# Configuración de logging
# ─────────────────────────────────────────────
os.makedirs("data/logs", exist_ok=True)
os.makedirs("data/processed", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("data/logs/limpieza.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Configuración
# ─────────────────────────────────────────────
CSV_ORIGEN    = "data/raw/cripto_precios.csv"
CSV_DESTINO   = "data/processed/cripto_precios_limpio.csv"

COLUMNAS_REQUERIDAS = ["id", "symbol", "name", "current_price", "market_cap",
                       "total_volume", "price_change_percentage_24h", "last_updated"]

COLUMNAS_SALIDA = COLUMNAS_REQUERIDAS + ["market_cap_category", "price_change_label"]

# Umbral de antigüedad máxima aceptada (días). Registros más antiguos se descartan.
MAX_ANTIGUEDAD_DIAS = 7


# ─────────────────────────────────────────────
# Funciones de transformación
# ─────────────────────────────────────────────

def estandarizar_fecha(valor: str) -> str:
    """
    Convierte 'last_updated' al formato ISO 8601 sin milisegundos ni zona horaria.
    Ejemplo: '2026-04-20T04:19:57.441Z' → '2026-04-20T04:19:57'
    """
    valor = valor.strip().rstrip("Z")
    if "." in valor:
        valor = valor[:valor.index(".")]
    return valor


def clasificar_market_cap(market_cap: float) -> str:
    """
    Devuelve una categoría descriptiva según la capitalización de mercado (USD).
        Large Cap  : >= 10,000 millones
        Mid Cap    : entre 1,000 y 10,000 millones
        Small Cap  : < 1,000 millones
    """
    if market_cap >= 10_000_000_000:
        return "Large Cap"
    elif market_cap >= 1_000_000_000:
        return "Mid Cap"
    else:
        return "Small Cap"


def etiquetar_variacion(cambio_24h: float) -> str:
    """
    Clasifica la variación de precio en 24h:
        positiva  : cambio > 0.1 %
        negativa  : cambio < -0.1 %
        estable   : entre -0.1 % y 0.1 %
    """
    if cambio_24h > 0.1:
        return "positiva"
    elif cambio_24h < -0.1:
        return "negativa"
    else:
        return "estable"


def fecha_muy_antigua(fecha_iso: str) -> bool:
    """Devuelve True si el registro supera el umbral de antigüedad máxima."""
    try:
        dt = datetime.fromisoformat(fecha_iso)
        ahora = datetime.now(timezone.utc).replace(tzinfo=None)
        return (ahora - dt.replace(tzinfo=None)) > timedelta(days=MAX_ANTIGUEDAD_DIAS)
    except Exception:
        return False  # Si no se puede parsear, se deja pasar (ya se validó antes)


# ─────────────────────────────────────────────
# Pipeline de limpieza
# ─────────────────────────────────────────────

def leer_csv(ruta: str) -> list[dict]:
    """Lee el CSV crudo y devuelve una lista de diccionarios."""
    if not os.path.exists(ruta):
        raise FileNotFoundError(f"No se encontró el archivo CSV: {ruta}")
    with open(ruta, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def eliminar_nulos(registros: list[dict]) -> tuple[list[dict], int]:
    """Elimina filas con campos críticos nulos o vacíos."""
    limpios = []
    eliminados = 0
    for r in registros:
        if all(r.get(col, "").strip() for col in COLUMNAS_REQUERIDAS):
            limpios.append(r)
        else:
            logger.warning(f"Registro con campo nulo eliminado: id={r.get('id', '?')}")
            eliminados += 1
    return limpios, eliminados


def eliminar_duplicados(registros: list[dict]) -> tuple[list[dict], int]:
    """Elimina duplicados según la clave compuesta (id, last_updated)."""
    vistos = set()
    limpios = []
    eliminados = 0
    for r in registros:
        clave = (r["id"], r["last_updated"])
        if clave not in vistos:
            vistos.add(clave)
            limpios.append(r)
        else:
            logger.warning(f"Duplicado eliminado: id={r['id']}, last_updated={r['last_updated']}")
            eliminados += 1
    return limpios, eliminados


def convertir_tipos(registros: list[dict]) -> tuple[list[dict], int]:
    """
    Convierte campos numéricos a float.
    Descarta las filas que no puedan convertirse.
    """
    validos = []
    eliminados = 0
    campos_numericos = ["current_price", "market_cap", "total_volume",
                        "price_change_percentage_24h"]
    for r in registros:
        try:
            for campo in campos_numericos:
                r[campo] = float(r[campo])
            validos.append(r)
        except (ValueError, KeyError) as e:
            logger.warning(f"Conversión numérica fallida ({r.get('id', '?')}): {e}")
            eliminados += 1
    return validos, eliminados


def filtrar_fuera_de_rango(registros: list[dict]) -> tuple[list[dict], int]:
    """Elimina registros con precio o market_cap <= 0."""
    validos = []
    eliminados = 0
    for r in registros:
        if r["current_price"] > 0 and r["market_cap"] > 0:
            validos.append(r)
        else:
            logger.warning(f"Registro fuera de rango eliminado: id={r['id']}, "
                           f"precio={r['current_price']}, market_cap={r['market_cap']}")
            eliminados += 1
    return validos, eliminados


def estandarizar_textos(registros: list[dict]) -> list[dict]:
    """Strip y estandarización de symbol (minúsculas) y name (título)."""
    for r in registros:
        r["symbol"] = r["symbol"].strip().lower()
        r["name"]   = r["name"].strip().title()
        r["id"]     = r["id"].strip().lower()
    return registros


def aplicar_transformaciones(registros: list[dict]) -> list[dict]:
    """
    Aplica todas las transformaciones derivadas:
      - Estandarización de last_updated
      - Columna market_cap_category
      - Columna price_change_label
    """
    for r in registros:
        r["last_updated"]        = estandarizar_fecha(r["last_updated"])
        r["market_cap_category"] = clasificar_market_cap(r["market_cap"])
        r["price_change_label"]  = etiquetar_variacion(r["price_change_percentage_24h"])
    return registros


def guardar_csv(registros: list[dict], ruta: str) -> None:
    """Escribe el dataset limpio en la carpeta de procesados."""
    with open(ruta, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNAS_SALIDA, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(registros)
    logger.info(f"Dataset limpio guardado en: {ruta}")


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    logger.info("=" * 50)
    logger.info("INICIO DE LIMPIEZA Y TRANSFORMACIÓN")
    logger.info(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")

    try:
        # 1. Lectura
        registros = leer_csv(CSV_ORIGEN)
        total_inicial = len(registros)
        logger.info(f"Registros leídos desde CSV crudo: {total_inicial}")

        # 2. Eliminación de nulos
        registros, n_nulos = eliminar_nulos(registros)
        logger.info(f"Registros eliminados por nulos      : {n_nulos}")

        # 3. Eliminación de duplicados
        registros, n_duplic = eliminar_duplicados(registros)
        logger.info(f"Registros eliminados por duplicados : {n_duplic}")

        # 4. Conversión de tipos
        registros, n_tipos = convertir_tipos(registros)
        logger.info(f"Registros eliminados por tipo       : {n_tipos}")

        # 5. Filtro de valores fuera de rango
        registros, n_rango = filtrar_fuera_de_rango(registros)
        logger.info(f"Registros eliminados por rango      : {n_rango}")

        # 6. Estandarización de textos
        registros = estandarizar_textos(registros)
        logger.info("Estandarización de textos aplicada.")

        # 7. Transformaciones y columnas derivadas
        registros = aplicar_transformaciones(registros)
        logger.info("Transformaciones y columnas derivadas aplicadas.")

        # 8. Guardar resultado
        guardar_csv(registros, CSV_DESTINO)

        # 9. Resumen final
        total_final = len(registros)
        total_eliminados = total_inicial - total_final
        logger.info(f"Registros finales en dataset limpio : {total_final}")
        logger.info(f"Total eliminados en limpieza        : {total_eliminados}")
        logger.info("LIMPIEZA FINALIZADA CORRECTAMENTE")

    except Exception as e:
        logger.error(f"Error durante la limpieza: {e}")
        raise
    finally:
        logger.info("=" * 50)


if __name__ == "__main__":
    main()