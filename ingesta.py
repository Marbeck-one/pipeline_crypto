"""
ingesta.py — Ingesta incremental de precios de criptomonedas desde CoinGecko API
Actividad 2.1 – Pipeline de Datos: Ingesta de datos automatizada con clave compuesta

Uso:
    python ingesta.py          → Modo real (requiere acceso a internet)
    python ingesta.py --demo   → Modo demo con datos simulados
"""

import os
import csv
import json
import logging
import sys
from datetime import datetime, timezone

# ─────────────────────────────────────────────
# Configuración de logging
# ─────────────────────────────────────────────
os.makedirs("data/logs", exist_ok=True)
os.makedirs("data/raw", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("data/logs/ingesta.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Configuración general
# ─────────────────────────────────────────────
API_URL = "https://api.coingecko.com/api/v3/coins/markets"
PARAMS = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 10,
    "page": 1,
    "sparkline": False
}

DESTINO_CSV     = "data/raw/cripto_precios.csv"
CHECKPOINT_FILE = "data/raw/.checkpoint.json"

COLUMNAS = ["id", "symbol", "name", "current_price", "market_cap",
            "total_volume", "price_change_percentage_24h", "last_updated"]

DATOS_DEMO = [
    {"id": "bitcoin",     "symbol": "btc",  "name": "Bitcoin",   "current_price": 84500,  "market_cap": 1670000000000, "total_volume": 38000000000, "price_change_percentage_24h": 1.25,  "last_updated": "2026-04-20T04:00:00Z"},
    {"id": "ethereum",    "symbol": "eth",  "name": "Ethereum",  "current_price": 1590,   "market_cap": 191000000000,  "total_volume": 18000000000, "price_change_percentage_24h": -0.87, "last_updated": "2026-04-20T04:00:00Z"},
    {"id": "tether",      "symbol": "usdt", "name": "Tether",    "current_price": 1.001,  "market_cap": 144000000000,  "total_volume": 62000000000, "price_change_percentage_24h": 0.01,  "last_updated": "2026-04-20T04:00:00Z"},
    {"id": "binancecoin", "symbol": "bnb",  "name": "BNB",       "current_price": 590,    "market_cap": 85000000000,   "total_volume": 1500000000,  "price_change_percentage_24h": 0.54,  "last_updated": "2026-04-20T04:00:00Z"},
    {"id": "solana",      "symbol": "sol",  "name": "Solana",    "current_price": 138,    "market_cap": 71000000000,   "total_volume": 3200000000,  "price_change_percentage_24h": 2.10,  "last_updated": "2026-04-20T04:00:00Z"},
    {"id": "ripple",      "symbol": "xrp",  "name": "XRP",       "current_price": 2.05,   "market_cap": 118000000000,  "total_volume": 4700000000,  "price_change_percentage_24h": -1.33, "last_updated": "2026-04-20T04:00:00Z"},
    {"id": "usd-coin",    "symbol": "usdc", "name": "USD Coin",  "current_price": 0.9998, "market_cap": 61000000000,   "total_volume": 6100000000,  "price_change_percentage_24h": 0.00,  "last_updated": "2026-04-20T04:00:00Z"},
    {"id": "dogecoin",    "symbol": "doge", "name": "Dogecoin",  "current_price": 0.165,  "market_cap": 24000000000,   "total_volume": 1100000000,  "price_change_percentage_24h": 3.45,  "last_updated": "2026-04-20T04:00:00Z"},
    {"id": "cardano",     "symbol": "ada",  "name": "Cardano",   "current_price": 0.62,   "market_cap": 21700000000,   "total_volume": 490000000,   "price_change_percentage_24h": 1.80,  "last_updated": "2026-04-20T04:00:00Z"},
    {"id": "avalanche-2", "symbol": "avax", "name": "Avalanche", "current_price": 22.5,   "market_cap": 9200000000,    "total_volume": 350000000,   "price_change_percentage_24h": -0.62, "last_updated": "2026-04-20T04:00:00Z"},
]


def cargar_checkpoint() -> set:
    """Devuelve el conjunto de claves (id_fecha) ya procesadas."""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            data = json.load(f)
            # Retorna las claves procesadas (id_timestamp)
            return set(data.get("ids_procesados", []))
    return set()


def guardar_checkpoint(claves: set) -> None:
    """Persiste las claves procesadas (id_fecha) para la próxima ejecución."""
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({
            "ids_procesados": list(claves),
            "ultima_ejecucion": datetime.now(timezone.utc).isoformat()
        }, f, indent=2)


def obtener_datos_api() -> list:
    """Consulta la API de CoinGecko y retorna la lista de monedas."""
    import requests
    headers = {"User-Agent": "pipeline-datos-duocuc/1.0"}
    logger.info("Consultando API de CoinGecko...")
    try:
        respuesta = requests.get(API_URL, params=PARAMS, headers=headers, timeout=10)
        respuesta.raise_for_status()
        datos = respuesta.json()
        logger.info(f"API respondio con {len(datos)} registros.")
        return datos
    except Exception as e:
        logger.error(f"Error al conectar con la API: {e}")
        return []


def obtener_datos_demo() -> list:
    """Retorna datos simulados para pruebas sin conexion."""
    logger.info("Modo DEMO activado — usando datos simulados.")
    return DATOS_DEMO


def filtrar_nuevos(datos: list, claves_previas: set) -> list:
    """Filtra registros usando la clave compuesta (id + last_updated)."""
    nuevos = []
    for m in datos:
        # Creamos una clave única por moneda y timestamp
        clave = f"{m['id']}_{m['last_updated']}"
        if clave not in claves_previas:
            nuevos.append(m)
    return nuevos


def guardar_csv(registros: list) -> int:
    """Agrega los registros nuevos al CSV destino."""
    archivo_existe = os.path.exists(DESTINO_CSV)
    with open(DESTINO_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNAS, extrasaction="ignore")
        if not archivo_existe:
            writer.writeheader()
        for r in registros:
            writer.writerow(r)
    return len(registros)


def main():
    modo_demo = "--demo" in sys.argv

    logger.info("=" * 50)
    logger.info("INICIO DEL PROCESO DE INGESTA")
    logger.info(f"Modo: {'DEMO' if modo_demo else 'REAL'}")
    logger.info(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")

    try:
        # 1. Cargar checkpoint (claves compuestas ID_FECHA)
        claves_previas = cargar_checkpoint()
        logger.info(f"Checkpoint cargado: {len(claves_previas)} registros previos en historial.")

        # 2. Obtener datos
        datos = obtener_datos_demo() if modo_demo else obtener_datos_api()
        if not datos:
            logger.warning("No se recibieron datos para procesar.")
            return

        # 3. Ingesta incremental — filtrar por clave compuesta (ID + Timestamp)
        nuevos = filtrar_nuevos(datos, claves_previas)
        logger.info(f"Registros con nuevos timestamps: {len(nuevos)} de {len(datos)}.")

        if not nuevos:
            logger.info("Sin actualizaciones de precio detectadas. Ingesta sin cambios.")
        else:
            # 4. Guardar en CSV
            guardados = guardar_csv(nuevos)
            logger.info(f"Registros guardados en '{DESTINO_CSV}': {guardados}")

            # 5. Actualizar checkpoint con las nuevas claves ID_FECHA
            claves_nuevas = {f"{r['id']}_{r['last_updated']}" for r in nuevos}
            claves_actualizadas = claves_previas | claves_nuevas
            guardar_checkpoint(claves_actualizadas)
            logger.info("Checkpoint actualizado con nuevos registros.")

        logger.info("INGESTA FINALIZADA CORRECTAMENTE")

    except Exception as e:
        logger.error(f"Error durante la ingesta: {e}")
        raise
    finally:
        logger.info("=" * 50)


if __name__ == "__main__":
    main()
