"""
ingesta.py — Ingesta incremental de precios de criptomonedas desde CoinGecko API
Actividad 2.1 – Pipeline de Datos: Ingesta de datos automatizada

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
    """Devuelve el conjunto de IDs ya procesados en ejecuciones anteriores."""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            data = json.load(f)
            return set(data.get("ids_procesados", []))
    return set()


def guardar_checkpoint(ids: set) -> None:
    """Persiste los IDs procesados para la próxima ejecución incremental."""
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({
            "ids_procesados": list(ids),
            "ultima_ejecucion": datetime.now(timezone.utc).isoformat()
        }, f, indent=2)


def obtener_datos_api() -> list:
    """Consulta la API de CoinGecko y retorna la lista de monedas."""
    import requests
    headers = {"User-Agent": "pipeline-datos-duocuc/1.0"}
    logger.info("Consultando API de CoinGecko...")
    respuesta = requests.get(API_URL, params=PARAMS, headers=headers, timeout=10)
    respuesta.raise_for_status()
    datos = respuesta.json()
    logger.info(f"API respondio con {len(datos)} registros.")
    return datos


def obtener_datos_demo() -> list:
    """Retorna datos simulados para pruebas sin conexion."""
    logger.info("Modo DEMO activado — usando datos simulados.")
    return DATOS_DEMO


def filtrar_nuevos(datos: list, ids_previos: set) -> list:
    """Filtra unicamente los registros que no han sido ingestados antes."""
    return [m for m in datos if m["id"] not in ids_previos]


def guardar_csv(registros: list) -> int:
    """Agrega los registros nuevos al CSV destino. Retorna la cantidad guardada."""
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
        # 1. Cargar checkpoint de ejecuciones previas
        ids_previos = cargar_checkpoint()
        logger.info(f"Checkpoint cargado: {len(ids_previos)} IDs previos registrados.")

        # 2. Obtener datos
        datos = obtener_datos_demo() if modo_demo else obtener_datos_api()
        logger.info(f"Total registros recibidos: {len(datos)}")

        # 3. Ingesta incremental — filtrar solo los nuevos
        nuevos = filtrar_nuevos(datos, ids_previos)
        logger.info(f"Registros nuevos a insertar: {len(nuevos)} de {len(datos)}.")

        if not nuevos:
            logger.info("Sin datos nuevos. Ingesta incremental sin cambios.")
        else:
            # 4. Guardar en CSV
            guardados = guardar_csv(nuevos)
            logger.info(f"Registros guardados en '{DESTINO_CSV}': {guardados}")

            # 5. Actualizar checkpoint
            ids_actualizados = ids_previos | {r["id"] for r in nuevos}
            guardar_checkpoint(ids_actualizados)
            logger.info("Checkpoint actualizado correctamente.")

        logger.info("INGESTA FINALIZADA CORRECTAMENTE")

    except Exception as e:
        logger.error(f"Error durante la ingesta: {e}")
        raise
    finally:
        logger.info("=" * 50)


if __name__ == "__main__":
    main()
