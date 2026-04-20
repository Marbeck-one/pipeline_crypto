"""
cargar_bd.py — Carga los datos del CSV a una base de datos SQLite
Actividad 2.1 – Pipeline de Datos: Carga a BD

Uso:
    python cargar_bd.py
"""

import os
import csv
import sqlite3
import logging
from datetime import datetime, timezone

# ─────────────────────────────────────────────
# Configuración de logging
# ─────────────────────────────────────────────
os.makedirs("data/logs", exist_ok=True)
os.makedirs("data/db", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("data/logs/carga_bd.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Configuración
# ─────────────────────────────────────────────
CSV_ORIGEN  = "data/raw/cripto_precios.csv"
DB_DESTINO  = "data/db/cripto.db"


def crear_tabla(conn: sqlite3.Connection) -> None:
    """Crea la tabla cripto_precios si no existe."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cripto_precios (
            id                          TEXT NOT NULL,
            symbol                      TEXT,
            name                        TEXT,
            current_price               REAL,
            market_cap                  REAL,
            total_volume                REAL,
            price_change_percentage_24h REAL,
            last_updated                TEXT,
            ingested_at                 TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (id, last_updated)
        )
    """)
    conn.commit()
    logger.info("Tabla 'cripto_precios' verificada/creada correctamente.")


def cargar_csv_a_bd(conn: sqlite3.Connection) -> tuple[int, int]:
    """
    Inserta registros del CSV en la BD, ignorando duplicados.
    Retorna (total_leidos, total_insertados).
    """
    if not os.path.exists(CSV_ORIGEN):
        raise FileNotFoundError(f"No se encontró el archivo CSV: {CSV_ORIGEN}")

    total_leidos    = 0
    total_insertados = 0

    with open(CSV_ORIGEN, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for fila in reader:
            total_leidos += 1
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO cripto_precios
                        (id, symbol, name, current_price, market_cap,
                         total_volume, price_change_percentage_24h, last_updated)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    fila["id"],
                    fila["symbol"],
                    fila["name"],
                    float(fila["current_price"]),
                    float(fila["market_cap"]),
                    float(fila["total_volume"]),
                    float(fila["price_change_percentage_24h"]),
                    fila["last_updated"],
                ))
                if conn.execute("SELECT changes()").fetchone()[0]:
                    total_insertados += 1
            except Exception as e:
                logger.warning(f"Fila omitida ({fila.get('id', '?')}): {e}")

    conn.commit()
    return total_leidos, total_insertados


def mostrar_resumen(conn: sqlite3.Connection) -> None:
    """Imprime un resumen de los datos almacenados en la BD."""
    total = conn.execute("SELECT COUNT(*) FROM cripto_precios").fetchone()[0]
    logger.info(f"Total de registros en BD: {total}")

    logger.info("Top 5 por market cap:")
    rows = conn.execute("""
        SELECT name, current_price, market_cap
        FROM cripto_precios
        ORDER BY market_cap DESC
        LIMIT 5
    """).fetchall()
    for r in rows:
        logger.info(f"  {r[0]:12s} | Precio: ${r[1]:>10,.2f} | Market Cap: ${r[2]:>20,.0f}")


def main():
    logger.info("=" * 50)
    logger.info("INICIO DE CARGA A BASE DE DATOS")
    logger.info(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")

    try:
        conn = sqlite3.connect(DB_DESTINO)
        logger.info(f"Conectado a BD SQLite: {DB_DESTINO}")

        crear_tabla(conn)

        leidos, insertados = cargar_csv_a_bd(conn)
        logger.info(f"Registros leídos del CSV : {leidos}")
        logger.info(f"Registros insertados en BD: {insertados}")
        logger.info(f"Duplicados ignorados      : {leidos - insertados}")

        mostrar_resumen(conn)
        logger.info("CARGA FINALIZADA CORRECTAMENTE")

    except Exception as e:
        logger.error(f"Error durante la carga: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()
        logger.info("=" * 50)


if __name__ == "__main__":
    main()
