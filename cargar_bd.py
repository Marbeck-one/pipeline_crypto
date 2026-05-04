"""
cargar_bd.py — Carga los datos del CSV limpio a Supabase (PostgreSQL)
Fase 1A – Migración SQLite → Supabase

Uso:
    python cargar_bd.py
"""

import os
import csv
import logging
import psycopg2
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────
os.makedirs("data/logs", exist_ok=True)

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
CSV_ORIGEN = "data/processed/cripto_precios_limpio.csv"

DB_CONFIG = {
    "host":     os.getenv("DB_HOST"),
    "port":     os.getenv("DB_PORT", 5432),
    "dbname":   os.getenv("DB_NAME", "postgres"),
    "user":     os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD"),
    "sslmode":  "require",          # Supabase exige SSL
    "connect_timeout": 10,
}


# ─────────────────────────────────────────────
# Módulos
# ─────────────────────────────────────────────

def conectar() -> psycopg2.extensions.connection:
    """Establece y retorna la conexión a Supabase."""
    conn = psycopg2.connect(**DB_CONFIG)
    logger.info("Conexión a Supabase establecida correctamente.")
    return conn


def crear_tabla(conn) -> None:
    """Crea la tabla si no existe (idempotente)."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS cripto_precios (
                id                          TEXT NOT NULL,
                symbol                      TEXT,
                name                        TEXT,
                current_price               NUMERIC,
                market_cap                  NUMERIC,
                total_volume                NUMERIC,
                price_change_percentage_24h NUMERIC,
                last_updated                TEXT,
                ingested_at                 TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (id, last_updated)
            );
        """)
    conn.commit()
    logger.info("Tabla 'cripto_precios' verificada/creada.")


def cargar_csv(conn) -> tuple[int, int]:
    """
    Inserta registros desde el CSV limpio usando ON CONFLICT DO NOTHING
    para evitar duplicados por la PK compuesta (id, last_updated).
    Retorna (total_leidos, total_insertados).
    """
    if not os.path.exists(CSV_ORIGEN):
        raise FileNotFoundError(f"CSV no encontrado: {CSV_ORIGEN}")

    leidos     = 0
    insertados = 0

    with open(CSV_ORIGEN, "r", encoding="utf-8") as f, conn.cursor() as cur:
        reader = csv.DictReader(f)
        for fila in reader:
            leidos += 1
            try:
                cur.execute("""
                    INSERT INTO cripto_precios
                        (id, symbol, name, current_price, market_cap,
                         total_volume, price_change_percentage_24h, last_updated)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id, last_updated) DO NOTHING;
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
                if cur.rowcount:
                    insertados += 1
            except Exception as e:
                logger.warning(f"Fila omitida ({fila.get('id','?')}): {e}")
                conn.rollback()

    conn.commit()
    return leidos, insertados


def mostrar_resumen(conn) -> None:
    """Loguea el top 5 por market cap desde Supabase."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT name, current_price, market_cap
            FROM cripto_precios
            ORDER BY market_cap DESC
            LIMIT 5;
        """)
        rows = cur.fetchall()

    total_query = "SELECT COUNT(*) FROM cripto_precios"
    with conn.cursor() as cur:
        cur.execute(total_query)
        total = cur.fetchone()[0]

    logger.info(f"Total registros en Supabase: {total}")
    logger.info("Top 5 por market cap:")
    for r in rows:
        logger.info(f"  {r[0]:15s} | Precio: ${r[1]:>10,.2f} | Market Cap: ${r[2]:>20,.0f}")


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    logger.info("=" * 50)
    logger.info("INICIO DE CARGA A SUPABASE")
    logger.info(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")

    conn = None
    try:
        conn = conectar()
        crear_tabla(conn)

        leidos, insertados = cargar_csv(conn)
        logger.info(f"Registros leídos   : {leidos}")
        logger.info(f"Registros insertados: {insertados}")
        logger.info(f"Duplicados ignorados: {leidos - insertados}")

        mostrar_resumen(conn)
        logger.info("CARGA FINALIZADA CORRECTAMENTE")

    except Exception as e:
        logger.error(f"Error durante la carga: {e}")
        raise
    finally:
        if conn:
            conn.close()
            logger.info("Conexión cerrada.")
        logger.info("=" * 50)


if __name__ == "__main__":
    main()