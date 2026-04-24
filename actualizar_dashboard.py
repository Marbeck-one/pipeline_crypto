import sqlite3
import json
import re

def main():
    # 1. Extraer los datos más recientes de la base de datos
    conn = sqlite3.connect("data/db/cripto.db")
    conn.row_factory = sqlite3.Row
    rows = [dict(r) for r in conn.execute("SELECT * FROM cripto_precios ORDER BY market_cap DESC")]
    conn.close()

    json_data = json.dumps(rows, indent=2)

    # 2. Leer el archivo HTML actual
    with open("dashboard.html", "r", encoding="utf-8") as f:
        html = f.read()

    # 3. Reemplazar el bloque de 'const DATA = [...];' usando expresiones regulares
    patron = r"const DATA = \[.*?\];"
    reemplazo = f"const DATA = {json_data};"
    html_actualizado = re.sub(patron, reemplazo, html, flags=re.DOTALL)

    # 4. Sobrescribir el HTML
    with open("dashboard.html", "w", encoding="utf-8") as f:
        f.write(html_actualizado)

if __name__ == "__main__":
    main()