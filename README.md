# Pipeline de Datos – Ingesta de Criptomonedas
**Actividad 2.1 – DuocUC | Informática y Telecomunicaciones**

---

## Descripción

Este proyecto implementa un **pipeline de datos completo** con sus cinco etapas: fuente, ingesta, limpieza, carga y visualización.  
Obtiene automáticamente los precios de las 10 principales criptomonedas en tiempo real desde la **API pública de CoinGecko**, aplica ingesta incremental para evitar duplicados, carga los datos en una base de datos SQLite y los presenta en un dashboard interactivo.

---

## Etapas del Pipeline

| Etapa | Componente | Descripción |
|---|---|---|
| **Fuente** | CoinGecko API | API pública REST que provee precios, market cap y volumen en tiempo real |
| **Ingesta** | `ingesta.py` | Consulta la API y guarda los datos en CSV con control incremental |
| **Limpieza** | `limpieza.py` | Elimina nulos, duplicados y registros fuera de rango; estandariza formatos; agrega columnas derivadas |
| **Carga** | `cargar_bd.py` | Inserta los datos limpios en SQLite con `INSERT OR IGNORE` para evitar duplicados |
| **Visualización** | `dashboard.html` | Dashboard HTML con precios, variaciones 24h, market cap y volumen |

---

## Estructura del proyecto

```
pipeline_cripto/
├── data/
│   ├── raw/
│   │   ├── cripto_precios.csv          ← Datos ingestados (crudos)
│   │   └── .checkpoint.json           ← Control incremental
│   ├── processed/
│   │   └── cripto_precios_limpio.csv  ← Dataset limpio y transformado
│   ├── db/
│   │   └── cripto.db                  ← Base de datos SQLite
│   └── logs/
│       ├── ingesta.log                ← Trazabilidad de la ingesta
│       ├── limpieza.log               ← Trazabilidad de la limpieza
│       └── carga_bd.log               ← Trazabilidad de la carga a BD
├── ingesta.py                         ← Etapa de ingesta (fuente → CSV crudo)
├── limpieza.py                        ← Etapa de limpieza (CSV crudo → CSV limpio)
├── cargar_bd.py                       ← Etapa de carga (CSV limpio → SQLite)
├── actualizar_dashboard.py            ← Exporta datos de SQLite al dashboard
├── dashboard.html                     ← Visualización del pipeline
├── requirements.txt                   ← Dependencias
└── README.md
```

---

## Requisitos

- Python 3.8 o superior
- Librería `requests`
- Navegador web (Chrome, Firefox, Edge)
- Conexión a internet (solo para modo real; el modo demo no la requiere)

---

## Despliegue local

### Paso 1 — Clonar o descargar el proyecto

```bash
git clone <url-del-repositorio>
cd pipeline_cripto
```

O descomprimir el ZIP del proyecto y acceder a la carpeta desde la terminal.

### Paso 2 — Crear y activar el entorno virtual

**Windows:**
```bash
python -m venv .venv
.venv\Scripts\activate
```

**macOS / Linux:**
```bash
python3 -m venv .venv
source .venv/bin/activate
```

Una vez activo, el prompt de la terminal mostrará `(.venv)` al inicio.

### Paso 3 — Instalar dependencias

```bash
pip install -r requirements.txt
```

### Paso 4 — Ejecutar el pipeline completo

Los scripts deben correrse **en orden**. Cada etapa depende de la anterior.

**4.1 — Ingesta** (obtiene datos y genera el CSV crudo):
```bash
python ingesta.py          # Modo real — requiere conexión a internet
python ingesta.py --demo   # Modo demo — usa datos simulados, sin internet
```

**4.2 — Limpieza y transformación** (limpia el CSV crudo y genera el dataset procesado):
```bash
python limpieza.py
```

**4.3 — Carga a base de datos** (lee el CSV limpio y carga a SQLite):
```bash
python cargar_bd.py
```

**4.4 — Visualización** (exportar datos de SQLite al dashboard):

El archivo `dashboard.html` tiene los datos **embebidos directamente desde SQLite** como JSON estático. Para actualizarlo con los datos más recientes de la BD, ejecutar la siguiente consulta y reemplazar el bloque `const DATA = [...]` en el HTML:

```bash
python - <<'EOF'
import sqlite3, json
conn = sqlite3.connect("data/db/cripto.db")
conn.row_factory = sqlite3.Row
rows = [dict(r) for r in conn.execute("SELECT * FROM cripto_precios ORDER BY market_cap DESC")]
print(json.dumps(rows, indent=2))
conn.close()
EOF
```

Luego abrir `dashboard.html` directamente en el navegador (doble clic o arrastrar al navegador).

> **Nota:** No se requiere servidor web. El dashboard es un archivo HTML estático que funciona abriéndolo localmente.

### Paso 5 — Verificar los logs

Revisar que todo corrió correctamente:

```
data/logs/ingesta.log    ← resultado de la ingesta
data/logs/carga_bd.log   ← resultado de la carga a SQLite
```

---

## Flujo completo resumido

```
[CoinGecko API]
      ↓
python ingesta.py        →  data/raw/cripto_precios.csv
      ↓
python limpieza.py       →  data/processed/cripto_precios_limpio.csv
      ↓
python cargar_bd.py      →  data/db/cripto.db
      ↓
dashboard.html           →  abrir en el navegador
```

---

## Cómo ejecutar

### 1. Ingesta de datos
```bash
python ingesta.py          # Modo real (requiere internet)
python ingesta.py --demo   # Modo demo con datos simulados
```

Cada ejecución:
1. Consulta la API de CoinGecko
2. Compara con el checkpoint de ejecuciones anteriores
3. Guarda **solo los registros nuevos** en el CSV crudo (ingesta incremental)
4. Actualiza el log con inicio, resultado y cantidad de registros procesados

### 2. Limpieza y transformación
```bash
python limpieza.py
```

Cada ejecución:
1. Lee el CSV crudo desde `data/raw/`
2. Elimina registros con campos nulos o vacíos
3. Elimina duplicados por clave `(id, last_updated)`
4. Convierte y valida los tipos numéricos
5. Filtra registros con precio o market_cap fuera de rango
6. Estandariza textos (`symbol` en minúsculas, `name` en título) y fechas (ISO 8601 sin milisegundos)
7. Agrega columnas derivadas: `market_cap_category` y `price_change_label`
8. Guarda el resultado en `data/processed/cripto_precios_limpio.csv`

### 3. Carga a base de datos
```bash
python cargar_bd.py
```

Cada ejecución:
1. Lee el CSV limpio desde `data/processed/`
2. Crea la tabla `cripto_precios` si no existe
3. Inserta los registros ignorando duplicados por `(id, last_updated)`
4. Imprime un resumen con el top 5 por market cap

### 4. Visualización
Abrir `dashboard.html` directamente en el navegador (doble clic o arrastrar al navegador).

---

## Datos obtenidos

| Campo | Descripción |
|---|---|
| `id` | Identificador único de la moneda |
| `symbol` | Símbolo (ej: `btc`) |
| `name` | Nombre completo |
| `current_price` | Precio actual en USD |
| `market_cap` | Capitalización de mercado |
| `total_volume` | Volumen de transacciones 24h |
| `price_change_percentage_24h` | Variación de precio en 24h (%) |
| `last_updated` | Timestamp de última actualización |

---

## Ingesta incremental

El archivo `.checkpoint.json` registra los IDs ya procesados.  
En cada ejecución, el script compara los datos recibidos con ese historial e inserta únicamente los registros que aún no han sido capturados, evitando duplicados.

---

## Limpieza y transformación de datos

La etapa de limpieza se ejecuta en `limpieza.py` y actúa como puente entre los datos crudos y la base de datos. Las transformaciones aplicadas son:

**Limpieza:**
- Eliminación de registros con cualquier campo requerido nulo o vacío.
- Eliminación de duplicados por clave compuesta `(id, last_updated)`.
- Descarte de filas con errores de tipo en campos numéricos.
- Filtrado de registros con `current_price <= 0` o `market_cap <= 0`.

**Estandarización:**
- `symbol` → minúsculas (ej: `BTC` → `btc`).
- `name` → formato título (ej: `BITCOIN` → `Bitcoin`).
- `last_updated` → ISO 8601 sin milisegundos ni zona horaria (ej: `2026-04-20T04:19:57.441Z` → `2026-04-20T04:19:57`).
- Campos numéricos convertidos explícitamente a `float`.

**Columnas derivadas:**
- `market_cap_category`: clasifica la moneda como `Large Cap` (≥ 10 B), `Mid Cap` (1–10 B) o `Small Cap` (< 1 B).
- `price_change_label`: etiqueta la variación 24h como `positiva` (> 0.1%), `negativa` (< −0.1%) o `estable`.

Todos los pasos quedan registrados en `data/logs/limpieza.log`.

---

## Trazabilidad (Logging)

Los tres scripts principales generan logs en `data/logs/`:

- `ingesta.log`: timestamp de inicio/fin, registros recibidos, nuevos insertados y errores de conexión.
- `limpieza.log`: timestamp de inicio/fin, registros leídos, eliminados por cada criterio (nulos, duplicados, tipo, rango) y transformaciones aplicadas.
- `carga_bd.log`: timestamp de inicio/fin, registros leídos del CSV limpio, insertados en BD, duplicados ignorados y resumen del top 5 por market cap.

---

## Fuente de datos

[CoinGecko API](https://www.coingecko.com/api/documentation) — API pública gratuita, sin necesidad de API key.