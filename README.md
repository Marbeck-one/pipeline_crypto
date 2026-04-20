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
| **Limpieza** | `ingesta.py` + `cargar_bd.py` | Selección de columnas, conversión de tipos y filtrado de filas inválidas |
| **Carga** | `cargar_bd.py` | Inserta los datos en SQLite con `INSERT OR IGNORE` para evitar duplicados |
| **Visualización** | `dashboard.html` | Dashboard HTML con precios, variaciones 24h, market cap y volumen |

---

## Estructura del proyecto

```
pipeline_cripto/
├── data/
│   ├── raw/
│   │   ├── cripto_precios.csv     ← Datos ingestados
│   │   └── .checkpoint.json      ← Control incremental
│   ├── db/
│   │   └── cripto.db             ← Base de datos SQLite
│   └── logs/
│       ├── ingesta.log           ← Trazabilidad de la ingesta
│       └── carga_bd.log          ← Trazabilidad de la carga a BD
├── ingesta.py                    ← Etapa de ingesta (fuente → CSV)
├── cargar_bd.py                  ← Etapa de carga (CSV → SQLite)
├── dashboard.html                ← Visualización del pipeline
├── requirements.txt              ← Dependencias
└── README.md
```

---

## Requisitos

- Python 3.8 o superior
- Librería `requests`

Instalar dependencias:
```bash
pip install -r requirements.txt
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
3. Guarda **solo los registros nuevos** en el CSV (ingesta incremental)
4. Actualiza el log con inicio, resultado y cantidad de registros procesados

### 2. Carga a base de datos
```bash
python cargar_bd.py
```

Cada ejecución:
1. Lee el CSV generado por `ingesta.py`
2. Crea la tabla `cripto_precios` si no existe
3. Inserta los registros ignorando duplicados por `(id, last_updated)`
4. Imprime un resumen con el top 5 por market cap

### 3. Visualización
Abrir `dashboard.html` directamente en el navegador.

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

## Limpieza de datos

La limpieza ocurre en dos momentos del pipeline:

- **`ingesta.py`**: selecciona solo las columnas definidas en `COLUMNAS`, descartando campos innecesarios de la API mediante `extrasaction="ignore"`.
- **`cargar_bd.py`**: convierte los valores numéricos con `float()` y omite con advertencia cualquier fila que contenga datos malformados o tipos incorrectos.

---

## Trazabilidad (Logging)

Ambos scripts generan logs en `data/logs/`:

- `ingesta.log`: timestamp de inicio/fin, registros recibidos, nuevos insertados y errores de conexión.
- `carga_bd.log`: timestamp de inicio/fin, registros leídos del CSV, insertados en BD, duplicados ignorados y resumen del top 5 por market cap.

---

## Fuente de datos

[CoinGecko API](https://www.coingecko.com/api/documentation) — API pública gratuita, sin necesidad de API key.