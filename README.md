# Pipeline de Datos – Ingesta de Criptomonedas
**Actividad 2.1 – DuocUC | Informática y Telecomunicaciones**

---

## Descripción

Este proyecto implementa la **etapa de ingesta** de un pipeline de datos.  
El script `ingesta.py` obtiene automáticamente los precios de las 10 principales criptomonedas en tiempo real desde la **API pública de CoinGecko**, aplica una lógica de **ingesta incremental** para no duplicar registros, y almacena los datos organizados en `data/raw/`.

---

## Estructura del proyecto

```
pipeline_cripto/
├── data/
│   ├── raw/
│   │   ├── cripto_precios.csv     ← Datos ingestados
│   │   └── .checkpoint.json      ← Control incremental
│   └── logs/
│       └── ingesta.log            ← Trazabilidad del proceso
├── ingesta.py                     ← Script principal
├── requirements.txt               ← Dependencias
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

```bash
python ingesta.py
```

Cada ejecución:
1. Consulta la API de CoinGecko
2. Compara con el checkpoint de ejecuciones anteriores
3. Guarda **solo los registros nuevos** en el CSV (ingesta incremental)
4. Actualiza el log con inicio, resultado y cantidad de registros procesados

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

## Trazabilidad (Logging)

El archivo `data/logs/ingesta.log` registra:
- Timestamp de inicio y fin
- Cantidad de registros obtenidos desde la API
- Cantidad de registros nuevos insertados
- Errores en caso de fallo de conexión u otro problema

---

## Fuente de datos

[CoinGecko API](https://www.coingecko.com/api/documentation) — API pública gratuita, sin necesidad de API key.
