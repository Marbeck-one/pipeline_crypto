#!/bin/bash
# Navegar al directorio del proyecto
cd /home/ubuntu/pipeline_crypto

# Activar el entorno virtual
source .venv/bin/activate

# Ejecutar el pipeline
python ingesta.py
python cargar_bd.py
python actualizar_dashboard.py

# Desplegar el HTML actualizado a la carpeta pública de Nginx
cp dashboard.html /var/www/html/index.html