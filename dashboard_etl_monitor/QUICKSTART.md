# ⚡ QUICKSTART - Comienza en 5 Minutos

## 📌 Lo Que Necesitas (Una sola vez)

```bash
# 1. Terminal: Navega a la carpeta
cd c:\Users\herlbeng\Documents\Platform Partners\platform_partners\gcloud_monitoring\dashboard_etl_monitor

# 2. Terminal: Instala dependencias
pip install -r requirements.txt

# 3. Terminal: Autentica con Google Cloud
gcloud auth application-default login
# Se abrirá navegador, haz login con tu cuenta de Google
```

## 🎯 Ejecutar Dashboard (Cada Vez)

### Opción A: Comando Directo (Más Simple)

```bash
streamlit run iam_access_monitor.py
```

### Opción B: Script de Utilidad (Recomendado)

```bash
bash run_iam_monitor.sh dashboard
```

**Resultado:** Se abre automáticamente en `http://localhost:8501`

---

## 🎨 Interfaz del Dashboard

```
SIDEBAR (Arriba a la izquierda)
├─ Ambiente: [dev ▼]
├─ Tipo Resource: ● Dataset
└─ ☑ Mostrar Service Accounts

ÁREA PRINCIPAL
├─ 📊 Selecciona datasets
├─ Matriz: Datasets vs Usuarios
├─ Estadísticas
└─ [📥 Descargar CSV]
```

### Uso del Dashboard

1. **En el SIDEBAR:**
   - Selecciona "dev", "qua" o "pro"
   - Elige "Dataset" como tipo de recurso

2. **En el ÁREA PRINCIPAL:**
   - Se cargarán los datasets disponibles
   - Selecciona los que quieres analizar
   - Automáticamente verás la matriz

3. **Matriz:**
   - Filas = Datasets
   - Columnas = Usuarios + Service Accounts
   - `✓` = Acceso, `-` = Sin acceso

4. **Exportar:**
   - Click en `[📥 Descargar como CSV]`
   - Llevas el archivo para auditoría

---

## 🔄 Sincronizar Accesos (Avanzado)

### Capturar Snapshot Actual

```bash
# Todos los ambientes
python sync_iam_access.py --environment all

# Solo dev
python sync_iam_access.py --environment dev

# Ver qué pasaría (sin insertar)
python sync_iam_access.py --environment dev --dry-run
```

### Ver Cambios desde Última Sincronización

```bash
python sync_iam_access.py --environment dev --compare
```

---

## ✅ Verificación Rápida

### Problema: Pantalla en Blanco

```bash
# Verifica que gcloud está configurado
gcloud auth list
# Deberías ver tu cuenta listada

# Si no, ejecuta
gcloud auth application-default login
```

### Problema: "ModuleNotFoundError"

```bash
# Reinstala dependencias
pip install -r requirements.txt --force-reinstall
```

### Problema: "Port 8501 already in use"

```bash
# Otro Streamlit corriendo. Mátalo:
# Ctrl+C en la otra terminal
# O usa puerto diferente:
streamlit run iam_access_monitor.py --server.port 8502
```

---

## 📚 Archivos Importantes

| Archivo | Qué es | Cuándo usarlo |
|---------|--------|--------------|
| `iam_access_monitor.py` | Dashboard | `streamlit run ...` |
| `sync_iam_access.py` | Sincroniza datos | `python ...` diario |
| `sql_iam_access_analysis.sql` | 8 queries SQL | Query en BigQuery |
| `run_iam_monitor.sh` | Utilidad | `bash ... dashboard` |
| `SETUP_GUIDE.md` | Guía completa | Cuando tengas dudas |

---

## 🎓 Casos de Uso Comunes

### 📋 "Quiero ver quién tiene acceso a qué"

1. Abre: `streamlit run iam_access_monitor.py`
2. Selecciona ambiente y datasets
3. Verás matriz: Usuarios → Acceso
4. Descarga CSV si lo necesitas

### 🔍 "Quiero auditar cambios de hace X días"

1. Ejecuta: `python sync_iam_access.py --environment pro --compare`
2. Ve a BigQuery: `pph-central.management.iam_access_history`
3. Verás qué cambió, quién y cuándo

### 👤 "Quiero saber si un usuario tiene acceso a dataset X"

1. En el dashboard, selecciona el dataset
2. En la matriz, busca la columna del usuario
3. Si tiene `✓` = tiene acceso

### 🚪 "Usuario se va, ¿qué revoco?"

1. SQL rápido:
```sql
SELECT dataset_id FROM `pph-central.management.iam_access_snapshot`
WHERE principal_email = "user@company.com"
AND snapshot_date = CURRENT_DATE()
```

---

## 🚀 Pasos Siguientes

```
HOY (5 min)
├─ pip install -r requirements.txt
├─ gcloud auth application-default login
└─ streamlit run iam_access_monitor.py

MAÑANA (10 min)
├─ python sync_iam_access.py --environment all
└─ Verifica datos en BigQuery

ESTA SEMANA (30 min)
├─ Lee SETUP_GUIDE.md completo
├─ Ejecuta queries SQL en BigQuery
└─ Crea alertas personalizadas

PRÓXIMA SEMANA (1 hora)
├─ Deploy en Cloud Run
├─ Mapea dominio personalizado
└─ Comparte URL con el equipo
```

---

## 💾 Recomendación: Ejecutar Diariamente

### Opción 1: Manual

```bash
# Cada mañana
python sync_iam_access.py --environment all
```

### Opción 2: Automático (Cloud Scheduler)

```bash
# Ver DEPLOYMENT_GUIDE.md
# Sección: "Cloud Scheduler + Cloud Run"
```

---

## 🎯 TL;DR (Very Quick Start)

```bash
# 1. Una sola vez:
pip install -r requirements.txt
gcloud auth application-default login

# 2. Cada vez que quieras ver datos:
streamlit run iam_access_monitor.py

# 3. Listo! Usa el dashboard
# http://localhost:8501
```

---

## 📞 Ayuda Rápida

| Pregunta | Respuesta |
|----------|-----------|
| ¿No funciona? | Ver SETUP_GUIDE.md → Troubleshooting |
| ¿Cómo deploy? | Ver DEPLOYMENT_GUIDE.md |
| ¿Más queries SQL? | Ver sql_iam_access_analysis.sql |
| ¿Documentación? | Ver INDEX.md → Lee en orden |

---

**¡Listo! Comienza con:** `streamlit run iam_access_monitor.py` 🚀
