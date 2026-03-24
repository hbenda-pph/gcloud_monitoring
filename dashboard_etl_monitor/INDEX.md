# 🔐 IAM Access Monitor - Resumen Ejecutivo

## ✨ Lo Que He Creado Para Ti

He generado un **sistema completo de monitoreo de accesos y credenciales en BigQuery** con:

### 🎯 Componentes Principales

| Archivo | Descripción | Propósito |
|---------|-------------|----------|
| **iam_access_monitor.py** | Dashboard Streamlit interactivo | Visualizar matriz de roles vs usuarios |
| **sync_iam_access.py** | Script de sincronización | Capturar snapshots de IAM en BigQuery |
| **sql_iam_access_analysis.sql** | 8 queries SQL avanzadas | Análisis profundo de accesos y cambios |
| **run_iam_monitor.sh** | CLI de utilidad | Ejecutar dashboard y sincronización fácilmente |
| **Dockerfile.iam_monitor** | Imagen Docker | Desplegar en Cloud Run |
| **cloudbuild.iam_monitor.yaml** | CI/CD configuration | Deploy automático |

### 📚 Documentación Completa

| Documento | Contenido |
|-----------|----------|
| **SETUP_GUIDE.md** | Guía completa de instalación y configuración |
| **README_IAM_ACCESS_MONITOR.md** | Manual de uso del dashboard |
| **DEPLOYMENT_GUIDE.md** | Instrucciones de despliegue en Cloud Run |
| **sql_iam_access_analysis.sql** | Queries SQL documentadas con ejemplos |

---

## 🚀 Inicio Rápido (5 minutos)

### 1️⃣ Instalar

```bash
cd dashboard_etl_monitor
pip install -r requirements.txt
```

### 2️⃣ Autenticar

```bash
gcloud auth application-default login
```

### 3️⃣ Ejecutar Dashboard

```bash
streamlit run iam_access_monitor.py
# O
bash run_iam_monitor.sh dashboard
```

Accede a: `http://localhost:8501`

---

## 📊 Características del Dashboard

### Matriz de Acceso Interactiva

```
                usuario@co  sa@project  contractor
data-prod          ✓           ✓            -
reports            ✓           ✓            ✓
sensitive          ✓           -            -
```

- **Filas**: Datasets
- **Columnas**: Usuarios + Service Accounts
- **Símbolos**: 
  - `✓` = Acceso disponible
  - `-` = Sin acceso
  - `🤖` = Service Account
  - `👤` = Usuario regular

### Filtros Disponibles

- ✅ Seleccionar múltiples datasets
- ✅ Filtrar por ambiente (dev/qua/pro)
- ✅ Mostrar/ocultar service accounts
- ✅ Mostrar permisos heredados
- ✅ Descargar como CSV

---

## 🔄 Sincronización de Accesos

### Capturar Snapshot Actual

```bash
# Todos los ambientes
python sync_iam_access.py --environment all

# Solo development
python sync_iam_access.py --environment dev

# Dry-run (ver sin insertar)
python sync_iam_access.py --environment dev --dry-run
```

### Detectar Cambios

```bash
# Compara con snapshot anterior
python sync_iam_access.py --environment dev --compare
```

Los datos se guardan en: `pph-central.management.iam_access_snapshot`

---

## 📈 Consultas SQL Analíticas

8 queries preconfiguradas:

1. **Acceso por Dataset** - Quién accede a cada dataset
2. **Service Accounts Activos** - Listado de SAs del sistema
3. **Matriz de Acceso** - Basada en Cloud Audit Logs
4. **Usuarios Inactivos** - Sin actividad en 30+ días
5. **Cambios Recientes** - Últimos 7 días
6. **Resumen de Permisos** - Por mes y usuario
7. **Patrones Anómales** - Detección de anomalías
8. **Auditoría IAM** - Histórico de cambios

Usa el archivo [sql_iam_access_analysis.sql](sql_iam_access_analysis.sql)

---

## 🌐 Despliegue en Cloud Run

```bash
# Opción 1: Cloud Build automático
gcloud builds submit \
  --config=cloudbuild.iam_monitor.yaml \
  --substitutions=_SERVICE_NAME=iam-access-monitor

# Opción 2: Despliegue directo
gcloud run deploy iam-access-monitor \
  --source . \
  --platform managed \
  --region us-central1 \
  --memory 2Gi
```

Ver más en [DEPLOYMENT_GUIDE.md](DEPLOYMENT_GUIDE.md)

---

## 📋 Arquitectura

```
┌─────────────────────────────────────┐
│   Dashboard Streamlit (UI)          │
│   📊 Matriz de Roles vs Usuarios    │
│   🔍 Filtros interactivos           │
└────────────────┬────────────────────┘
                 │
        ┌────────┴────────┐
        │                 │
    ┌───▼────┐      ┌──────▼──────┐
    │BigQuery│      │  Sync SAG   │
    │ (read) │      │ (snapshots) │
    └────────┘      └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │   BigQuery  │
                    │   (audit tb)│
                    └─────────────┘
```

---

## ✅ Checklist de Configuración

- [ ] `pip install -r requirements.txt` ✓
- [ ] `gcloud auth application-default login` ✓
- [ ] Dataset `pph-central.management` existe
- [ ] Permisos `roles/bigquery.admin` o similar
- [ ] Ejecutar: `bash run_iam_monitor.sh dashboard`

---

## 🔐 Seguridad

- ✅ Lectura de datos únicamente (sin modificaciones)
- ✅ Autenticación via Google Cloud
- ✅ Service account con permisos mínimos recomendados
- ✅ Datos en tiempo real, no almacenados localmente
- ✅ HTTPS en Cloud Run

---

## 🎯 Casos de Uso

### 📋 Auditoría de Cumplimiento
Verifica quién tiene acceso a datos sensibles
```bash
# En el dashboard selecciona:
1. Ambiente: pro
2. Datasets sensibles
3. Descarga CSV → compartí con compliance
```

### 🚪 Offboarding de Empleado
Identifica qué revocar al partir un empleado
```sql
SELECT * FROM iam_access_snapshot
WHERE principal_email = "departing@company.com"
```

### 🔔 Cambios en Acceso
Detecta cuándo y quién cambió permisos
```bash
python sync_iam_access.py --environment dev --compare
```

### 📊 Reporte Mensual
Genera estadísticas de control de acceso
```bash
# Ejecuta query #6 en sql_iam_access_analysis.sql
```

---

## 📁 Estructura de Archivos Nuevos

```
dashboard_etl_monitor/
├── 🎯 ARCHIVOS NUEVOS:
│   ├── iam_access_monitor.py ..................... Dashboard principal
│   ├── sync_iam_access.py ........................ Script de sync
│   ├── sql_iam_access_analysis.sql .............. 8 queries SQL
│   ├── run_iam_monitor.sh ........................ CLI de utilidad
│   ├── Dockerfile.iam_monitor ................... Para Cloud Run
│   ├── cloudbuild.iam_monitor.yaml ............. CI/CD config
│   ├── SETUP_GUIDE.md ........................... Guía de instalación
│   ├── README_IAM_ACCESS_MONITOR.md ............. Manual de uso
│   ├── DEPLOYMENT_GUIDE.md ...................... Deploy guide
│   └── INDEX.md (este archivo) .................. Resumen ejecutivo
│
└── 📝 ARCHIVOS MODIFICADOS:
    └── requirements.txt ......................... Nuevas dependencias
```

---

## 🔗 Documentación (Lee en Este Orden)

1. **📍 INDEX.md** ← Tú estás aquí (5 min)
2. **🚀 SETUP_GUIDE.md** - Configuración inicial (10 min)
3. **📊 README_IAM_ACCESS_MONITOR.md** - Usar dashboard (5 min)
4. **🌐 DEPLOYMENT_GUIDE.md** - Deploy en producción (10 min)
5. **📈 sql_iam_access_analysis.sql** - Queries avanzadas (referencia)

---

## 🆘 Troubleshooting Rápido

| Problema | Solución |
|----------|----------|
| `ModuleNotFoundError` | `pip install -r requirements.txt` |
| `PermissionDenied` | `gcloud auth application-default login` |
| Dashboard no abre | Verifica puerto 8501: `http://localhost:8501` |
| Sin datasets | Verifica permisos en proyecto GCP |

Más detalles en `SETUP_GUIDE.md`

---

## 💡 Tips y Mejores Prácticas

✅ **Hacer:**
- Ejecutar sync_iam_access.py regularmente (diario/semanal)
- Revisar matriz antes de cambios de permisos
- Usar CSV exports para auditoría
- Monitorear usuarios inactivos

❌ **Evitar:**
- Ejecutar en paralelo (múltiples sync simultáneos)
- Dejar service accounts sin usar
- Cambiar permisos sin log histórico
- Usar credenciales de usuario en producción

---

## 🎓 Próximos Pasos

### Nivel 1: Usar el Dashboard (Hoy)
```bash
bash run_iam_monitor.sh install
bash run_iam_monitor.sh dashboard
```

### Nivel 2: Sincronizar Accesos (Mañana)
```bash
python sync_iam_access.py --environment all
# Programar con Cloud Scheduler
```

### Nivel 3: Deploy en Cloud Run (Esta Semana)
```bash
gcloud builds submit --config cloudbuild.iam_monitor.yaml
# Compartir URL con el equipo
```

### Nivel 4: Análisis Avanzado (Luego)
- Ejecutar queries SQL personalizadas
- Crear alertas por Slack
- Integrar con CMS/ITSM

---

## 📞 Soporte y Contacto

**Para:**
- Errores técnicos → Ver SETUP_GUIDE.md → Troubleshooting
- Features nuevos → Contactar Platform Engineering
- Preguntas SQL → Revisar sql_iam_access_analysis.sql

---

## 📊 Estadísticas de Desarrollo

| Métrica | Valor |
|---------|-------|
| Archivos creados | 6 |
| Docs generadas | 4 |
| Líneas de código | ~1,500 |
| Queries SQL | 8 |
| Tiempo setup: | ~5 min |

---

## 🎉 Resumen

Tienes todo para:

✅ Ver matriz interactiva de accesos en BigQuery  
✅ Auditar quién tiene acceso a qué  
✅ Sincronizar snapshots de IAM para histórico  
✅ Ejecutar 8 queries SQL de análisis  
✅ Deployar en Cloud Run en producción  

**¡Comienza con `bash run_iam_monitor.sh dashboard`!** 🚀

---

**Última actualización:** Marzo 2026  
**Versión:** 1.0.0  
**Status:** ✅ Listo para usar
