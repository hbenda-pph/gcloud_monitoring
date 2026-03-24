#!/bin/bash

##############################################################################
# Script: Ejecutor del Dashboard IAM Access Monitor
# Descripción: Facilita la ejecución del dashboard y scripts complementarios
# Uso: ./run_iam_monitor.sh [comando] [opciones]
##############################################################################

set -e

# Colores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Funciones auxiliares
log_info() {
    echo -e "${BLUE}ℹ ${1}${NC}"
}

log_success() {
    echo -e "${GREEN}✓ ${1}${NC}"
}

log_warning() {
    echo -e "${YELLOW}⚠ ${1}${NC}"
}

log_error() {
    echo -e "${RED}✗ ${1}${NC}"
}

print_header() {
    echo -e "\n${BLUE}════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}  ${1}${NC}"
    echo -e "${BLUE}════════════════════════════════════════════════════════${NC}\n"
}

# Verificar dependencias
check_dependencies() {
    print_header "Verificando dependencias..."
    
    if ! command -v python3 &> /dev/null; then
        log_error "Python 3 no está instalado"
        exit 1
    fi
    log_success "Python 3 encontrado"
    
    if ! command -v gcloud &> /dev/null; then
        log_warning "gcloud CLI no está instalado. Algunas funciones podrían no funcionar"
    else
        log_success "gcloud CLI encontrado"
    fi
    
    if ! command -v streamlit &> /dev/null 2>&1; then
        log_warning "Streamlit no está instalado. Instalando..."
        pip install streamlit
    else
        log_success "Streamlit encontrado"
    fi
}

# Instalar dependencias
install_deps() {
    print_header "Instalando dependencias..."
    
    if [ -f "requirements.txt" ]; then
        log_info "Instalando desde requirements.txt..."
        pip install -r requirements.txt
        log_success "Dependencias instaladas"
    else
        log_error "requirements.txt no encontrado"
        exit 1
    fi
}

# Verificar autenticación
check_auth() {
    print_header "Verificando autenticación de Google Cloud..."
    
    if [ -z "$GOOGLE_APPLICATION_CREDENTIALS" ]; then
        log_warning "GOOGLE_APPLICATION_CREDENTIALS no está configurada"
        log_info "Ejecutando: gcloud auth application-default login"
        gcloud auth application-default login
    else
        log_success "GOOGLE_APPLICATION_CREDENTIALS configurada"
        log_info "Archivo: $GOOGLE_APPLICATION_CREDENTIALS"
    fi
}

# Ejecutar dashboard
run_dashboard() {
    print_header "Iniciando Dashboard IAM Access Monitor"
    
    log_info "URL: http://localhost:8501"
    log_info "Presiona Ctrl+C para detener"
    
    streamlit run iam_access_monitor.py
}

# Ejecutar sincronización de IAM
run_sync() {
    local environment=${1:-"all"}
    local dry_run=${2:-""}
    
    print_header "Ejecutando Sincronización IAM"
    
    cmd="python sync_iam_access.py --environment $environment"
    
    if [ "$dry_run" = "--dry-run" ]; then
        cmd="$cmd --dry-run"
    fi
    
    log_info "Comando: $cmd"
    eval "$cmd"
}

# Mostrar uso
show_help() {
    cat << EOF
${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}
${GREEN}IAM Access Monitor - Ejecutor${NC}
${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}

${YELLOW}USO:${NC}
    ./run_iam_monitor.sh [COMANDO] [OPCIONES]

${YELLOW}COMANDOS:${NC}
    ${GREEN}dashboard${NC}              Ejecuta el dashboard Streamlit
    ${GREEN}install${NC}                Instala dependencias
    ${GREEN}sync${NC}                   Sincroniza accesos IAM a BigQuery
    ${GREEN}sync-dry-run${NC}           Sincroniza (dry-run, sin insertar datos)
    ${GREEN}check-auth${NC}             Verifica autenticación de GCP
    ${GREEN}check-deps${NC}             Verifica dependencias
    ${GREEN}help${NC}                   Muestra este mensaje

${YELLOW}EJEMPLOS:${NC}
    # Instalar dependencias e iniciar dashboard
    ./run_iam_monitor.sh install
    ./run_iam_monitor.sh check-auth
    ./run_iam_monitor.sh dashboard

    # Sincronizar accesos de todos los ambientes
    ./run_iam_monitor.sh sync

    # Sincronizar solo ambiente dev (sin insertar datos)
    ./run_iam_monitor.sh sync dev --dry-run

    # Sincronizar y detectar cambios desde último snapshot
    ./run_iam_monitor.sh sync all --compare

${YELLOW}CONFIGURACIÓN:${NC}
    - Edita ENVIRONMENT_CONFIG en iam_access_monitor.py para agregar proyectos
    - Usa gcloud auth application-default login para autenticarse
    - Requiere rol: roles/bigquery.admin o roles/bigquery.dataViewer

${YELLOW}DOCUMENTACIÓN:${NC}
    - README_IAM_ACCESS_MONITOR.md - Guía completa del dashboard
    - sql_iam_access_analysis.sql - Consultas SQL para análisis
    - sync_iam_access.py - Script de sincronización

${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}
EOF
}

# Main
case "${1:-help}" in
    dashboard)
        check_dependencies
        check_auth
        run_dashboard
        ;;
    install)
        check_dependencies
        install_deps
        log_success "Setup completado. Ahora ejecuta: ./run_iam_monitor.sh dashboard"
        ;;
    sync)
        check_dependencies
        check_auth
        shift
        run_sync "$@"
        ;;
    sync-dry-run)
        check_dependencies
        check_auth
        shift
        run_sync "${1:-all}" "--dry-run"
        ;;
    check-auth)
        check_auth
        ;;
    check-deps)
        check_dependencies
        ;;
    help)
        show_help
        ;;
    *)
        log_error "Comando desconocido: $1"
        show_help
        exit 1
        ;;
esac
