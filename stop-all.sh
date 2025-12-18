#!/bin/bash

# ==============================================================================
# DataFlow 360 - Stop All Services
# ==============================================================================
# Ce script arrête tous les services Docker Compose dans l'ordre inverse
# ==============================================================================

set -e  # Exit on error

# Couleurs pour les messages
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${RED}========================================${NC}"
echo -e "${RED}  DataFlow 360 - Arrêt Services${NC}"
echo -e "${RED}========================================${NC}"
echo ""

# Fonction pour afficher les messages
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# 1. Airflow
log_info "Arrêt d'Airflow..."
docker compose -f docker-compose.airflow.yml down -v
log_success "Airflow arrêté"
echo ""

# 2. Core services (si séparé)
if [ -f "docker-compose.core.yml" ]; then
    log_info "Arrêt des services core..."
    docker compose -f docker-compose.core.yml down -v
    log_success "Services core arrêtés"
    echo ""
fi

# 3. Data Warehouse
log_info "Arrêt du Data Warehouse..."
docker compose -f docker-compose.datawarehouse.yml down -v
log_success "Data Warehouse arrêté"
echo ""

# 4. Monitoring (en premier car moins critique)
if [ -f "docker-compose.monitoring.yml" ]; then
    log_info "Arrêt des services de monitoring..."
    docker compose -f docker-compose.monitoring.yml down -v
    log_success "Services de monitoring arrêtés"
    echo ""
fi

# 5. Stockage (si séparé)
if [ -f "docker-compose.stockage.yml" ]; then
    log_info "Arrêt des services de stockage..."
    docker compose -f docker-compose.stockage.yml down -v
    log_success "Services de stockage arrêtés"
    echo ""
fi

# 6. Infrastructure de base (en dernier car elle est la fondation)
# log_info "Arrêt de l'infrastructure de base..."
# docker compose -f docker-compose.yml down
# log_success "Infrastructure de base arrêtée"
# echo ""

# Option: Supprimer le réseau (décommentez si souhaité)
# log_info "Suppression du réseau Docker 'my_network'..."
# docker network rm my_network 2>/dev/null || log_warning "Le réseau 'my_network' n'existe pas ou est utilisé"

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Tous les services sont arrêtés !${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
log_info "Conteneurs restants :"
docker ps -a --format "table {{.Names}}\t{{.Status}}" | grep -E "Exit|Up" || echo "Aucun conteneur actif"
echo ""
