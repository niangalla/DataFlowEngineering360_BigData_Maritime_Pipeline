#!/bin/bash

# ==============================================================================
# DataFlow 360 - Start All Services
# ==============================================================================
# Ce script démarre tous les services Docker Compose dans le bon ordre
# ==============================================================================

set -e  # Exit on error

# Couleurs pour les messages
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  DataFlow 360 - Démarrage Services${NC}"
echo -e "${BLUE}========================================${NC}"
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

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Créer le réseau Docker s'il n'existe pas
# log_info "Création du réseau Docker 'my_network'..."
# docker network create my_network 2>/dev/null || log_warning "Le réseau 'my_network' existe déjà"

# FIX PERMISSIONS: Assurer que les dossiers de données sont accessibles par le conteneur Airflow (UID 50000)
log_info "Configuration des permissions sur les dossiers de données (via Docker)..."
mkdir -p src/data/source/from_apis
mkdir -p src/data/source/scraped
# Utiliser un conteneur temporaire pour changer les permissions (contourne le besoin de sudo local)
docker run --rm -v "$(pwd)/src/data:/data" alpine chmod -R 777 /data
log_success "Permissions appliquées (777) sur src/data/source/"
echo ""

# 1. Infrastructure de base (Hadoop, Kafka, Zookeeper, etc.)
# log_info "Démarrage de l'infrastructure de base..."
# docker compose -f docker-compose.yml up -d
# log_success "Infrastructure de base démarrée"
# echo ""

# Attendre que les services de base soient prêts
# log_info "Attente de la disponibilité des services de base (30s)..."
# sleep 30

# 2. Stockage (si séparé)
if [ -f "docker-compose.stockage.yml" ]; then
    log_info "Démarrage des services de stockage..."
    docker compose -f docker-compose.stockage.yml up -d
    log_success "Services de stockage démarrés"
    echo ""
    sleep 5
fi

# 5. Data Warehouse
log_info "Démarrage du Data Warehouse (PostgreSQL)..."
docker compose -f docker-compose.datawarehouse.yml up -d
log_success "Data Warehouse démarré"
echo ""

# 3. Core services (si séparé)
if [ -f "docker-compose.core.yml" ]; then
    log_info "Démarrage des services core..."
    docker compose -f docker-compose.core.yml up -d
    log_success "Services core démarrés"
    echo ""
    sleep 20
fi

# 6. Monitoring (Prometheus, Grafana, ELK)
if [ -f "docker-compose.monitoring.yml" ]; then
    log_info "Démarrage des services de monitoring..."
    docker compose -f docker-compose.monitoring.yml up -d
    log_success "Services de monitoring démarrés"
    echo ""
fi

# 4. Airflow
log_info "Démarrage d'Airflow..."
docker compose -f docker-compose.airflow.yml up -d
log_success "Airflow démarré"
echo ""

# Attendre qu'Airflow soit initialisé
log_info "Attente de l'initialisation d'Airflow (20s)..."
sleep 10

# Afficher l'état des conteneurs
echo ""
log_info "État des conteneurs Docker :"
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | head -20

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Tous les services sont démarrés !${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
log_info "Accès aux services :"
echo "  - Airflow UI:        http://localhost:8080 (admin/admin)"
echo "  - Spark Master UI:   http://localhost:8081"
echo "  - Hadoop NameNode:   http://localhost:9870"
echo "  - Kafka UI:          http://localhost:8090"
echo "  - Grafana:           http://localhost:3000 (si monitoring activé)"
echo ""
