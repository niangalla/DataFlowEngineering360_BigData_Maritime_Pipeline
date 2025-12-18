#!/usr/bin/env python3
"""
Script de test pour explorer les endpoints Sinay API et trouver celui pour l'historique.
"""

import requests
import json
import os
from datetime import datetime, timedelta
import os
# from dotenv import load_dotenv

# load_dotenv()

SAFECUBE_API_KEY = os.getenv('SAFECUBE_API_KEY')
DAKAR_UNLOCODE = 'SNDKR'

# Période de test (dernier mois)
start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
end_date = datetime.now().strftime('%Y-%m-%d')

print("=" * 80)
print("🔬 TEST DES ENDPOINTS SINAY API")
print("=" * 80)
print(f"📅 Période de test: {start_date} → {end_date}")
print(f"🚢 Port: {DAKAR_UNLOCODE} (Dakar)")
print(f"🔑 API Key: {'✅ Configurée' if SAFECUBE_API_KEY else '❌ Manquante'}")
print("=" * 80)

headers = {
    'API_KEY': SAFECUBE_API_KEY,
    'Accept': 'application/json'
}

# Liste des endpoints à tester
endpoints_to_test = [
    {
        "name": "Schedules (Actuel - Futur)",
        "url": "https://api.sinay.ai/schedule/api/v1/schedules/port/traffic",
        "params": {
            'unlocode': DAKAR_UNLOCODE,
            'start_date': start_date,
            'end_date': end_date,
            'limit': 10
        }
    },
    {
        "name": "Vessels Port Calls",
        "url": "https://api.sinay.ai/vessels/api/v1/port/calls",
        "params": {
            'unlocode': DAKAR_UNLOCODE,
            'start_date': start_date,
            'end_date': end_date,
            'limit': 10
        }
    },
    {
        "name": "Historical Port Calls",
        "url": "https://api.sinay.ai/vessels/api/v1/historical/port-calls",
        "params": {
            'unlocode': DAKAR_UNLOCODE,
            'start_date': start_date,
            'end_date': end_date,
            'limit': 10
        }
    },
    {
        "name": "Port Traffic Historical",
        "url": "https://api.sinay.ai/schedule/api/v1/port/traffic/historical",
        "params": {
            'unlocode': DAKAR_UNLOCODE,
            'start_date': start_date,
            'end_date': end_date,
            'limit': 10
        }
    },
    {
        "name": "AIS Historical Port Calls",
        "url": "https://api.sinay.ai/ais/api/v1/port/calls",
        "params": {
            'unlocode': DAKAR_UNLOCODE,
            'start_date': start_date,
            'end_date': end_date,
            'limit': 10
        }
    },
    {
        "name": "Vessels Intelligence Port Calls",
        "url": "https://api.sinay.ai/vessels-intelligence/api/v1/port/calls",
        "params": {
            'unlocode': DAKAR_UNLOCODE,
            'from_date': start_date,
            'to_date': end_date,
            'limit': 10
        }
    }
]

successful_endpoints = []

for i, endpoint in enumerate(endpoints_to_test, 1):
    print(f"\n{'─' * 80}")
    print(f"📡 Test {i}/{len(endpoints_to_test)}: {endpoint['name']}")
    print(f"🔗 URL: {endpoint['url']}")
    print(f"📋 Params: {endpoint['params']}")
    print(f"{'─' * 80}")
    
    try:
        response = requests.get(
            endpoint['url'],
            headers=headers,
            params=endpoint['params'],
            timeout=10
        )
        
        print(f"📊 Status Code: {response.status_code}")
        
        if response.status_code == 200:
            try:
                data = response.json()
                print(f"✅ SUCCESS! Réponse valide")
                print(f"\n📦 Structure de la réponse:")
                print(f"   Type: {type(data)}")
                
                if isinstance(data, dict):
                    print(f"   Clés: {list(data.keys())}")
                    
                    # Chercher les données d'arrivées/départs
                    if 'data' in data:
                        data_section = data['data']
                        if isinstance(data_section, dict):
                            print(f"   data.keys: {list(data_section.keys())}")
                            
                            arrivals = data_section.get('arrivals', [])
                            departures = data_section.get('departures', [])
                            
                            if arrivals or departures:
                                print(f"\n🚢 Données trouvées:")
                                print(f"   Arrivées: {len(arrivals)}")
                                print(f"   Départs: {len(departures)}")
                                
                                # Analyser les dates
                                if arrivals:
                                    first_arrival = arrivals[0]
                                    print(f"\n📅 Premier événement (arrivée):")
                                    print(f"   {json.dumps(first_arrival, indent=2)[:500]}")
                                    
                                    # Extraire la date
                                    date_field = first_arrival.get('arrival_time') or first_arrival.get('eta') or first_arrival.get('date')
                                    if date_field:
                                        print(f"\n⏰ Date détectée: {date_field}")
                                        try:
                                            parsed_date = datetime.fromisoformat(str(date_field).replace('Z', '+00:00'))
                                            now = datetime.now(parsed_date.tzinfo)
                                            
                                            if parsed_date < now:
                                                print(f"   ✅ PASSÉ - C'est de l'historique réel!")
                                            else:
                                                print(f"   ⚠️  FUTUR - Ce sont des prévisions")
                                        except:
                                            print(f"   ❓ Impossible de parser la date")
                
                # Sauvegarder l'endpoint qui fonctionne
                successful_endpoints.append({
                    'name': endpoint['name'],
                    'url': endpoint['url'],
                    'response_preview': str(data)[:200]
                })
                
            except json.JSONDecodeError:
                print(f"❌ Réponse non-JSON")
                print(f"   Contenu: {response.text[:200]}")
        
        elif response.status_code == 404:
            print(f"❌ Endpoint introuvable (404)")
        elif response.status_code == 401:
            print(f"❌ Non autorisé (401) - Vérifier API Key")
        elif response.status_code == 403:
            print(f"❌ Accès refusé (403) - Endpoint peut nécessiter permissions spéciales")
        else:
            print(f"❌ Erreur {response.status_code}")
            print(f"   Message: {response.text[:200]}")
    
    except requests.exceptions.Timeout:
        print(f"⏱️  Timeout - L'endpoint ne répond pas")
    except requests.exceptions.ConnectionError:
        print(f"🔌 Erreur de connexion")
    except Exception as e:
        print(f"💥 Erreur: {str(e)}")

# Résumé
print(f"\n{'=' * 80}")
print(f"📊 RÉSUMÉ")
print(f"{'=' * 80}")
print(f"✅ Endpoints fonctionnels: {len(successful_endpoints)}/{len(endpoints_to_test)}")

if successful_endpoints:
    print(f"\n🎯 Endpoints à utiliser:")
    for ep in successful_endpoints:
        print(f"\n   ✓ {ep['name']}")
        print(f"     URL: {ep['url']}")
else:
    print(f"\n❌ Aucun endpoint fonctionnel trouvé")
    print(f"\n💡 Recommandations:")
    print(f"   1. Vérifier que SAFECUBE_API_KEY est correcte dans .env")
    print(f"   2. Consulter la documentation Sinay: https://api.sinay.ai/docs")
    print(f"   3. Contacter le support Sinay pour l'endpoint historique")

print(f"\n{'=' * 80}")
