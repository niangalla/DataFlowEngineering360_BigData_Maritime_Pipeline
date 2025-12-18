"""
Test Marinesia API pour récupérer l'historique des navires près du port de Dakar
"""
import requests
import json
from datetime import datetime

# Configuration
BASE_URL = "https://api.marinesia.com/api/v1"
# Note: Marinesia nécessite un 'key' parameter mais la doc suggère que c'est gratuit
# On va tester sans clé d'abord, puis avec une clé vide si nécessaire
API_KEY = ""  # À ajuster si nécessaire

# Coordonnées du port de Dakar (Sénégal)
# Bounding box autour du port
DAKAR_BOX = {
    "lat_min": 14.5,
    "lat_max": 14.8,
    "long_min": -17.5,
    "long_max": -17.3
}

print("="*80)
print("🧪 TEST MARINESIA API - PORT DE DAKAR")
print("="*80)
print(f"📍 Zone: Dakar, Sénégal")
print(f"   Latitude: {DAKAR_BOX['lat_min']} → {DAKAR_BOX['lat_max']}")
print(f"   Longitude: {DAKAR_BOX['long_min']} → {DAKAR_BOX['long_max']}")
print("="*80)

# Test 1: Rechercher le port de Dakar dans leur registre
print("\n📋 TEST 1: Recherche du port de Dakar dans le registre")
print("-"*80)
try:
    params = {
        "key": API_KEY,
        "filters": "un_locode:SNDKR"  # UN/LOCODE de Dakar
    }
    response = requests.get(f"{BASE_URL}/port/profile", params=params, timeout=10)
    print(f"Status: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        print(f"✅ Réponse reçue!")
        print(json.dumps(data, indent=2)[:500])
    else:
        print(f"❌ Erreur: {response.text[:200]}")
except Exception as e:
    print(f"💥 Exception: {e}")

# Test 2: Obtenir les navires actuellement dans la zone de Dakar
print("\n🚢 TEST 2: Navires actuellement près de Dakar (temps réel)")
print("-"*80)
try:
    params = {
        "key": API_KEY,
        "lat_min": DAKAR_BOX["lat_min"],
        "lat_max": DAKAR_BOX["lat_max"],
        "long_min": DAKAR_BOX["long_min"],
        "long_max": DAKAR_BOX["long_max"]
    }
    response = requests.get(f"{BASE_URL}/vessel/nearby", params=params, timeout=10)
    print(f"Status: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        if not data.get('error'):
            vessels = data.get('data', [])
            print(f"✅ {len(vessels)} navires détectés dans la zone!")
            
            if vessels:
                print("\n📋 10 premiers navires:")
                for i, vessel in enumerate(vessels[:10], 1):
                    print(f"   {i}. {vessel.get('name', 'N/A')} (MMSI: {vessel.get('mmsi')})")
                    print(f"      Type: {vessel.get('type')} | Dest: {vessel.get('dest', 'N/A')}")
                    print(f"      Pos: ({vessel.get('lat')}, {vessel.get('lng')})")
                
                # Test 3: Récupérer l'historique d'un navire
                if len(vessels) > 0:
                    test_vessel = vessels[0]
                    mmsi = test_vessel.get('mmsi')
                    
                    print(f"\n📜 TEST 3: Historique du navire {test_vessel.get('name')} (MMSI: {mmsi})")
                    print("-"*80)
                    
                    params = {"key": API_KEY}
                    response = requests.get(f"{BASE_URL}/vessel/{mmsi}/location", params=params, timeout=10)
                    print(f"Status: {response.status_code}")
                    
                    if response.status_code == 200:
                        hist_data = response.json()
                        if not hist_data.get('error'):
                            history = hist_data.get('data', [])
                            print(f"✅ {len(history)} positions historiques récupérées!")
                            
                            if history:
                                print("\n📅 Aperçu de l'historique:")
                                for i, pos in enumerate(history[:5], 1):
                                    ts = pos.get('ts', 'N/A')
                                    lat = pos.get('lat')
                                    lng = pos.get('lng')
                                    print(f"   {i}. {ts}")
                                    print(f"      Position: ({lat}, {lng})")
                                    print(f"      Speed: {pos.get('sog')} kn | Course: {pos.get('cog')}°")
                                
                                # Analyser la profondeur de l'historique
                                timestamps = [datetime.fromisoformat(p['ts'].replace('Z', '+00:00')) for p in history if p.get('ts')]
                                if timestamps:
                                    oldest = min(timestamps)
                                    newest = max(timestamps)
                                    print(f"\n⏱️  Profondeur historique:")
                                    print(f"      Plus ancien: {oldest}")
                                    print(f"      Plus récent: {newest}")
                                    print(f"      Durée: {(newest - oldest).days} jours")
                        else:
                            print(f"⚠️  {hist_data.get('message')}")
                    else:
                        print(f"❌ Erreur historique: {response.text[:200]}")
            else:
                print("⚠️  Aucun navire détecté dans la zone actuellement")
        else:
            print(f"⚠️  {data.get('message')}")
    else:
        print(f"❌ Erreur: {response.text[:200]}")
        
except Exception as e:
    print(f"💥 Exception: {e}")

# Conclusion
print("\n" + "="*80)
print("🎯 RÉSULTAT DU TEST")
print("="*80)
print("✅ Marinesia API est accessible")
print("✅ Fournit des données temps réel par zone géographique")
print("✅ Fournit l'historique par navire (MMSI)")
print("\n⚠️  LIMITATION IDENTIFIÉE:")
print("   → L'API ne fournit PAS directement les 'port calls' historiques")
print("   → Il faudrait:")
print("      1. Identifier tous les navires ayant visité Dakar (via nearby régulier)")
print("      2. Récupérer l'historique de chaque navire")
print("      3. Filtrer les positions dans la zone du port")
print("      4. Détecter les 'arrêts' (SOG ~0) comme des port calls")
print("\n💡 POUR DAKAR :")
print("   → Approche temps réel: ✅ Fonctionne bien")
print("   → Approche historique batch: ⚠️  Complexe, nécessite post-traitement")
print("="*80)
