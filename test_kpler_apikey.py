from kpler.sdk.configuration import Configuration
from kpler.sdk.resources.port_calls import PortCalls
from kpler.sdk import Platform
from datetime import datetime, timedelta
import os
import base64

email = os.getenv('KPLER_EMAIL')
api_key = os.getenv('KPLER_API_KEY')

print("="*80)
print("🔬 TEST KPLER SDK - PORT CALLS HISTORIQUES (API KEY)")
print("="*80)
print(f"📧 Email: {email}")
print(f"🔑 API Key: {api_key[:20]}... ({len(api_key)} chars)" if api_key else "NOT SET")
print("="*80)

if not email or not api_key:
    print("❌ ERROR: KPLER_EMAIL or KPLER_API_KEY not set.")
    exit(1)

try:
    # Décoder l'API key (semble être en base64)
    # Format typique: username:password encodé en base64
    print("\n🔓 Décodage de l'API Key...")
    decoded = base64.b64decode(api_key).decode('utf-8')
    
    if ':' in decoded:
        parts = decoded.split(':', 1)
        print(f"   Format détecté: username:password")
        username = parts[0]
        password = parts[1]
        print(f"   Username: {username}")
    else:
        print("   Format non reconnu, tentative avec API key directe")
        password = api_key
    
    # Configuration avec Platform Oil (pour maritime)
    print("\n🔐 Authentification...")
    config = Configuration(
        email=email,
        password=password,
        platform=Platform.Oil  # Pour les données maritimes
    )
    
    print("✅ Connexion établie!")
    
    # Test PortCalls pour récupérer l'historique
    print("\n📋 Création du client PortCalls...")
    port_calls_client = PortCalls(config)
    
    # Récupérer colonnes disponibles
    print("\n📊 Récupération des colonnes disponibles...")
    columns = port_calls_client.get_columns()
    print(f"Colonnes disponibles: {len(columns)} colonnes")
    print(columns[['id', 'name', 'type']].head(15))
    
    # Tenter de récupérer les port calls pour Dakar (derniers 30 jours)
    print("\n🚢 Récupération des Port Calls pour les 30 derniers jours...")
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    print(f"   Période: {start_date.strftime('%Y-%m-%d')} → {end_date.strftime('%Y-%m-%d')}")
    
    # Recherche par zone géographique (Dakar, Sénégal)
    df = port_calls_client.get(
        start_date=start_date.strftime('%Y-%m-%d'),
        end_date=end_date.strftime('%Y-%m-%d'),
        to_port=['Dakar'],  # Port de destination
        columns=['date', 'vessel_name', 'to_port', 'from_port', 'product']
    )
    
    print(f"\n📦 Résultats trouvés: {len(df)} escales")
    
    if not df.empty:
        print("\n✅ SUCCESS: Données historiques récupérées!")
        print("\n📋 Aperçu des données:")
        print(df.head(10))
        print(f"\n📅 Période couverte: {df['date'].min()} à {df['date'].max()}")
        
        print("\n💡 KPLER FONCTIONNE POUR L'HISTORIQUE!")
        print("   → Cette API peut être une alternative à Sinay!")
    else:
        print("\n⚠️  WARNING: Aucune donnée trouvée pour Dakar")
        print("   Cela peut indiquer:")
        print("   - Pas d'escales dans cette période")
        print("   - Nom du port incorrect dans Kpler (essayer 'SNDKR' ou autre)")
        print("   - Restrictions d'accès aux données")

except Exception as e:
    print(f"\n💥 EXCEPTION: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "="*80)
