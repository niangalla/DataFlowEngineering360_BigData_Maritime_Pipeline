from kpler.sdk.configuration import Configuration
from kpler.sdk.resources.port_calls import PortCalls
from kpler.sdk import Platform
from datetime import datetime, timedelta
import os

email = os.getenv('KPLER_EMAIL')
api_key = os.getenv('KPLER_API_KEY')

print("="*80)
print("🔬 TEST KPLER SDK - PORT CALLS HISTORIQUES")
print("="*80)
print(f"📧 Email: {email}")
print(f"🔑 API Key: {api_key[:20] if api_key else 'NOT SET'}...")
print("="*80)

if not email or not api_key:
    print("❌ ERROR: KPLER_EMAIL or KPLER_API_KEY not set.")
    exit(1)

try:
    # Essayer avec l'API key directement comme password
    print("\n🔐 Authentification (API Key comme password)...")
    config = Configuration(
        email=email,
        password=api_key,  # Utiliser l'API key comme password
        platform=Platform.Liquids  # Pour les données maritimes liquides (pétrole, etc.)
    )
    
    print("✅ Configuration créée!")
    
    # Test PortCalls
    print("\n📋 Création du client PortCalls...")
    port_calls_client = PortCalls(config)
    
    # Récupérer colonnes disponibles (  cela va tester l'auth)
    print("\n📊 Test de connexion - récupération des colonnes...")
    columns = port_calls_client.get_columns()
    print(f"✅ Authentification réussie! {len(columns)} colonnes disponibles")
    print("\nPremières colonnes:")
    print(columns[['id', 'name', 'type']].head(15))
    
    # Tenter de récupérer les port calls pour Dakar
    print("\n" + "="*80)
    print("🚢 RÉCUPÉRATION DES DONNÉES HISTORIQUES")
    print("="*80)
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    print(f"📅 Période: {start_date.strftime('%Y-%m-%d')} → {end_date.strftime('%Y-%m-%d')}")
    print(f"🎯 Port cible: Dakar (Sénégal)")
    
    # Test 1: Par nom de port
    print("\n🔍 Tentative 1: Recherche par 'to_port=Dakar'...")
    try:
        df = port_calls_client.get(
            start_date=start_date.strftime('%Y-%m-%d'),
            end_date=end_date.strftime('%Y-%m-%d'),
            to_port=['Dakar'],
            columns=['date', 'vessel_name', 'to_port', 'from_port', 'product']
        )
        
        if not df.empty:
            print(f"✅ SUCCESS! {len(df)} escales trouvées!")
            print("\n📋 Aperçu:")
            print(df.head(10))
            print(f"\n📅 Dates: {df['date'].min()} → {df['date'].max()}")
        else:
            print("⚠️  Aucune donnée avec 'Dakar'")
            
            # Test 2: Par pays
            print("\n🔍 Tentative 2: Recherche par pays 'Senegal'...")
            df = port_calls_client.get(
                start_date=start_date.strftime('%Y-%m-%d'),
                end_date=end_date.strftime('%Y-%m-%d'),
                to_country=['Senegal'],
                columns=['date', 'vessel_name', 'to_port', 'to_country', 'product']
            )
            
            if not df.empty:
                print(f"✅ {len(df)} escales trouvées pour le Sénégal!")
                print("\n📋 Aperçu:")
                print(df.head(10))
                print(f"\n📅 Dates: {df['date'].min()} → {df['date'].max()}")
            else:
                print("⚠️  Aucune donnée pour Senegal non plus")
                print("\n💡 Suggestions:")
                print("   - Vérifier le nom exact dans le référentiel Kpler")
                print("   - Utiliser Installations.search('Dakar') pour trouver le bon nom")
                print("   - Votre accès Kpler pourrait être limité à certaines zones")
                
    except Exception as e:
        print(f"❌ Erreur lors de la requête: {e}")
        raise

    print("\n" + "="*80)
    print("🎯 CONCLUSION")
    print("="*80)
    if not df.empty:
        print("✅ KPLER PEUT RÉCUPÉRER DES DONNÉES HISTORIQUES!")
        print("   → Alternative viable à Sinay pour l'Option B")
    else:
        print("⚠️  Pas de données trouvées, mais l'API fonctionne")
        print("   → Nécessite investigation sur le référentiel Kpler")

except Exception as e:
    print(f"\n💥 EXCEPTION: {e}")
    print("\n📝 Détails de l'erreur:")
    import traceback
    traceback.print_exc()
    
    print("\n" + "="*80)
    print("❌ CONCLUSION:")
    print("   L'authentification ou la requête a échoué")
    print("   Kpler nécessite probablement un mot de passe valide")

print("\n" + "="*80)
