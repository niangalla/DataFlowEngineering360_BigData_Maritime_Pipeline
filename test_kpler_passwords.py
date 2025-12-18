from kpler.sdk.configuration import Configuration
from kpler.sdk.resources.port_calls import PortCalls
from kpler.sdk import Platform
from datetime import datetime, timedelta
import os

email = os.getenv('KPLER_EMAIL', 'alla.niang@univ-thies.sn')
passwords_to_try = ['Neymarjr10', 'Passer123']

print("="*80)
print("🔬 TEST KPLER SDK - MULTIPLES MOTS DE PASSE")
print("="*80)
print(f"📧 Email: {email}")
print(f"🔑 Passwords à tester: {passwords_to_try}")
print("="*80)

for password in passwords_to_try:
    print(f"\n{'='*80}")
    print(f"🔐 TENTATIVE AVEC PASSWORD: {password}")
    print(f"{'='*80}")
    
    try:
        config = Configuration(
            email=email,
            password=password,
            platform=Platform.Liquids
        )
        
        print("✅ Configuration créée!")
        
        print("📋 Création du client PortCalls...")
        port_calls_client = PortCalls(config)
        
        print("📊 Test de connexion - récupération des colonnes...")
        columns = port_calls_client.get_columns()
        
        print(f"✅✅✅ AUTHENTIFICATION RÉUSSIE avec '{password}'!")
        print(f"      {len(columns)} colonnes disponibles")
        
        # Si on arrive ici, on a trouvé le bon password, testons les données
        print("\n" + "="*80)
        print("🚢 RÉCUPÉRATION DES DONNÉES HISTORIQUES")
        print("="*80)
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        
        print(f"📅 Période: {start_date.strftime('%Y-%m-%d')} → {end_date.strftime('%Y-%m-%d')}")
        
        # Essayer plusieurs variantes de nom
        for port_name in ['Dakar', 'SNDKR']:
            print(f"\n🔍 Recherche pour port: '{port_name}'...")
            try:
                df = port_calls_client.get(
                    start_date=start_date.strftime('%Y-%m-%d'),
                    end_date=end_date.strftime('%Y-%m-%d'),
                    to_port=[port_name],
                    columns=['date', 'vessel_name', 'to_port', 'from_port', 'product']
                )
                
                if not df.empty:
                    print(f"✅ {len(df)} escales trouvées avec '{port_name}'!")
                    print("\n📋 Aperçu des 10 premières:")
                    print(df.head(10).to_string())
                    print(f"\n📅 Période: {df['date'].min()} → {df['date'].max()}")
                    break
                else:
                    print(f"   Aucune donnée avec '{port_name}'")
            except Exception as e:
                print(f"   Erreur avec '{port_name}': {e}")
        
        # Essayer par pays
        if df.empty:
            print(f"\n🔍 Recherche par pays: 'Senegal'...")
            try:
                df = port_calls_client.get(
                    start_date=start_date.strftime('%Y-%m-%d'),
                    end_date=end_date.strftime('%Y-%m-%d'),
                    to_country=['Senegal'],
                    columns=['date', 'vessel_name', 'to_port', 'to_country', 'product']
                )
                
                if not df.empty:
                    print(f"✅ {len(df)} escales trouvées pour Senegal!")
                    print("\n📋 Aperçu des 10 premières:")
                    print(df.head(10).to_string())
                    print(f"\n📅 Période: {df['date'].min()} → {df['date'].max()}")
            except Exception as e:
                print(f"   Erreur: {e}")
        
        print("\n" + "="*80)
        print("🎯 RÉSULTAT FINAL")
        print("="*80)
        print(f"✅ PASSWORD VALIDE: {password}")
        print(f"✅ KPLER SDK FONCTIONNE!")
        if not df.empty:
            print(f"✅ DONNÉES HISTORIQUES DISPONIBLES!")
            print(f"   → Kpler peut être utilisé pour l'Option B")
        else:
            print(f"⚠️  Pas de données pour cette région/période")
            print(f"   → Vérifier le référentiel Kpler ou l'accès API")
        
        # On arrête dès qu'on trouve un password qui fonctionne
        break
        
    except Exception as e:
        print(f"❌ ÉCHEC avec '{password}'")
        print(f"   Erreur: {str(e)[:200]}")
        continue

print("\n" + "="*80)
