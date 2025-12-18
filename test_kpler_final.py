from kpler.sdk.configuration import Configuration
from kpler.sdk.resources.port_calls import PortCalls
from kpler.sdk import Platform
from datetime import datetime, timedelta
import os

email = 'alla.niang@univ-thies.sn'
password = 'Passer123'  # Le premier qui a fonctionné

print("="*80)
print("🔬 TEST KPLER SDK - PORT CALLS HISTORIQUES (CORRIGÉ)")
print("="*80)
print(f"📧 Email: {email}")
print(f"🔑 Password: {password}")
print("="*80)

try:
    config = Configuration(
        email=email,
        password=password,
        platform=Platform.Liquids
    )
    
    print("\n✅ Authentification réussie!")
    
    port_calls_client = PortCalls(config)
    
    # Récupérer les 30 derniers jours
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    print(f"\n📅 Période: {start_date.strftime('%Y-%m-%d')} → {end_date.strftime('%Y-%m-%d')}")
    
    # Essayer différentes zones
    for zone_name in ['Dakar', 'Senegal', 'West Africa']:
        print(f"\n" + "="*80)
        print(f"🔍 RECHERCHE PAR ZONE: '{zone_name}'")
        print("="*80)
        
        try:
            df = port_calls_client.get(
                start_date=start_date,
                end_date=end_date,
                zones=[zone_name],
                columns=['vessel_name', 'installation_name', 'eta', 'start', 'end', 'flow_quantity_cubic_meters']
            )
            
            print(f"📦 Résultats: {len(df)} escales")
            
            if not df.empty:
                print(f"✅✅✅ DONNÉES HISTORIQUES TROUVÉES POUR '{zone_name}'!")
                print("\n📋 Aperçu des 15 premières escales:")
                print(df.head(15).to_string())
                
                if 'start' in df.columns:
                    df_with_dates = df[df['start'].notna()]
                    if not df_with_dates.empty:
                        print(f"\n📅 Période réelle des données:")
                        print(f"   Première escale: {df_with_dates['start'].min()}")
                        print(f"   Dernière escale: {df_with_dates['start'].max()}")
                
                print("\n" + "="*80)
                print("🎯 CONCLUSION FINALE")
                print("="*80)
                print("✅ KPLER SDK FONCTIONNE PARFAITEMENT!")
                print("✅ DONNÉES HISTORIQUES DISPONIBLES!")
                print("✅ ALTERNATIVE VIABLE À SINAY POUR L'OPTION B!")
                print("\n💡 Pour utiliser Kpler dans votre pipeline:")
                print("   1. Mettez à jour .env avec KPLER_PASSWORD=Passer123")
                print("   2. Créez un nouveau fetcher fetch_kpler_port.py")
                print("   3. L'intégrez dans port_historical_collection_dag.py")
                break
            else:
                print(f"   Aucune donnée pour '{zone_name}'")
                
        except Exception as e:
            print(f"❌ Erreur avec '{zone_name}': {str(e)[:200]}")
            continue
    else:
        print("\n⚠️  Aucune des zones testées n'a retourné de données")
        print("   Essayez d'autres noms de zones ou consultez le référentiel Kpler")

except Exception as e:
    print(f"\n💥 EXCEPTION GÉNÉRALE: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "="*80)
