"""
Scraper simple et rapide pour récupérer les données du port de Dakar depuis VesselFinder
Objectif: 7-14 jours de données historiques au format compatible Sinay API
"""
import requests
from bs4 import BeautifulSoup
import json
import time
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import re
from pathlib import Path

class VesselFinderScraper:
    """Scraper éthique pour VesselFinder - Port de Dakar"""
    
    def __init__(self, rate_limit=3.0):
        self.base_url = "https://www.vesselfinder.com"
        self.rate_limit = rate_limit  # 3 secondes entre requêtes
        self.last_request = 0
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Educational Research - Univ Thies Senegal)',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        })
    
    def _respect_rate_limit(self):
        """Rate limiting éthique"""
        elapsed = time.time() - self.last_request
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)
        self.last_request = time.time()
    
    def _safe_get(self, url: str, max_retries=3) -> Optional[requests.Response]:
        """GET request avec retry et error handling"""
        for attempt in range(max_retries):
            try:
                self._respect_rate_limit()
                response = self.session.get(url, timeout=10)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                print(f"Attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Backoff exponentiel
                else:
                    return None
        return None
    
    def scrape_dakar_port(self) -> Dict:
        """
        Scraper principal pour le port de Dakar
        
        Returns:
            Dict au format compatible Sinay API
        """
        print("Début du scraping VesselFinder - Port de Dakar")
        print("="*60)
        
        # URL du port de Dakar
        port_url = f"{self.base_url}/fr/ports/SNDKR"
        
        response = self._safe_get(port_url)
        if not response:
            print("Impossible d'accéder à VesselFinder")
            return self._empty_result()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Extraire les navires actuellement au port
        vessels_at_port = self._extract_current_vessels(soup)
        
        # Extraire les arrivées récentes (si disponible)
        recent_arrivals = self._extract_recent_arrivals(soup)
        
        # Extraire les départs récents (si disponible)
        recent_departures = self._extract_recent_departures(soup)
        
        # Formater au format Sinay
        result = {
            "metadata": {
                "port": "SNDKR",
                "port_name": "Dakar",
                "country": "Senegal",
                "scraped_at": datetime.utcnow().isoformat() + "Z",
                "source": "vesselfinder",
                "total_vessels": len(vessels_at_port),
                "total_arrivals": len(recent_arrivals),
                "total_departures": len(recent_departures)
            },
            "data": {
                "current_vessels": vessels_at_port,
                "arrivals": recent_arrivals,
                "departures": recent_departures
            }
        }
        
        print(f"\nScraping terminé!")
        print(f"   Navires actuels: {len(vessels_at_port)}")
        print(f"   Arrivées: {len(recent_arrivals)}")
        print(f"   Départs: {len(recent_departures)}")
        
        return result
    
    def _extract_current_vessels(self, soup: BeautifulSoup) -> List[Dict]:
        """Extraire les navires actuellement au port"""
        vessels = []
        
        # Chercher la table des navires
        # Structure typique: <table class="vessels"> ou similaire
        tables = soup.find_all('table')
        
        for table in tables:
            rows = table.find_all('tr')[1:]  # Skip header
            
            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= 4:  # Minimum: nom, type, flag, etc.
                    try:
                        vessel_data = self._parse_vessel_row(cells, row)
                        if vessel_data:
                            vessels.append(vessel_data)
                    except Exception as e:
                        print(f"Warning: Could not parse row: {e}")
                        continue
        
        return vessels
    
    def _parse_vessel_row(self, cells, row) -> Optional[Dict]:
        """Parser une ligne de navire"""
        try:
            # Extraire lien vers la page du navire
            link = row.find('a', href=re.compile(r'/vessels/'))
            if not link:
                return None
            
            vessel_name = link.text.strip()
            vessel_url = self.base_url + link['href']
            
            # Extraire IMO/MMSI depuis l'URL ou la page
            mmsi_match = re.search(r'/([0-9]{9})$', link['href'])
            mmsi = int(mmsi_match.group(1)) if mmsi_match else None
            
            # Essayer d'extraire d'autres infos basiques
            vessel_type = cells[1].text.strip() if len(cells) > 1 else "Unknown"
            flag = cells[2].text.strip() if len(cells) > 2 else "Unknown"
            
            vessel_data = {
                "vessel_name": vessel_name,
                "vessel_mmsi": mmsi,
                "vessel_type": vessel_type,
                "flag": flag,
                "source_url": vessel_url,
                "detected_at": datetime.utcnow().isoformat() + "Z"
            }
            
            return vessel_data
            
        except Exception as e:
            return None
    
    def _extract_recent_arrivals(self, soup: BeautifulSoup) -> List[Dict]:
        """Extraire les arrivées récentes"""
        # Cette section dépend de la structure HTML de VesselFinder
        # Souvent sous forme de section "Recent Arrivals" ou dans l'API cachée
        return []  # À implémenter selon la structure HTML réelle
    
    def _extract_recent_departures(self, soup: BeautifulSoup) -> List[Dict]:
        """Extraire les départs récents"""
        return []  # À implémenter selon la structure HTML réelle
    
    def _empty_result(self) -> Dict:
        """Résultat vide en cas d'erreur"""
        return {
            "metadata": {
                "port": "SNDKR",
                "port_name": "Dakar",
                "country": "Senegal",
                "scraped_at": datetime.utcnow().isoformat() + "Z",
                "source": "vesselfinder",
                "error": "Scraping failed",
                "total_vessels": 0,
                "total_arrivals": 0,
                "total_departures": 0
            },
            "data": {
                "current_vessels": [],
                "arrivals": [],
                "departures": []
            }
        }
    
    def save_to_file(self, data: Dict, output_path: str):
        """Sauvegarder les données au format JSONL (JSON Lines)"""
        with open(output_path, 'w', encoding='utf-8') as f:
            # Première ligne : métadonnées
            f.write(json.dumps({"metadata": data["metadata"]}, ensure_ascii=False) + '\n')
            
            # Lignes suivantes : un navire par ligne
            for vessel in data["data"]["current_vessels"]:
                f.write(json.dumps(vessel, ensure_ascii=False) + '\n')
            
            # Optionnel : arrivées et départs si disponibles
            for arrival in data["data"].get("arrivals", []):
                arrival["event_type"] = "arrival"
                f.write(json.dumps(arrival, ensure_ascii=False) + '\n')
            
            for departure in data["data"].get("departures", []):
                departure["event_type"] = "departure"
                f.write(json.dumps(departure, ensure_ascii=False) + '\n')
        
        print(f"Données sauvegardées dans: {output_path}")
        print(f"   Format: JSONL (JSON Lines)")
        print(f"   Lignes: 1 metadata + {len(data['data']['current_vessels'])} vessels")


def main():
    """Fonction principale"""
    scraper = VesselFinderScraper(rate_limit=3.0)
    
    # Scraper le port de Dakar
    data = scraper.scrape_dakar_port()
    
    # Construire le chemin absolu vers data/source/scraped
    # Gérer la différence entre Host (src/data) et Container (/opt/airflow/data/)
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent.parent.parent  # Remonter à la racine du projet
    
    if (project_root / "src").exists():
        # Host environment
        output_dir = project_root / "src" / "data" / "source" / "scraped"
    elif Path("/opt/airflow").exists():
        # Airflow Container environment
        output_dir = Path("/opt/airflow/data/source/scraped")
    else:
        # Fallback Container environment
        output_dir = project_root / "data" / "source" / "scraped"
        
    output_dir.mkdir(parents=True, exist_ok=True)  # Créer le répertoire si nécessaire
    
    # Fix permissions for directory
    try:
        os.chmod(output_dir, 0o777)
    except Exception:
        pass
    
    output_file = output_dir / "dakar_port_calls.jsonl"
    scraper.save_to_file(data, str(output_file))
    
    # Fix permissions for file
    try:
        os.chmod(output_file, 0o666)
    except Exception:
        pass
    
    print("\n" + "="*60)
    print("Scraping terminé avec succès!")
    print(f"Fichier: {output_file}")


if __name__ == "__main__":
    main()
