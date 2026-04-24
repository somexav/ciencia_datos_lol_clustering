# collector.py
import requests
import pandas as pd
import time
import json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Set, Tuple, Optional
import sqlite3
import os
from collections import deque, defaultdict
import hashlib
import sys

# ============================================================================
# CONFIGURACIÓN
# ============================================================================

with open('config.json', 'r') as f:
    config = json.load(f)

API_KEY = config["API_KEY"]
REGION_CONTINENTAL = config["REGION_CONTINENTAL"]
START_RIOT_IDS = config["START_RIOT_IDS"]

# METAS DE RECOLECCIÓN
TARGET_TOTAL_MATCHES = 1000
MATCHES_PER_PLAYER = 5
MAX_PLAYERS_PROCESSED = 20000
MAX_CONCURRENT_THREADS = 3

# TIMELINE CONFIG
INCLUDE_TIMELINE = True
TIMELINE_ONLY_AFTER_MIN = 10

# ALMACENAMIENTO
OUTPUT_DIR = "data"
JSON_DIR = os.path.join(OUTPUT_DIR, "matches")  # Solo JSONs, separado del cache
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")        # Cache con hash

# ============================================================================
# CACHE INTELIGENTE
# ============================================================================

class IntelligentCache:
    """Cache en memoria y disco para reducir API calls (solo temporal)"""
    
    def __init__(self, cache_dir: str):
        self.cache_dir = cache_dir
        self.memory_cache = {}
        os.makedirs(cache_dir, exist_ok=True)
    
    def _get_cache_path(self, key: str) -> str:
        key_hash = hashlib.md5(key.encode()).hexdigest()
        return os.path.join(self.cache_dir, f"{key_hash}.json")
    
    def get(self, key: str):
        if key in self.memory_cache:
            return self.memory_cache[key]
        
        cache_file = self._get_cache_path(key)
        if os.path.exists(cache_file):
            try:
                with open(cache_file, 'r') as f:
                    data = json.load(f)
                    self.memory_cache[key] = data
                    return data
            except:
                return None
        return None
    
    def set(self, key: str, data):
        self.memory_cache[key] = data
        cache_file = self._get_cache_path(key)
        try:
            with open(cache_file, 'w') as f:
                json.dump(data, f, indent=2)
        except:
            pass

# ============================================================================
# API WRAPPER
# ============================================================================

class RiotAPIWrapper:
    """Wrapper optimizado para API calls con rate limiting"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.last_request_time = 0
        self.min_request_interval = 1.2
        self.requests_count = 0
        self.rate_limit_reset = time.time() + 120
        
    def make_request(self, url: str, params: Dict = None, max_retries: int = 3):
        self._wait_if_needed()
        
        headers = {"X-Riot-Token": self.api_key}
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=headers, params=params, timeout=15)
                self.requests_count += 1
                self.last_request_time = time.time()
                
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 120))
                    print(f"⏳ Rate limit excedido, esperando {retry_after}s...")
                    time.sleep(retry_after)
                    self.rate_limit_reset = time.time() + retry_after
                    self.requests_count = 0
                    continue
                elif response.status_code == 403:
                    print("❌ API Key expirada o inválida")
                    return None
                else:
                    print(f"⚠️ Error {response.status_code} en {url}")
                    time.sleep(2)
                    continue
                    
            except requests.exceptions.Timeout:
                print(f"⏰ Timeout en intento {attempt + 1}")
                time.sleep(3)
                continue
            except Exception as e:
                print(f"🌐 Error de conexión: {e}")
                time.sleep(5)
                continue
        
        return None
    
    def _wait_if_needed(self):
        current_time = time.time()
        
        if current_time > self.rate_limit_reset:
            self.requests_count = 0
            self.rate_limit_reset = current_time + 120
        
        if self.requests_count >= 95:
            wait_time = self.rate_limit_reset - current_time + 5
            if wait_time > 0:
                print(f"⏳ Cerca del rate limit, esperando {wait_time:.1f}s...")
                time.sleep(wait_time)
                self.requests_count = 0
                self.rate_limit_reset = time.time() + 120
        
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.min_request_interval:
            time.sleep(self.min_request_interval - time_since_last)

# ============================================================================
# GUARDAR JSONs
# ============================================================================

def save_match_json(match_id: str, match_data: Dict, timeline_data: Dict = None):
    """Guarda JSONs con MATCH_ID REAL (no hash)"""
    os.makedirs(JSON_DIR, exist_ok=True)
    
    match_file = os.path.join(JSON_DIR, f"{match_id}.json")
    with open(match_file, 'w') as f:
        json.dump(match_data, f, indent=2)
    
    if timeline_data:
        timeline_file = os.path.join(JSON_DIR, f"{match_id}_timeline.json")
        with open(timeline_file, 'w') as f:
            json.dump(timeline_data, f, indent=2)

# ============================================================================
# SNOWBALL COLLECTOR (SOLO RECOLECTA JSONs)
# ============================================================================

class SnowballCollector:
    """SOLO recolecta JSONs, NO procesa wide, NO tiene base de datos"""
    
    def __init__(self, api_key: str):
        self.api = RiotAPIWrapper(api_key)
        self.cache = IntelligentCache(CACHE_DIR)
        
        # Estado del snowball
        self.puuid_queue = deque()
        self.processed_puuids = set()
        self.collected_match_ids = set()
        self.match_data_cache = {}
        
        # Estadísticas
        self.stats = {
            "start_time": time.time(),
            "total_matches": 0,
            "total_players": 0,
            "api_calls": 0,
            "cache_hits": 0,
            "json_saved": 0,
        }
    
    def get_puuid_from_riot_id(self, game_name: str, tag_line: str) -> str:
        cache_key = f"puuid_{game_name}_{tag_line}"
        cached = self.cache.get(cache_key)
        if cached:
            self.stats["cache_hits"] += 1
            return cached
        
        url = f"https://{REGION_CONTINENTAL}.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{game_name}/{tag_line}"
        data = self.api.make_request(url)
        if data and "puuid" in data:
            puuid = data["puuid"]
            self.cache.set(cache_key, puuid)
            return puuid
        return None
    
    def get_match_ids(self, puuid: str, count: int = 20, queue: int = 420) -> List[str]:
        cache_key = f"match_ids_{puuid}_{count}_{queue}"
        cached = self.cache.get(cache_key)
        if cached:
            self.stats["cache_hits"] += 1
            return cached
        
        url = f"https://{REGION_CONTINENTAL}.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids"
        params = {
            "start": 0,
            "count": count,
            "queue": queue,
            "startTime": int((datetime.now() - timedelta(days=30)).timestamp())
        }
        match_ids = self.api.make_request(url, params)
        if match_ids:
            self.cache.set(cache_key, match_ids)
        return match_ids or []
    
    def get_match_data(self, match_id: str) -> Dict:
        if match_id in self.match_data_cache:
            self.stats["cache_hits"] += 1
            return self.match_data_cache[match_id]
        
        cache_key = f"match_{match_id}"
        cached = self.cache.get(cache_key)
        if cached:
            self.stats["cache_hits"] += 1
            self.match_data_cache[match_id] = cached
            return cached
        
        url = f"https://{REGION_CONTINENTAL}.api.riotgames.com/lol/match/v5/matches/{match_id}"
        match_data = self.api.make_request(url)
        if match_data:
            self.cache.set(cache_key, match_data)
            self.match_data_cache[match_id] = match_data
        return match_data
    
    def get_timeline_data(self, match_id: str) -> Dict:
        cache_key = f"timeline_{match_id}"
        cached = self.cache.get(cache_key)
        if cached:
            self.stats["cache_hits"] += 1
            return cached
        
        url = f"https://{REGION_CONTINENTAL}.api.riotgames.com/lol/match/v5/matches/{match_id}/timeline"
        timeline_data = self.api.make_request(url)
        if timeline_data:
            self.cache.set(cache_key, timeline_data)
        return timeline_data
    
    def initialize_from_riot_ids(self, riot_ids: List[str]) -> bool:
        print(f"🎯 Inicializando con {len(riot_ids)} Riot IDs...")
        for riot_id in riot_ids:
            if "#" in riot_id:
                game_name, tag_line = riot_id.split("#")
            else:
                game_name = riot_id
                tag_line = "LAT"
            
            print(f"   🔍 Obteniendo {game_name}#{tag_line}...")
            puuid = self.get_puuid_from_riot_id(game_name, tag_line)
            if puuid:
                if puuid not in self.processed_puuids:
                    self.puuid_queue.append(puuid)
                    self.stats["total_players"] += 1
                    print(f"      ✅ PUUID obtenido")
                else:
                    print(f"      ⚠️ Ya procesado")
            else:
                print(f"      ❌ No se pudo obtener")
        return len(self.puuid_queue) > 0
    
    def _process_single_player(self, puuid: str) -> Tuple[int, int]:
        """SOLO guarda JSONs, nada de wide, nada de DB"""
        new_match_count = 0
        new_player_count = 0
        
        match_ids = self.get_match_ids(puuid, count=MATCHES_PER_PLAYER)
        
        for match_id in match_ids:
            if match_id in self.collected_match_ids:
                continue
            
            match_data = self.get_match_data(match_id)
            if not match_data:
                continue
            
            info = match_data.get("info", {})
            participants = info.get("participants", [])
            
            if not participants:
                continue
            
            game_duration = info.get("gameDuration", 0)
            if game_duration < 900:
                continue
            
            timeline_data = None
            if INCLUDE_TIMELINE and game_duration >= TIMELINE_ONLY_AFTER_MIN * 60:
                timeline_data = self.get_timeline_data(match_id)
            
            # ✅ SOLO GUARDAR JSON, NADA MÁS
            save_match_json(match_id, match_data, timeline_data)
            self.stats["json_saved"] += 1
            
            # Extraer nuevos jugadores
            for participant in participants:
                participant_puuid = participant.get("puuid", "")
                if (participant_puuid and 
                    participant_puuid not in self.processed_puuids and
                    participant_puuid not in self.puuid_queue):
                    self.puuid_queue.append(participant_puuid)
                    new_player_count += 1
            
            self.collected_match_ids.add(match_id)
            new_match_count += 1
            self.stats["total_matches"] = len(self.collected_match_ids)
        
        return new_match_count, new_player_count
    
    def process_player_batch(self, batch_size: int = 5) -> Tuple[int, int]:
        if not self.puuid_queue:
            return 0, 0
        
        batch = []
        for _ in range(min(batch_size, len(self.puuid_queue))):
            batch.append(self.puuid_queue.popleft())
        
        print(f"📦 Procesando batch de {len(batch)} jugadores...")
        total_new_matches = 0
        total_new_players = 0
        
        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_THREADS) as executor:
            future_to_puuid = {
                executor.submit(self._process_single_player, puuid): puuid
                for puuid in batch
            }
            for future in as_completed(future_to_puuid):
                puuid = future_to_puuid[future]
                try:
                    new_matches, new_players = future.result(timeout=45)
                    total_new_matches += new_matches
                    total_new_players += new_players
                    print(f"   ✅ {puuid[:12]}: +{new_matches} partidas, +{new_players} jugadores")
                except Exception as e:
                    print(f"   ❌ Error procesando {puuid[:12]}: {e}")
        
        self.processed_puuids.update(batch)
        return total_new_matches, total_new_players
    
    def run(self, target_matches: int = TARGET_TOTAL_MATCHES):
        print("=" * 70)
        print("❄️  SNOWBALL COLLECTOR - SOLO GUARDAR JSONs")
        print("=" * 70)
        
        print("\n1️⃣ INICIALIZANDO...")
        if not self.initialize_from_riot_ids(START_RIOT_IDS):
            print("❌ No se pudo inicializar con Riot IDs")
            return
        
        print(f"   PUUIDs iniciales en cola: {len(self.puuid_queue)}")
        print(f"\n2️⃣ EJECUTANDO SNOWBALL SAMPLING...")
        print(f"   Objetivo: {target_matches} partidas únicas")
        print(f"   📁 JSONs guardados en: {JSON_DIR}")
        
        iteration = 0
        last_progress_time = time.time()
        
        while (self.puuid_queue and 
               len(self.collected_match_ids) < target_matches and
               len(self.processed_puuids) < MAX_PLAYERS_PROCESSED):
            
            iteration += 1
            current_time = time.time()
            
            if current_time - last_progress_time > 10:
                elapsed = time.time() - self.stats["start_time"]
                matches_per_min = self.stats["total_matches"] / (elapsed / 60) if elapsed > 0 else 0
                print(f"\n📊 Iteración {iteration} | {time.strftime('%H:%M:%S')}")
                print(f"   Partidas: {self.stats['total_matches']:,} / {target_matches:,}")
                print(f"   JSONs guardados: {self.stats['json_saved']:,}")
                print(f"   Jugadores: {len(self.processed_puuids):,} procesados")
                print(f"   En cola: {len(self.puuid_queue):,} por procesar")
                print(f"   Velocidad: {matches_per_min:.1f} partidas/min")
                last_progress_time = current_time
            
            new_matches, new_players = self.process_player_batch(batch_size=5)
            time.sleep(1)
            
            if len(self.collected_match_ids) >= target_matches:
                print(f"\n🎯 META ALCANZADA: {len(self.collected_match_ids)} partidas!")
                break
        
        # Reporte final
        elapsed = time.time() - self.stats["start_time"]
        hours = int(elapsed // 3600)
        minutes = int((elapsed % 3600) // 60)
        seconds = int(elapsed % 60)
        
        print("\n" + "=" * 70)
        print("📊 REPORTE FINAL - COLECTOR")
        print("=" * 70)
        print(f"⏱️  Tiempo total: {hours:02d}:{minutes:02d}:{seconds:02d}")
        print(f"📦 Partidas recolectadas: {len(self.collected_match_ids):,}")
        print(f"👥 Jugadores procesados: {len(self.processed_puuids):,}")
        print(f"📄 JSONs guardados: {self.stats['json_saved']:,}")
        print(f"💾 Cache hits: {self.stats['cache_hits']:,}")
        print(f"📁 Directorio JSON: {JSON_DIR}")
        print("=" * 70)

# ============================================================================
# MAIN
# ============================================================================

def main():
    print("🚀 COLECTOR DE DATOS LoL - SOLO GUARDAR JSONs")
    print("=" * 60)
    
    # Crear directorios
    os.makedirs(JSON_DIR, exist_ok=True)
    os.makedirs(CACHE_DIR, exist_ok=True)
    
    # Ejecutar colector
    collector = SnowballCollector(API_KEY)
    collector.run(TARGET_TOTAL_MATCHES)

if __name__ == "__main__":
    main()