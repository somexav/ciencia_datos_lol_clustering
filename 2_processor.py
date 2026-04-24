# processor.py
import pandas as pd
import json
import sqlite3
import os
from typing import Dict, List, Tuple, Optional
from collections import defaultdict
from datetime import datetime
import numpy as np
import csv
# ============================================================================
# CONFIGURACIÓN
# ============================================================================
DATABASE_FILE_NAME = "lol_matches_wide_V2"
OUTPUT_DIR = "data"
CSV_FILE = os.path.join(OUTPUT_DIR, "lol_matches_wide.csv")

JSON_DIR = os.path.join(OUTPUT_DIR, "matches")
DB_WIDE_FILE = os.path.join(OUTPUT_DIR, f"output/{DATABASE_FILE_NAME}.db")

# SOLO SNAPSHOTS 5, 10, 15
SNAPSHOT_MINUTES = [5, 10, 15]

# Diccionario de clases de campeones
with open('data/input/champion_classes.json', 'r', encoding='utf-8') as f:
    CHAMPION_CLASSES = json.load(f)
# ============================================================================
# CARGAR JSONs
# ============================================================================

def load_match_json(match_id: str) -> Tuple[Optional[Dict], Optional[Dict]]:
    """Carga JSONs de partida desde disco"""
    match_file = os.path.join(JSON_DIR, f"{match_id}.json")
    timeline_file = os.path.join(JSON_DIR, f"{match_id}_timeline.json")
    
    match_data = None
    timeline_data = None
    
    if os.path.exists(match_file):
        with open(match_file, 'r', encoding='utf-8') as f:
            match_data = json.load(f)
    
    if os.path.exists(timeline_file):
        with open(timeline_file, 'r', encoding='utf-8') as f:
            timeline_data = json.load(f)
    
    return match_data, timeline_data

# ============================================================================
# EXTRACCIÓN DE SNAPSHOTS (CON WARDS)
# ============================================================================

def extract_snapshot_from_timeline(timeline_data: Dict, target_minute: int) -> Dict:
    """Extrae snapshot del timeline en un minuto específico"""
    if not timeline_data:
        return {}
    
    frames = timeline_data.get("info", {}).get("frames", [])
    target_timestamp = target_minute * 60 * 1000
    
    snapshot_frame = None
    for frame in frames:
        if frame.get("timestamp", 0) <= target_timestamp:
            snapshot_frame = frame
        else:
            break
    
    snapshot = {
        'blue_gold': 0, 'red_gold': 0,
        'blue_xp': 0, 'red_xp': 0,
        'blue_cs': 0, 'red_cs': 0,
        'blue_kills': 0, 'red_kills': 0,
        'blue_dragons': 0, 'red_dragons': 0,
        'blue_heralds': 0, 'red_heralds': 0,
        'blue_turrets': 0, 'red_turrets': 0,
        'blue_wards_placed': 0, 'red_wards_placed': 0,
        'blue_wards_killed': 0, 'red_wards_killed': 0,
    }
    
    if snapshot_frame:
        participant_frames = snapshot_frame.get("participantFrames", {})
        for participant_id, frame_data in participant_frames.items():
            pid = int(participant_id)
            gold = frame_data.get("totalGold", 0)
            xp = frame_data.get("xp", 0)
            minions = frame_data.get("minionsKilled", 0)
            jungle = frame_data.get("jungleMinionsKilled", 0)
            cs = minions + jungle
            
            if pid <= 5:
                snapshot['blue_gold'] += gold
                snapshot['blue_xp'] += xp
                snapshot['blue_cs'] += cs
            else:
                snapshot['red_gold'] += gold
                snapshot['red_xp'] += xp
                snapshot['red_cs'] += cs
    
    # Contar eventos hasta el snapshot
    for frame in frames:
        if frame.get("timestamp", 0) > target_timestamp:
            break
        
        for event in frame.get("events", []):
            event_type = event.get("type", "")
            killer_id = event.get("killerId", 0)
            creator_id = event.get("creatorId", 0)
            
            # Kills
            if event_type == "CHAMPION_KILL":
                if 1 <= killer_id <= 5:
                    snapshot['blue_kills'] += 1
                elif 6 <= killer_id <= 10:
                    snapshot['red_kills'] += 1
            
            # Dragones y Herald
            elif event_type == "ELITE_MONSTER_KILL":
                monster = event.get("monsterType", "")
                if monster == "DRAGON":
                    if 1 <= killer_id <= 5:
                        snapshot['blue_dragons'] += 1
                    elif 6 <= killer_id <= 10:
                        snapshot['red_dragons'] += 1
                elif monster == "RIFTHERALD":
                    if 1 <= killer_id <= 5:
                        snapshot['blue_heralds'] += 1
                    elif 6 <= killer_id <= 10:
                        snapshot['red_heralds'] += 1
            
            # Torres
            elif event_type == "BUILDING_KILL":
                if event.get("buildingType") == "TOWER_BUILDING":
                    if 1 <= killer_id <= 5:
                        snapshot['blue_turrets'] += 1
                    elif 6 <= killer_id <= 10:
                        snapshot['red_turrets'] += 1
            
            # WARDS COLOCADAS
            elif event_type == "WARD_PLACED":
                if 1 <= creator_id <= 5:
                    snapshot['blue_wards_placed'] += 1
                elif 6 <= creator_id <= 10:
                    snapshot['red_wards_placed'] += 1
            
            # WARDS DESTRUIDAS
            elif event_type == "WARD_KILL":
                if 1 <= killer_id <= 5:
                    snapshot['blue_wards_killed'] += 1
                elif 6 <= killer_id <= 10:
                    snapshot['red_wards_killed'] += 1
    
    return snapshot

# ============================================================================
# EXTRACCIÓN DE PRIMEROS OBJETIVOS
# ============================================================================

def extract_first_objectives(timeline_data: Dict) -> Dict:
    """Extrae primeros objetivos con equipo y tiempo"""
    result = {
        'first_blood_team': None,
        'first_blood_time': None,
        'first_tower_team': None,
        'first_tower_time': None,
        'first_dragon_team': None,
        'first_dragon_time': None,
        'first_herald_team': None,
        'first_herald_time': None,
    }
    
    if not timeline_data:
        return result
    
    frames = timeline_data.get("info", {}).get("frames", [])
    
    first_blood_found = False
    first_tower_found = False
    first_dragon_found = False
    first_herald_found = False
    
    for frame in frames:
        for event in frame.get("events", []):
            event_type = event.get("type", "")
            timestamp = event.get("timestamp", 0) / 1000
            killer_id = event.get("killerId", 0)
            
            if not first_blood_found and event_type == "CHAMPION_KILL":
                is_first_blood = False
                if event.get('bounty', 0) > 0:
                    is_first_blood = True
                else:
                    for dmg in event.get('victimDamageReceived', []):
                        if 'FIRST_BLOOD' in str(dmg):
                            is_first_blood = True
                            break
                
                if is_first_blood:
                    if 1 <= killer_id <= 5:
                        result['first_blood_team'] = 'blue'
                    elif 6 <= killer_id <= 10:
                        result['first_blood_team'] = 'red'
                    result['first_blood_time'] = timestamp
                    first_blood_found = True
            
            elif not first_tower_found and event_type == "BUILDING_KILL":
                if event.get("buildingType") == "TOWER_BUILDING":
                    if 1 <= killer_id <= 5:
                        result['first_tower_team'] = 'blue'
                    elif 6 <= killer_id <= 10:
                        result['first_tower_team'] = 'red'
                    result['first_tower_time'] = timestamp
                    first_tower_found = True
            
            elif not first_dragon_found and event_type == "ELITE_MONSTER_KILL":
                monster = event.get("monsterType", "")
                if monster == "DRAGON":
                    if 1 <= killer_id <= 5:
                        result['first_dragon_team'] = 'blue'
                    elif 6 <= killer_id <= 10:
                        result['first_dragon_team'] = 'red'
                    result['first_dragon_time'] = timestamp
                    first_dragon_found = True
            
            elif not first_herald_found and event_type == "ELITE_MONSTER_KILL":
                monster = event.get("monsterType", "")
                if monster == "RIFTHERALD":
                    if 1 <= killer_id <= 5:
                        result['first_herald_team'] = 'blue'
                    elif 6 <= killer_id <= 10:
                        result['first_herald_team'] = 'red'
                    result['first_herald_time'] = timestamp
                    first_herald_found = True
    
    return result

# ============================================================================
# PROCESAR A WIDE - VERSIÓN LIMPIA (CON WARDS)
# ============================================================================

def process_match_to_wide(match_id: str) -> Optional[Dict]:
    """Procesa partida a formato wide - CON WARDS PLACED/KILLED"""
    try:
        match_data, timeline_data = load_match_json(match_id)
        
        if not match_data or not timeline_data:
            return None
        
        info = match_data.get("info", {})
        participants = info.get("participants", [])
        
        # Separar equipos
        blue_team = [p for p in participants if p.get("teamId") == 100]
        red_team = [p for p in participants if p.get("teamId") == 200]
        
        # Ordenar por posición
        def sort_by_position(team):
            pos_order = {'TOP': 0, 'JUNGLE': 1, 'MIDDLE': 2, 'BOTTOM': 3, 'UTILITY': 4}
            return sorted(team, key=lambda x: pos_order.get(x.get('individualPosition', ''), 5))
        
        blue_team = sort_by_position(blue_team)
        red_team = sort_by_position(red_team)
        
        if len(blue_team) != 5 or len(red_team) != 5:
            return None
        
        # ====================================================================
        # METADATA - SOLO LO ESENCIAL
        # ====================================================================
        match_info = {
            'match_id': match_id,
            'game_duration': info.get("gameDuration", 0) / 60.0,
        }
        
        # ====================================================================
        # CHAMPIONS POR ROL (para composición)
        # ====================================================================
        roles = ['top', 'jungle', 'mid', 'adc', 'support']
        for i, role in enumerate(roles):
            match_info[f'blue_{role}_champion'] = blue_team[i].get('championName', '')
            match_info[f'red_{role}_champion'] = red_team[i].get('championName', '')
        
        # ====================================================================
        # COMPOSICIÓN DE EQUIPOS (VARIABLES NUMÉRICAS)
        # ====================================================================
        def count_classes(team):
            counts = defaultdict(int)
            for player in team:
                champ = player.get('championName', '')
                champ_class = CHAMPION_CLASSES.get(champ, 'Unknown')
                if champ_class in ['Tank', 'Fighter', 'Mage', 'Marksman', 'Controller']:
                    counts[champ_class] += 1
            return counts
        
        blue_classes = count_classes(blue_team)
        red_classes = count_classes(red_team)
        
        for cls in ['Tank', 'Fighter', 'Mage', 'Marksman', 'Controller']:
            match_info[f'blue_num_{cls.lower()}s'] = blue_classes.get(cls, 0)
            match_info[f'red_num_{cls.lower()}s'] = red_classes.get(cls, 0)
        
        # ====================================================================
        # SNAPSHOTS - SOLO PARA CALCULAR DIFERENCIAS
        # ====================================================================
        snapshots = {}
        for minute in SNAPSHOT_MINUTES:
            if info.get("gameDuration", 0) >= minute * 60:
                snapshots[minute] = extract_snapshot_from_timeline(timeline_data, minute)
        
        # ====================================================================
        # DIFERENCIAS (BLUE - RED) - FEATURES PRINCIPALES
        # ====================================================================
        for minute in SNAPSHOT_MINUTES:
            if minute in snapshots:
                snap = snapshots[minute]
                match_info[f'gold_diff_{minute}'] = snap.get('blue_gold', 0) - snap.get('red_gold', 0)
                match_info[f'xp_diff_{minute}'] = snap.get('blue_xp', 0) - snap.get('red_xp', 0)
                match_info[f'cs_diff_{minute}'] = snap.get('blue_cs', 0) - snap.get('red_cs', 0)
                match_info[f'kills_diff_{minute}'] = snap.get('blue_kills', 0) - snap.get('red_kills', 0)
                match_info[f'dragon_diff_{minute}'] = snap.get('blue_dragons', 0) - snap.get('red_dragons', 0)
                match_info[f'herald_diff_{minute}'] = snap.get('blue_heralds', 0) - snap.get('red_heralds', 0)
                match_info[f'tower_diff_{minute}'] = snap.get('blue_turrets', 0) - snap.get('red_turrets', 0)
                
                # ✅ NUEVAS: DIFERENCIAS DE WARDS
                match_info[f'wards_placed_diff_{minute}'] = snap.get('blue_wards_placed', 0) - snap.get('red_wards_placed', 0)
                match_info[f'wards_killed_diff_{minute}'] = snap.get('blue_wards_killed', 0) - snap.get('red_wards_killed', 0)
        
        # ====================================================================
        # THRESHOLDS BINARIOS - SOLO LOS MÁS PREDICTIVOS (10 MINUTOS)
        # ====================================================================
        if 'gold_diff_10' in match_info:
            match_info['gold_lead_gt_1500_10'] = 1 if match_info['gold_diff_10'] > 1500 else 0
        
        if 'kills_diff_10' in match_info:
            match_info['kill_lead_gt_3_10'] = 1 if match_info['kills_diff_10'] > 3 else 0
        
        if 'tower_diff_10' in match_info:
            match_info['tower_lead_gt_1_10'] = 1 if match_info['tower_diff_10'] > 1 else 0
        
        if 'dragon_diff_10' in match_info and 'herald_diff_10' in match_info:
            match_info['double_objective_10'] = 1 if match_info['dragon_diff_10'] >= 1 and match_info['herald_diff_10'] >= 1 else 0
        
        # ✅ NUEVO: THRESHOLD DE VENTAJA DE WARDS
        if 'wards_placed_diff_10' in match_info:
            match_info['wards_lead_gt_5_10'] = 1 if match_info['wards_placed_diff_10'] > 5 else 0
        
        # ====================================================================
        # PRIMEROS OBJETIVOS - CON EQUIPO Y TIEMPO
        # ====================================================================
        first_obj = extract_first_objectives(timeline_data)
        
        # Variables binarias alineadas con BLUE
        match_info['first_blood_blue'] = 1 if first_obj['first_blood_team'] == 'blue' else 0
        match_info['first_tower_blue'] = 1 if first_obj['first_tower_team'] == 'blue' else 0
        match_info['first_dragon_blue'] = 1 if first_obj['first_dragon_team'] == 'blue' else 0
        match_info['first_herald_blue'] = 1 if first_obj['first_herald_team'] == 'blue' else 0
        
        # Tiempos (útiles para análisis)
        match_info['first_tower_time'] = first_obj['first_tower_time']
        match_info['first_dragon_time'] = first_obj['first_dragon_time']
        match_info['first_herald_time'] = first_obj['first_herald_time']
        match_info['first_blood_time'] = first_obj['first_blood_time']
        
        # ====================================================================
        # TARGET
        # ====================================================================
        match_info['blue_wins'] = 1 if blue_team[0].get('win', False) else 0
        
        return match_info
        
    except Exception as e:
        print(f"❌ Error procesando match {match_id}: {e}")
        return None

# ============================================================================
# BASE DE DATOS WIDE - VERSIÓN CON WARDS
# ============================================================================

class WideMatchDatabase:
    """Base de datos para formato wide - CON WARDS"""
    
    def __init__(self, db_file: str):
        self.db_file = db_file
        self._init_database()
    
    def _init_database(self):
        os.makedirs(os.path.dirname(self.db_file), exist_ok=True)
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS matches_wide (
                -- IDENTIFICACIÓN
                match_id TEXT PRIMARY KEY,
                game_duration REAL,
                
                -- COMPOSICIÓN BLUE
                blue_top_champion TEXT, blue_jungle_champion TEXT, 
                blue_mid_champion TEXT, blue_adc_champion TEXT, 
                blue_support_champion TEXT,
                blue_num_tanks INTEGER, blue_num_fighters INTEGER, 
                blue_num_mages INTEGER, blue_num_marksmans INTEGER, 
                blue_num_controllers INTEGER,
                
                -- COMPOSICIÓN RED
                red_top_champion TEXT, red_jungle_champion TEXT, 
                red_mid_champion TEXT, red_adc_champion TEXT, 
                red_support_champion TEXT,
                red_num_tanks INTEGER, red_num_fighters INTEGER, 
                red_num_mages INTEGER, red_num_marksmans INTEGER, 
                red_num_controllers INTEGER,
                
                -- DIFERENCIAS 5 MIN
                gold_diff_5 INTEGER, xp_diff_5 INTEGER, cs_diff_5 INTEGER,
                kills_diff_5 INTEGER, dragon_diff_5 INTEGER, 
                herald_diff_5 INTEGER, tower_diff_5 INTEGER,
                wards_placed_diff_5 INTEGER, wards_killed_diff_5 INTEGER,
                
                -- DIFERENCIAS 10 MIN
                gold_diff_10 INTEGER, xp_diff_10 INTEGER, cs_diff_10 INTEGER,
                kills_diff_10 INTEGER, dragon_diff_10 INTEGER, 
                herald_diff_10 INTEGER, tower_diff_10 INTEGER,
                wards_placed_diff_10 INTEGER, wards_killed_diff_10 INTEGER,
                
                -- DIFERENCIAS 15 MIN
                gold_diff_15 INTEGER, xp_diff_15 INTEGER, cs_diff_15 INTEGER,
                kills_diff_15 INTEGER, dragon_diff_15 INTEGER, 
                herald_diff_15 INTEGER, tower_diff_15 INTEGER,
                wards_placed_diff_15 INTEGER, wards_killed_diff_15 INTEGER,
                
                -- THRESHOLDS (10 MIN)
                gold_lead_gt_1500_10 INTEGER,
                kill_lead_gt_3_10 INTEGER,
                tower_lead_gt_1_10 INTEGER,
                double_objective_10 INTEGER,
                wards_lead_gt_5_10 INTEGER,
                
                -- PRIMEROS OBJETIVOS (BINARIO BLUE)
                first_blood_blue INTEGER,
                first_tower_blue INTEGER,
                first_dragon_blue INTEGER,
                first_herald_blue INTEGER,
                
                -- TIEMPOS DE PRIMEROS OBJETIVOS
                first_tower_time REAL,
                first_dragon_time REAL,
                first_herald_time REAL,
                first_blood_time REAL,
                
                -- TARGET
                blue_wins INTEGER,
                
                -- METADATA
                processed_at TIMESTAMP
            )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_wide_match_id ON matches_wide(match_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_wide_blue_wins ON matches_wide(blue_wins)')
        
        conn.commit()
        conn.close()
    
    def save_wide_match_data(self, match_data: Dict):
        """Guarda datos de partida en formato wide"""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        
        try:
            # Lista completa de columnas (CON WARDS)
            columns = [
                'match_id', 'game_duration',
                'blue_top_champion', 'blue_jungle_champion', 'blue_mid_champion',
                'blue_adc_champion', 'blue_support_champion',
                'blue_num_tanks', 'blue_num_fighters', 'blue_num_mages',
                'blue_num_marksmans', 'blue_num_controllers',
                'red_top_champion', 'red_jungle_champion', 'red_mid_champion',
                'red_adc_champion', 'red_support_champion',
                'red_num_tanks', 'red_num_fighters', 'red_num_mages',
                'red_num_marksmans', 'red_num_controllers',
                'gold_diff_5', 'xp_diff_5', 'cs_diff_5', 'kills_diff_5',
                'dragon_diff_5', 'herald_diff_5', 'tower_diff_5',
                'wards_placed_diff_5', 'wards_killed_diff_5',
                'gold_diff_10', 'xp_diff_10', 'cs_diff_10', 'kills_diff_10',
                'dragon_diff_10', 'herald_diff_10', 'tower_diff_10',
                'wards_placed_diff_10', 'wards_killed_diff_10',
                'gold_diff_15', 'xp_diff_15', 'cs_diff_15', 'kills_diff_15',
                'dragon_diff_15', 'herald_diff_15', 'tower_diff_15',
                'wards_placed_diff_15', 'wards_killed_diff_15',
                'gold_lead_gt_1500_10', 'kill_lead_gt_3_10',
                'tower_lead_gt_1_10', 'double_objective_10',
                'wards_lead_gt_5_10',
                'first_blood_blue', 'first_tower_blue',
                'first_dragon_blue', 'first_herald_blue',
                'first_tower_time', 'first_dragon_time',
                'first_herald_time', 'first_blood_time',
                'blue_wins', 'processed_at'
            ]
            
            values = []
            for col in columns:
                values.append(match_data.get(col))
            
            placeholders = ','.join(['?'] * len(columns))
            query = f'''
                INSERT OR REPLACE INTO matches_wide 
                ({','.join(columns)})
                VALUES ({placeholders})
            '''
            
            cursor.execute(query, values)
            conn.commit()
            
        except Exception as e:
            print(f"❌ Error guardando datos wide: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def get_match_count(self) -> int:
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM matches_wide")
        count = cursor.fetchone()[0]
        conn.close()
        return count
    
    def export_to_csv(self, filename: str):
        conn = sqlite3.connect(self.db_file)
        try:
            df = pd.read_sql_query("SELECT * FROM matches_wide", conn)
            if not df.empty:
                df.to_csv(filename, index=False)
                print(f"   📊 CSV exportado: {filename} ({len(df)} partidas)")
        except Exception as e:
            print(f"   ❌ Error exportando CSV: {e}")
        finally:
            conn.close()
# ============================================================================
# PROCESAR Y ESCRIBIR DIRECTO A CSV (CON COLUMNAS FIJAS)
# ============================================================================

def batch_process_to_csv(batch_size: int = 100):
    """Procesa JSONs y escribe DIRECTAMENTE en CSV - CON COLUMNAS FIJAS"""
    
    json_files = [f for f in os.listdir(JSON_DIR) 
                  if f.endswith('.json') and not f.endswith('_timeline.json')]
    
    print(f"📁 Procesando {len(json_files)} archivos...")
    
    # ========================================================================
    # PASO 1: DEFINIR COLUMNAS FIJAS (TODAS LAS POSIBLES)
    # ========================================================================
    FIXED_COLUMNS = [
        # IDENTIFICACIÓN
        'match_id', 'game_duration',
        
        # COMPOSICIÓN BLUE
        'blue_top_champion', 'blue_jungle_champion', 'blue_mid_champion',
        'blue_adc_champion', 'blue_support_champion',
        'blue_num_tanks', 'blue_num_fighters', 'blue_num_mages',
        'blue_num_marksmans', 'blue_num_controllers',
        
        # COMPOSICIÓN RED
        'red_top_champion', 'red_jungle_champion', 'red_mid_champion',
        'red_adc_champion', 'red_support_champion',
        'red_num_tanks', 'red_num_fighters', 'red_num_mages',
        'red_num_marksmans', 'red_num_controllers',
        
        # DIFERENCIAS 5 MIN
        'gold_diff_5', 'xp_diff_5', 'cs_diff_5', 'kills_diff_5',
        'dragon_diff_5', 'herald_diff_5', 'tower_diff_5',
        'wards_placed_diff_5', 'wards_killed_diff_5',
        
        # DIFERENCIAS 10 MIN
        'gold_diff_10', 'xp_diff_10', 'cs_diff_10', 'kills_diff_10',
        'dragon_diff_10', 'herald_diff_10', 'tower_diff_10',
        'wards_placed_diff_10', 'wards_killed_diff_10',
        
        # DIFERENCIAS 15 MIN
        'gold_diff_15', 'xp_diff_15', 'cs_diff_15', 'kills_diff_15',
        'dragon_diff_15', 'herald_diff_15', 'tower_diff_15',
        'wards_placed_diff_15', 'wards_killed_diff_15',
        
        # THRESHOLDS
        'gold_lead_gt_1500_10', 'kill_lead_gt_3_10',
        'tower_lead_gt_1_10', 'double_objective_10',
        'wards_lead_gt_5_10',
        
        # PRIMEROS OBJETIVOS
        'first_blood_blue', 'first_tower_blue',
        'first_dragon_blue', 'first_herald_blue',
        'first_tower_time', 'first_dragon_time',
        'first_herald_time', 'first_blood_time',
        
        # TARGET
        'blue_wins',
        
        # METADATA
        'processed_at'
    ]
    
    print(f"📋 Columnas fijas: {len(FIXED_COLUMNS)}")
    
    # ========================================================================
    # PASO 2: ESCRIBIR CSV CON COLUMNAS FIJAS
    # ========================================================================
    header_written = False
    total_successful = 0
    
    for i, filename in enumerate(json_files):
        match_id = filename.replace('.json', '')
        
        try:
            wide_data = process_match_to_wide(match_id)
            
            if wide_data:
                # Crear diccionario con TODAS las columnas fijas
                row = {}
                for col in FIXED_COLUMNS:
                    row[col] = wide_data.get(col)  # Si no existe, pone None
                
                # Escribir
                with open(CSV_FILE, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=FIXED_COLUMNS)
                    
                    if not header_written:
                        writer.writeheader()
                        header_written = True
                    
                    writer.writerow(row)
                
                total_successful += 1
            
            if (i + 1) % 50 == 0:
                print(f"   ✅ {i + 1}/{len(json_files)} - Guardadas: {total_successful}")
                
        except Exception as e:
            print(f"   ❌ Error {match_id}: {e}")
            continue
    
    print(f"\n🎉 Procesamiento completado!")
    print(f"   ✅ Partidas guardadas: {total_successful}")
    print(f"   💾 Archivo: {CSV_FILE}")
    print(f"   📋 Columnas fijas: {len(FIXED_COLUMNS)}")
    
    # Verificar
    df = pd.read_csv(CSV_FILE)
    print(f"   📊 Shape: {df.shape}")
    
    return total_successful

# ============================================================================
# ALTERNATIVA: PANDAS CON CHUNKS (MÁS LENTO, MENOS CÓDIGO)
# ============================================================================

def batch_process_pandas_chunks(chunk_size: int = 100):
    """Procesa en lotes y guarda con pandas (menos eficiente)"""
    
    json_files = [f for f in os.listdir(JSON_DIR) 
                  if f.endswith('.json') and not f.endswith('_timeline.json')]
    
    print(f"📁 Procesando {len(json_files)} archivos en chunks de {chunk_size}...")
    
    for chunk_start in range(0, len(json_files), chunk_size):
        chunk_files = json_files[chunk_start:chunk_start + chunk_size]
        chunk_data = []
        
        for filename in chunk_files:
            match_id = filename.replace('.json', '')
            try:
                wide_data = process_match_to_wide(match_id)
                if wide_data:
                    chunk_data.append(wide_data)
            except:
                continue
        
        # Guardar chunk
        df_chunk = pd.DataFrame(chunk_data)
        mode = 'w' if chunk_start == 0 else 'a'
        header = (chunk_start == 0)
        df_chunk.to_csv(CSV_FILE, mode=mode, header=header, index=False)
        
        print(f"   ✅ Chunk {chunk_start//chunk_size + 1}: {len(df_chunk)} partidas")
    
    print(f"\n🎉 Completado: {CSV_FILE}")
# ============================================================================
# MAIN
# ============================================================================

def main():
    print("🚀 PROCESADOR WIDE - VERSIÓN CON WARDS")
    print("=" * 60)
    print("📁 Directorio JSON:", JSON_DIR)
    print("💾 Archivo CSV:", CSV_FILE)
    print("=" * 60)
    
    batch_size = input("\n📦 Tamaño del lote (default 100): ").strip()
    batch_size = int(batch_size) if batch_size else 100
    
    batch_process_to_csv(batch_size=batch_size)

if __name__ == "__main__":
    main()