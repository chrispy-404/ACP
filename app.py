import streamlit as st
import pandas as pd
import numpy as np
import mysql.connector
import os
from datetime import datetime, date, time as dt_time
import time
from dateutil.relativedelta import relativedelta

# --- 1. PAGE CONFIG ---
st.set_page_config(page_title="Digitale Einsatzplanung", layout="wide")

# --- 2. KONSTANTEN ---
DATE_FORMAT = '%Y-%m-%d' 
GERMAN_WEEKDAYS = ['Mo', 'Di', 'Mi', 'Do', 'Fr', 'Sa', 'So']
LOGO_PATH = 'acp_logo.png' 

# --- INITIALE DATEN (Automatisch generiert & Sortiert) ---
# Format: "Standortname": ["Slot1", "Slot2", ...]
INITIAL_LOCATIONS = {
    "0278 Darmstadt": ["278 - Darmstadt"],
    "0530 Megastore Cologne": ["530 -  Megastore Cologne"],
    "0537 Berlin Karl-Marx-Str.": ["0537 Berlin Karl-Marx-Str."],
    "0562 Hamburg Billstedt ": ["0562 Hamburg Billstedt"],
    "0695 Nürnberg": ["0695 Nürnberg"],
    "0827 Mannheim": ["0827 Mannheim"],
    "0976 Essen": ["976 Essen"],
    "1069 Bremen Waterfront": ["1069 Bremen Waterfront"],
    "1101 Bonn": ["1101 Bonn"],
    "1156 Bielefeld Loom": ["1156 Bielefeld Loom"],
    "1203 Dortmund": ["1203 Dortmund"],
    "1336 Hannover": ["1336 Hannover"],
    "1354 Wiesbaden": ["1354 Wiesbaden"],
    "1355 Frankfurt Nord West Zentrum": ["1355 Frankfurt Nord West Zentrum"],
    "1367 Düsseldorf Mega": ["1367 - Düsseldorf Mega"],
    "1413 Alexanderplatz": ["1413 - Alexanderplatz"],
    "1476 Dresden": ["1476 Dresden"],
    "1624 Stuttgart Königsstraße ": ["1624 Königsstraße Stuttgart"],
    "1739 Berlin Tauentzienstr.": ["1739 Berlin Tauentzienstr."],
    "1781Offenbach": ["1781Offenbach"],
    "1997 Oberhausen": ["1997 Oberhausen"],
    "2063 MTZ Main Taunus Zentrum": ["2063 MTZ Main Taunus Zentrum"],
    "2115 Mall of Berlin": ["2115 - Mall of Berlin"],
    "2125 Hamburg Hafencity": ["2125 Hamburg Hafencity"],
    "2127 Frankfurt My Zeil": ["2127 Frankfurt My Zeil MA 1"],
    "2127 Frankfurt MyZeil": ["2127 Frankfurt MyZeil MA 2"],
    "2147 Trier Galerie": ["MA 1"],
    "2217 München Kaufingerstraße ": ["MA 1", "MA 2"],
    "2303 Gesundbrunnen": ["2303 Gesundbrunnen"],
    "829 Duisburg": ["829 Duisburg"],
    "Arcese Louis Vuitton Lager Köln Arcese": ["MA1 Innen", "MA2 Aussen", "MA3 Aussen Nacht", "MA4"],
    "Balenciaga Hamburg": ["MA1", "MA2", "MA3"],
    "Balenciaga Ingolstadt": ["MA1", "MA2", "Pause (auf Abruf)"],
    "Breuninger Düsseldorf": ["MA 2", "MA 3", "MA1", "MA4"],
    "Breuninger Veranstaltung Extrabestellungen": ["MA1", "MA2", "MA3", "MA4", "MA5", "MA6", "MA7", "MA8"],
    "DSV Stuttgart GmbH & Co. KG": ["MA1", "MA2", "MA3"],
    "Dior Düsseldorf neuer Store Königsallee 19 ": ["MA 3", "MA 4", "MA 5", "MA 6", "MA 7", "MA1", "MA2"],
    "Fendi Düsseldorf": ["MA1", "Pause Ablöser"],
    "Fendi München": ["MA1", "MA2", "MA3"],
    "Gucci Düsseldorf": ["MA1", "MA2"],
    "Gucci Hamburg Neuer Wall": ["MA1", "MA2", "MA3 Zusatz", "MA4 Zusatz"],
    "Hogan Düsseldorf": ["MA1", "MA2"],
    "JVA Willich": ["MA1"],
    "Marokanisches Konsulat": ["MA1"],
    "Moviepark": ["MA 10", "MA 11", "MA 12", "MA 13", "MA 14", "MA 15", "MA 16", "MA 17", "MA 18", "MA 19", "MA 20", "MA 21", "MA 6", "MA 7", "MA 8", "MA 9", "MA1", "MA2", "MA3", "MA4", "MA5"],
    "PWC Security Sodexo": ["Sodexo MA1 Sicherheitskraft"],
    "Personenschutz Israelische Delegation ": ["MA 6", "MA1", "MA2", "MA3", "MA4", "MA5"],
    "Prada Düsseldorf": ["MA2", "Prada Düsseldorf"],
    "PwC Empfang ": ["PWC MA1 Empfang"],
    "Rathaus Neuss": ["MA1", "MA2", "MA3"],
    "Saint Laurent Düsseldorf in Breuninger": ["MA1", "MA2"],
    "Sitec BLB Schwannstraße 10": ["MA 2", "MA 3", "MA 4", "MA1"],
    "Sitec HSD": ["MA 6", "MA1", "MA2", "MA3", "MA4", "MA5"],
    "Sitec WDR Rolltor": ["MA 4", "MA 5", "MA1", "MA2", "MA3"],
    "Sitec Ü-Wagen Neuss": ["MA 1", "MA 2"],
    "Tods Düsseldorf": ["MA1", "MA2"],
    "Walbrecht Brandschutz Köln": ["MA1", "MA2", "MA3", "MA4"],
    "Wohnbau Niederkasseler Lohweg Tiefgarage ": ["MA1", "MA2", "MA3", "MA4"],
    "YSL Hamburg": ["MA1", "MA2"],
    "ZDF Volle Kanne": ["MA 1", "MA 2"]
}

# --- BENUTZER & PASSWÖRTER ---
USER_CREDENTIALS = {
    'admin': 'admin123',
    'planer': 'planer2025',
    'saki': 'saki123'
}

# --- 3. KONFIGURATION ---
OBJECT_COLUMN_NAME = 'Objektname' 
MA_SLOT_COLUMN_NAME = 'MA_Slot' 
DB_DATE_COL = 'Datum' 

# --- 4. DATENBANK VERBINDUNG (MySQL/TiDB) ---
def get_db_connection():
    try:
        conn = mysql.connector.connect(
            host=st.secrets["mysql"]["host"],
            port=st.secrets["mysql"]["port"],
            user=st.secrets["mysql"]["user"],
            password=st.secrets["mysql"]["password"],
            database=st.secrets["mysql"]["database"]
        )
        return conn
    except Exception as e:
        st.error(f"Datenbank-Verbindungsfehler: {e}")
        st.stop()

def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS mitarbeiter_verzeichnis (ID INT AUTO_INCREMENT PRIMARY KEY, Mitarbeitername VARCHAR(255) UNIQUE, Geburtsdatum VARCHAR(20), Personalnummer VARCHAR(50), Bewacher_ID VARCHAR(50), Anstellung VARCHAR(50), Position VARCHAR(50), Vertrag_bis VARCHAR(20), Adresse VARCHAR(255), PLZ VARCHAR(20), Telefonnummer VARCHAR(50), Ausweis_gueltig_bis VARCHAR(20))""")
    cursor.execute(f"""CREATE TABLE IF NOT EXISTS locations_spalte (ID INT AUTO_INCREMENT PRIMARY KEY, `{OBJECT_COLUMN_NAME}` VARCHAR(255), `{MA_SLOT_COLUMN_NAME}` VARCHAR(255), Ansprechpartner VARCHAR(255), Telefon VARCHAR(100))""")
    cursor.execute("""CREATE TABLE IF NOT EXISTS urlaub_krank (ID INT AUTO_INCREMENT PRIMARY KEY, Datum VARCHAR(20), Mitarbeiter VARCHAR(255), Status VARCHAR(50), UNIQUE KEY ma_date (Datum, Mitarbeiter))""")
    cursor.execute("""CREATE TABLE IF NOT EXISTS einsaetze (EinsatzID INT AUTO_INCREMENT PRIMARY KEY, Datum VARCHAR(20), Objekt VARCHAR(255), MA_Slot VARCHAR(255), Anfang DOUBLE, Ende DOUBLE, Pause DOUBLE, Mitarbeiter VARCHAR(255), Zeit DOUBLE)""")
    conn.commit()
    cursor.close()
    conn.close()

def seed_initial_data(conn):
    """Fügt Start-Daten aus der Liste in die DB ein, falls Standort noch nicht existiert."""
    cursor = conn.cursor()
    try:
        # Sortiere Locations alphabetisch beim Anlegen
        sorted_locations = sorted(INITIAL_LOCATIONS.keys())
        
        for loc_name in sorted_locations:
            slots = INITIAL_LOCATIONS[loc_name]
            # Prüfen ob Standort schon existiert
            cursor.execute(f"SELECT count(*) FROM locations_spalte WHERE `{OBJECT_COLUMN_NAME}` = %s", (loc_name,))
            if cursor.fetchone()[0] == 0:
                # Anlegen der spezifischen Slots aus der Liste
                for slot in slots:
                    cursor.execute(f"INSERT INTO locations_spalte (`{OBJECT_COLUMN_NAME}`, `{MA_SLOT_COLUMN_NAME}`) VALUES (%s, %s)", (loc_name, slot))
        conn.commit()
    except Exception as e:
        print(f"Info: Initialdaten Check Fehler: {e}")
    finally:
        cursor.close()

# Einmalige Initialisierung beim Start
if 'db_initialized' not in st.session_state:
    try:
        init_db()
        conn = get_db_connection()
        seed_initial_data(conn)
        conn.close()
        st.session_state['db_initialized'] = True
    except Exception as e:
        st.error(f"Fehler bei der DB-Initialisierung: {e}")

# --- 5. HELPER & LOGIK ---

def to_bold(text):
    chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    bold_chars = "𝐀𝐁𝐂𝐃𝐄𝐅𝐆𝐇𝐈𝐉𝐊𝐋𝐌𝐍𝐎𝐏𝐐𝐑𝐒𝐓𝐔𝐕𝐖𝐗𝐘𝐙𝐚𝐛𝐜𝐝𝐞𝐟𝐠𝐡𝐢𝐣𝐤𝐥𝐦𝐧𝐨𝐩𝐪𝐫𝐬𝐭𝐮𝐯𝐰𝐱𝐲𝐳𝟎𝟏𝟐𝟑𝟒𝟓𝟔𝟕𝟖𝟗"
    return text.translate(str.maketrans(chars, bold_chars))

def format_duration_str(hours_float):
    if not hours_float or hours_float == 0: return "0 Std 0 Min"
    h = int(hours_float)
    m = int(round((hours_float - h) * 60))
    if m == 60: h += 1; m = 0
    return f"{h} Std {m} Min"

def float_to_input_str(val):
    if pd.isna(val) or val == 0: return ""
    minutes = round(val * 24 * 60)
    hours = int(minutes // 60)
    mins = int(minutes % 60)
    if hours >= 24: hours = 23; mins = 59
    return f"{hours:02d}:{mins:02d}"

def parse_user_time(val_str):
    if not val_str: return 0.0
    s = str(val_str).strip().replace(',', '.')
    try:
        if ':' in s:
            parts = s.split(':')
            h = int(parts[0]); m = int(parts[1]) if len(parts) > 1 else 0
            return (h + m/60.0) / 24.0
        v = float(s)
        if v >= 100: 
            h = int(v // 100); m = int(v % 100)
            return (h + m/60.0) / 24.0
        return v / 24.0
    except: return 0.0

def calculate_arbeitszeit(anfang_zeit_float, ende_zeit_float, pause_zeit_std):
    if anfang_zeit_float == 0.0 and ende_zeit_float == 0.0: return 0.0
    if ende_zeit_float < anfang_zeit_float:
        zeit_differenz = (ende_zeit_float + 1.0) - anfang_zeit_float
    else:
        zeit_differenz = ende_zeit_float - anfang_zeit_float
    pause_als_tag_bruch = pause_zeit_std / 24.0
    arbeitszeit_stunden = 24.0 * (zeit_differenz - pause_als_tag_bruch)
    return max(0.0, arbeitszeit_stunden)

def validate_einsatz(urlaub_krank_df, mitarbeiter_name, einsatz_datum):
    if isinstance(einsatz_datum, str):
        try: einsatz_datum = datetime.strptime(einsatz_datum, DATE_FORMAT).date()
        except: pass 
        
    filter_date_str = einsatz_datum.strftime(DATE_FORMAT) 
    eintrag = urlaub_krank_df[
        (urlaub_krank_df['Datum'] == filter_date_str) & 
        (urlaub_krank_df['Mitarbeiter'] == mitarbeiter_name)
    ]
    if not eintrag.empty:
        status = eintrag['Status'].iloc[0]
        if pd.notna(status) and status.strip() != "":
            return False, f"⚠️ {status}"
    return True, "OK"

def check_double_booking(conn, mitarbeiter, datum, start_float, end_float, current_object):
    if not mitarbeiter or (start_float == 0 and end_float == 0):
        return False, ""
    
    if isinstance(datum, str):
        try: datum = datetime.strptime(datum, DATE_FORMAT).date()
        except: pass

    date_str = datum.strftime(DATE_FORMAT)
    s1 = start_float; e1 = end_float
    if e1 < s1: e1 += 1.0 
    
    query = "SELECT Objekt, MA_Slot, Anfang, Ende FROM einsaetze WHERE Mitarbeiter = %s AND Datum = %s AND Objekt != %s"
    df_conflicts = pd.read_sql(query, conn, params=(mitarbeiter, date_str, current_object))
    
    for _, row in df_conflicts.iterrows():
        s2 = row['Anfang']; e2 = row['Ende']
        if e2 < s2: e2 += 1.0
        
        if max(s1, s2) < min(e1, e2):
            t_start = float_to_input_str(row['Anfang'])
            t_end = float_to_input_str(row['Ende'])
            return True, f"Überschneidung mit '{row['Objekt']} ({row['MA_Slot']})' ({t_start}-{t_end})"
            
    return False, ""

# --- 6. DATENBANK OPERATIONEN ---

@st.cache_data(ttl=60) 
def load_data_from_db(_conn, table_name):
    return pd.read_sql(f"SELECT * FROM {table_name}", _conn)

@st.cache_data(ttl=5) 
def load_einsaetze_for_object(_conn, object_name):
    query = f"SELECT * FROM einsaetze WHERE Objekt = '{object_name}' ORDER BY Datum"
    df = pd.read_sql(query, _conn)
    df['Datum'] = pd.to_datetime(df['Datum']).dt.date
    return df

def save_einsaetze_to_db(conn, df_einsaetze, object_name, start_date, end_date):
    cursor = conn.cursor()
    try:
        s_str = start_date.strftime(DATE_FORMAT)
        e_str = end_date.strftime(DATE_FORMAT)
        cursor.execute("DELETE FROM einsaetze WHERE Objekt = %s AND Datum >= %s AND Datum <= %s", (object_name, s_str, e_str))
        
        if not df_einsaetze.empty:
            data = []
            for _, row in df_einsaetze.iterrows():
                data.append((row['Datum'], row['Objekt'], row['MA_Slot'], row['Anfang'], row['Ende'], row['Pause'], row['Mitarbeiter'], row['Zeit']))
            
            sql = "INSERT INTO einsaetze (Datum, Objekt, MA_Slot, Anfang, Ende, Pause, Mitarbeiter, Zeit) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.executemany(sql, data)
            
        conn.commit()
    except Exception as e: conn.rollback(); raise e
    finally: cursor.close()

# --- DB UPDATE ---

def delete_standort(conn, objekt_name):
    cursor = conn.cursor()
    try: 
        cursor.execute(f"DELETE FROM locations_spalte WHERE `{OBJECT_COLUMN_NAME}` = %s", (objekt_name,))
        conn.commit()
        return True
    except: conn.rollback(); return False
    finally: cursor.close()

def update_standort(conn, alter_name, neuer_name, neue_slot_anzahl, aktuelle_slots, neuer_ansprechpartner, neues_telefon):
    cursor = conn.cursor()
    try:
        sql_update_meta = f"UPDATE locations_spalte SET `{OBJECT_COLUMN_NAME}` = %s, Ansprechpartner = %s, Telefon = %s WHERE `{OBJECT_COLUMN_NAME}` = %s"
        cursor.execute(sql_update_meta, (neuer_name, neuer_ansprechpartner, neues_telefon, alter_name))
        
        if alter_name != neuer_name: 
            cursor.execute("UPDATE einsaetze SET Objekt = %s WHERE Objekt = %s", (neuer_name, alter_name))
            
        # Logik für Änderung der Slot-Anzahl ist bei benannten Slots schwieriger
        # Hier vereinfacht: Wir fügen nur neue MA Slots hinzu wenn Zahl steigt
        aktuelle_anzahl = len(aktuelle_slots)
        if neue_slot_anzahl > aktuelle_anzahl:
            for i in range(aktuelle_anzahl + 1, neue_slot_anzahl + 1):
                cursor.execute(f"INSERT INTO locations_spalte (`{OBJECT_COLUMN_NAME}`, `{MA_SLOT_COLUMN_NAME}`, Ansprechpartner, Telefon) VALUES (%s,%s,%s,%s)", (neuer_name, f"MA{i}", neuer_ansprechpartner, neues_telefon))
        elif neue_slot_anzahl < aktuelle_anzahl:
            # Löschen von hinten ist riskant bei benannten Slots, wir löschen die, die wie generierte Slots aussehen oder am Ende stehen
            # Einfachste Lösung: User muss manuell löschen über Editor
            pass 
        
        conn.commit()
        return True
    except Exception as e: conn.rollback(); st.error(f"Fehler: {e}"); return False
    finally: cursor.close()

def delete_mitarbeiter(conn, ma_name):
    cursor = conn.cursor()
    try: 
        cursor.execute("DELETE FROM mitarbeiter_verzeichnis WHERE Mitarbeitername = %s", (ma_name,))
        conn.commit()
        return True
    except: conn.rollback(); return False
    finally: cursor.close()

def update_mitarbeiter(conn, altes_profil, new_vals):
    cursor = conn.cursor()
    try:
        sql = """UPDATE mitarbeiter_verzeichnis SET Mitarbeitername=%s, Geburtsdatum=%s, Personalnummer=%s, Bewacher_ID=%s, Anstellung=%s, Position=%s, Vertrag_bis=%s, Adresse=%s, PLZ=%s, Telefonnummer=%s, Ausweis_gueltig_bis=%s WHERE Mitarbeitername=%s"""
        v = (new_vals['Mitarbeitername'], new_vals['Geburtsdatum'], new_vals['Personalnummer'], new_vals['Bewacher_ID'], new_vals['Anstellung'], new_vals['Position'], new_vals['Vertrag_bis'], new_vals['Adresse'], new_vals['PLZ'], new_vals['Telefonnummer'], new_vals['Ausweis_gueltig_bis'], altes_profil['Mitarbeitername'])
        cursor.execute(sql, v)
        
        if altes_profil['Mitarbeitername'] != new_vals['Mitarbeitername']:
            cursor.execute("UPDATE urlaub_krank SET Mitarbeiter = %s WHERE Mitarbeiter = %s", (new_vals['Mitarbeitername'], altes_profil['Mitarbeitername']))
            cursor.execute("UPDATE einsaetze SET Mitarbeiter = %s WHERE Mitarbeiter = %s", (new_vals['Mitarbeitername'], altes_profil['Mitarbeitername']))
            
        conn.commit()
        return True
    except Exception as e: conn.rollback(); st.error(str(e)); return False
    finally: cursor.close()

# --- LOGIN SYSTEM ---

def check_login():
    if 'logged_in' not in st.session_state:
        st.session_state['logged_in'] = False
    if not st.session_state['logged_in']:
        col1, col2, col3 = st.columns([1,1,1])
        with col2:
            if os.path.exists(LOGO_PATH):
                st.image(LOGO_PATH, use_container_width=True)
            st.header("Anmeldung")
            username = st.text_input("Benutzername")
            password = st.text_input("Passwort", type="password")
            if st.button("Einloggen", type="primary"):
                if username in USER_CREDENTIALS and USER_CREDENTIALS[username] == password:
                    st.session_state['logged_in'] = True
                    st.session_state['username'] = username
                    st.rerun()
                else:
                    st.error("Falscher Benutzername oder Passwort")
        return False
    return True

def logout():
    st.session_state['logged_in'] = False
    st.rerun()

# --- DIALOGE ---
@st.dialog("Neuen Standort anlegen")
def dialog_neuer_standort(conn):
    name = st.text_input("Name des Standorts:")
    c1, c2 = st.columns(2)
    anspr = c1.text_input("Ansprechpartner:")
    tel = c2.text_input("Telefon:")
    count = st.number_input("Anzahl MA-Slots:", min_value=1, value=5)
    if st.button("Erstellen", type="primary"):
        if name:
            cursor = conn.cursor()
            try:
                for i in range(1, int(count)+1): 
                    cursor.execute(f"INSERT INTO locations_spalte (`{OBJECT_COLUMN_NAME}`, `{MA_SLOT_COLUMN_NAME}`, Ansprechpartner, Telefon) VALUES (%s,%s,%s,%s)", (name, f"MA{i}", anspr, tel))
                conn.commit(); st.success("OK"); load_data_from_db.clear(); time.sleep(0.5); st.rerun()
            except Exception as e: conn.rollback(); st.error(str(e))
            finally: cursor.close()

@st.dialog("Standort bearbeiten")
def dialog_edit_standort(conn, name, slots, anspr, tel):
    st.write(f"Bearbeite: **{name}**")
    new_name = st.text_input("Name:", value=name)
    c1, c2 = st.columns(2)
    new_anspr = c1.text_input("Ansprechpartner:", value=anspr if anspr else "")
    new_tel = c2.text_input("Telefon:", value=tel if tel else "")
    new_cnt = st.number_input("Slots:", min_value=1, value=len(slots))
    c_save, c_del = st.columns(2)
    if c_save.button("Speichern"):
        if update_standort(conn, name, new_name, new_cnt, slots, new_anspr, new_tel): st.success("OK"); load_data_from_db.clear(); time.sleep(0.5); st.rerun()
    if c_del.button("Löschen", type="primary"):
        if delete_standort(conn, name): st.success("Gelöscht"); load_data_from_db.clear(); time.sleep(0.5); st.rerun()

@st.dialog("Neuer Mitarbeiter")
def dialog_neuer_mitarbeiter(conn):
    with st.form("new_ma"):
        name = st.text_input("Name *")
        c1,c2 = st.columns(2)
        geb=c1.date_input("Geburtsdatum", value=date(2000,1,1)); pnr=c1.text_input("Personalnr"); anst=c1.text_input("Anstellung"); pos=c1.text_input("Position"); tel=c1.text_input("Tel")
        bid=c2.text_input("Bewacher ID"); vbis=c2.date_input("Vertrag bis", value=date(2026,1,1)); abis=c2.date_input("Ausweis bis", value=date(2027,1,1))
        adr=st.text_input("Adresse"); plz=st.text_input("PLZ")
        if st.form_submit_button("Speichern", type="primary"):
            if name:
                cursor = conn.cursor()
                try:
                    cursor.execute("INSERT INTO mitarbeiter_verzeichnis (Mitarbeitername, Geburtsdatum, Personalnummer, Bewacher_ID, Anstellung, Position, Vertrag_bis, Adresse, PLZ, Telefonnummer, Ausweis_gueltig_bis) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                                     (name, geb.strftime(DATE_FORMAT), pnr, bid, anst, pos, vbis.strftime(DATE_FORMAT), adr, plz, tel, abis.strftime(DATE_FORMAT)))
                    conn.commit(); st.success("OK"); load_data_from_db.clear(); time.sleep(0.5); st.rerun()
                except Exception as e: conn.rollback(); st.error(str(e))
                finally: cursor.close()

@st.dialog("Mitarbeiter bearbeiten")
def dialog_edit_mitarbeiter(conn, row):
    with st.form("edit_ma"):
        name = st.text_input("Name", value=row['Mitarbeitername'])
        def d(x): 
            try: return pd.to_datetime(x).date()
            except: return date(2000,1,1)
        c1,c2 = st.columns(2)
        geb=c1.date_input("Geb", value=d(row['Geburtsdatum'])); pnr=c1.text_input("Pnr", value=row['Personalnummer'] or ""); anst=c1.text_input("Anst", value=row['Anstellung'] or ""); pos=c1.text_input("Pos", value=row['Position'] or ""); tel=c1.text_input("Tel", value=row['Telefonnummer'] or "")
        bid=c2.text_input("BID", value=row['Bewacher_ID'] or ""); vbis=c2.date_input("V-Bis", value=d(row['Vertrag_bis'])); abis=c2.date_input("A-Bis", value=d(row['Ausweis_gueltig_bis']))
        adr=st.text_input("Adr", value=row['Adresse'] or ""); plz=st.text_input("PLZ", value=row['PLZ'] or "")
        s = st.form_submit_button("Speichern")
        if s:
            vals = {'Mitarbeitername':name,'Geburtsdatum':geb.strftime(DATE_FORMAT),'Personalnummer':pnr,'Bewacher_ID':bid,'Anstellung':anst,'Position':pos,'Vertrag_bis':vbis.strftime(DATE_FORMAT),'Adresse':adr,'PLZ':plz,'Telefonnummer':tel,'Ausweis_gueltig_bis':abis.strftime(DATE_FORMAT)}
            if update_mitarbeiter(conn, {'Mitarbeitername':row['Mitarbeitername']}, vals): 
                st.success("OK"); load_data_from_db.clear()
                st.session_state.ma_editor_key += 1 # Reset Table Key
                time.sleep(0.5); st.rerun()
    if st.button("Löschen", type="primary"):
        if delete_mitarbeiter(conn, row['Mitarbeitername']): 
            st.success("Gelöscht"); load_data_from_db.clear()
            st.session_state.ma_editor_key += 1
            time.sleep(0.5); st.rerun()

# --- SEITEN ---
def seite_stammdaten_verwaltung(conn):
    st.header("Stammdatenverwaltung")
    t1, t2, t3 = st.tabs(["Mitarbeiter", "Standorte", "Urlaub/Krank"])
    df_ma = load_data_from_db(conn, 'mitarbeiter_verzeichnis')
    df_loc = load_data_from_db(conn, 'locations_spalte')
    
    # Session State Keys für Reset
    if 'ma_editor_key' not in st.session_state: st.session_state.ma_editor_key = 0
    if 'loc_editor_key' not in st.session_state: st.session_state.loc_editor_key = 0
    
    with t1:
        if st.button("➕ Neuer Mitarbeiter"): dialog_neuer_mitarbeiter(conn)
        search = st.text_input("Suche:", placeholder="Name...")
        
        df_show = df_ma.drop(columns=['ID']).copy()
        df_show.insert(0, "Auswahl", False)
        if search: df_show = df_show[df_show['Mitarbeitername'].str.contains(search, case=False, na=False)]
        
        # Sortieren der Anzeige
        df_show = df_show.sort_values(by="Mitarbeitername")

        col_config = {
            "Auswahl": st.column_config.CheckboxColumn("Edit", width="small"),
            "Mitarbeitername": st.column_config.TextColumn("Name", width="medium")
        }

        edited_df = st.data_editor(
            df_show,
            column_config=col_config,
            disabled=df_show.columns.drop("Auswahl"),
            hide_index=True,
            use_container_width=True,
            key=f"editor_ma_{st.session_state.ma_editor_key}"
        )
        
        selected_rows = edited_df[edited_df["Auswahl"]]
        if not selected_rows.empty:
            row = selected_rows.iloc[0]
            dialog_edit_mitarbeiter(conn, row)

    with t2:
        if st.button("➕ Neuer Standort"): dialog_neuer_standort(conn)
        if not df_loc.empty:
            df_grp = df_loc.groupby(OBJECT_COLUMN_NAME).agg({
                MA_SLOT_COLUMN_NAME: list,
                'Ansprechpartner': 'first',
                'Telefon': 'first'
            }).reset_index().sort_values(OBJECT_COLUMN_NAME)
            
            df_grp.insert(0, "Auswahl", False)
            
            col_config_loc = {
                "Auswahl": st.column_config.CheckboxColumn("Edit", width="small"),
                OBJECT_COLUMN_NAME: st.column_config.TextColumn("Standort", width="medium"),
                MA_SLOT_COLUMN_NAME: st.column_config.ListColumn("Slots", width="large"),
                "Ansprechpartner": st.column_config.TextColumn("Ansprechpartner", width="medium"),
                "Telefon": st.column_config.TextColumn("Telefon", width="medium")
            }
            
            edited_grp = st.data_editor(
                df_grp,
                column_config=col_config_loc,
                disabled=df_grp.columns.drop("Auswahl"),
                hide_index=True,
                use_container_width=True,
                key=f"editor_loc_{st.session_state.loc_editor_key}"
            )
            
            sel_loc = edited_grp[edited_grp["Auswahl"]]
            if not sel_loc.empty:
                r = sel_loc.iloc[0]
                dialog_edit_standort(conn, r[OBJECT_COLUMN_NAME], r[MA_SLOT_COLUMN_NAME], r['Ansprechpartner'], r['Telefon'])
    
    with t3:
        with st.form("uk"):
            c1,c2,c3=st.columns(3)
            dates = c1.date_input("Zeitraum", [], help="Start & Ende wählen")
            # Sortierte Mitarbeiterliste für Dropdown
            ma_list_sorted = sorted(df_ma['Mitarbeitername'].unique().tolist())
            ma = c2.selectbox("Mitarbeiter", [""]+ma_list_sorted)
            stat = c3.selectbox("Status", ["Urlaub","Krank","Ausfall","Standby"])
            if st.form_submit_button("Speichern", type="primary"):
                if ma and dates:
                    cursor = conn.cursor()
                    d1 = dates[0]; d2 = dates[1] if len(dates)>1 else d1
                    rng = pd.date_range(d1, d2).to_list()
                    try:
                        for d in rng: 
                            cursor.execute("REPLACE INTO urlaub_krank (Datum, Mitarbeiter, Status) VALUES (%s,%s,%s)", (d.strftime(DATE_FORMAT), ma, stat))
                        conn.commit(); st.success("OK"); load_data_from_db.clear()
                    except Exception as e: conn.rollback(); st.error(str(e))
                    finally: cursor.close()

def seite_einsatzplanung(conn, df_loc, df_uk, MA_LIST):
    st.header("Einsatzplanung")
    if df_loc.empty: st.warning("Keine Standorte."); return
    
    today = date.today()
    year_options = list(range(today.year - 1, today.year + 3))
    month_options = [datetime(2000, m, 1).strftime("%B") for m in range(1, 13)]

    st.sidebar.header("Filter")
    # Sortierte Standorte in Sidebar
    sorted_locs = sorted(df_loc[OBJECT_COLUMN_NAME].unique())
    obj = st.sidebar.selectbox("Objekt:", sorted_locs)
    
    selected_year = st.sidebar.selectbox("Jahr:", year_options, index=year_options.index(today.year))
    selected_month_name = st.sidebar.selectbox("Monat:", month_options, index=today.month - 1)
    
    selected_month_num = month_options.index(selected_month_name) + 1
    selected_month_str = f"{selected_month_name} {selected_year}"
    d_start = date(selected_year, selected_month_num, 1)
    d_end = (pd.to_datetime(d_start) + relativedelta(months=+1, days=-1)).date()
    
    row_info = df_loc[df_loc[OBJECT_COLUMN_NAME] == obj].iloc[0]
    anspr = row_info['Ansprechpartner']; tel = row_info['Telefon']
    info_str = f" ({', '.join(filter(None, [f'Anspr: {anspr}' if anspr else '', f'Tel: {tel}' if tel else '']))})" if anspr or tel else ""
    st.subheader(f"Plan: {obj}{info_str} - {selected_month_str}")

    slots = df_loc[df_loc[OBJECT_COLUMN_NAME]==obj][MA_SLOT_COLUMN_NAME].unique().tolist()
    # Sortiere Slots alphabetisch
    try: slots.sort() 
    except: pass
    
    df_saved = load_einsaetze_for_object(conn, obj)
    rng = pd.date_range(d_start, d_end).normalize().date
    df_plan = pd.DataFrame({'Datum': rng})
    # Wochentag im Datum Text für Anzeige
    df_plan['Datum_Tag'] = df_plan['Datum'].apply(lambda d: f"{d.strftime('%d.%m.%Y')} ({GERMAN_WEEKDAYS[d.weekday()]})")
    
    # FIX: Datum Spalte "medium" -> Mehr Platz für '01.01.2025 (Do)'
    col_cfg = {"Datum": None} 
    col_cfg['Datum_Tag'] =
