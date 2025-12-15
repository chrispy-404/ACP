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

# --- INITIALE DATEN (Hier Standorte eintragen) ---
# Format: "Standortname": Anzahl_Slots
INITIAL_LOCATIONS = {
    "Haupttor": 3,       # Erstellt MA1, MA2, MA3
    "Empfang": 1,        # Erstellt MA1
    "Werkschutz": 2      # Erstellt MA1, MA2
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
    # Holt die Daten aus den Streamlit Secrets
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
    # Tabellen erstellen (MySQL Syntax)
    cursor.execute("""CREATE TABLE IF NOT EXISTS mitarbeiter_verzeichnis (ID INT AUTO_INCREMENT PRIMARY KEY, Mitarbeitername VARCHAR(255) UNIQUE, Geburtsdatum VARCHAR(20), Personalnummer VARCHAR(50), Bewacher_ID VARCHAR(50), Anstellung VARCHAR(50), Position VARCHAR(50), Vertrag_bis VARCHAR(20), Adresse VARCHAR(255), PLZ VARCHAR(20), Telefonnummer VARCHAR(50), Ausweis_gueltig_bis VARCHAR(20))""")
    
    # Achtung: Spaltennamen in MySQL sollten keine Leerzeichen haben, wir nutzen Backticks
    cursor.execute(f"""CREATE TABLE IF NOT EXISTS locations_spalte (ID INT AUTO_INCREMENT PRIMARY KEY, `{OBJECT_COLUMN_NAME}` VARCHAR(255), `{MA_SLOT_COLUMN_NAME}` VARCHAR(255), Ansprechpartner VARCHAR(255), Telefon VARCHAR(100))""")
    
    cursor.execute("""CREATE TABLE IF NOT EXISTS urlaub_krank (ID INT AUTO_INCREMENT PRIMARY KEY, Datum VARCHAR(20), Mitarbeiter VARCHAR(255), Status VARCHAR(50), UNIQUE KEY ma_date (Datum, Mitarbeiter))""")
    
    cursor.execute("""CREATE TABLE IF NOT EXISTS einsaetze (EinsatzID INT AUTO_INCREMENT PRIMARY KEY, Datum VARCHAR(20), Objekt VARCHAR(255), MA_Slot VARCHAR(255), Anfang DOUBLE, Ende DOUBLE, Pause DOUBLE, Mitarbeiter VARCHAR(255), Zeit DOUBLE)""")
    
    conn.commit()
    cursor.close()
    conn.close()

def seed_initial_data(conn):
    """Fügt Start-Daten in die Datenbank ein, falls diese noch nicht existieren."""
    cursor = conn.cursor()
    try:
        for loc_name, num_slots in INITIAL_LOCATIONS.items():
            # Prüfen ob Standort schon existiert
            cursor.execute(f"SELECT count(*) FROM locations_spalte WHERE `{OBJECT_COLUMN_NAME}` = %s", (loc_name,))
            if cursor.fetchone()[0] == 0:
                # Anlegen
                for i in range(1, num_slots + 1):
                    slot = f"MA{i}"
                    cursor.execute(f"INSERT INTO locations_spalte (`{OBJECT_COLUMN_NAME}`, `{MA_SLOT_COLUMN_NAME}`) VALUES (%s, %s)", (loc_name, slot))
        conn.commit()
    except Exception as e:
        print(f"Info: Initialdaten konnten nicht vollständig angelegt werden (vielleicht existieren sie schon): {e}")
    finally:
        cursor.close()

# Einmalige Initialisierung beim Start
if 'db_initialized' not in st.session_state:
    try:
        init_db()
        # Automatisch Standorte anlegen
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
    
    # MySQL Query (%s statt ?)
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

# --- 6. DATENBANK OPERATIONEN (MySQL angepasst) ---

@st.cache_data(ttl=60) 
def load_data_from_db(_conn, table_name):
    # Bei MySQL müssen Spalten mit Leerzeichen/Sonderzeichen in Backticks, hier sicherheitshalber generell
    # Einfacher: wir lesen einfach alles
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
        
        # Batch Insert
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
            
        aktuelle_anzahl = len(aktuelle_slots)
        if neue_slot_anzahl > aktuelle_anzahl:
            for i in range(aktuelle_anzahl + 1, neue_slot_anzahl + 1):
                cursor.execute(f"INSERT INTO locations_spalte (`{OBJECT_COLUMN_NAME}`, `{MA_SLOT_COLUMN_NAME}`, Ansprechpartner, Telefon) VALUES (%s,%s,%s,%s)", (neuer_name, f"MA{i}", neuer_ansprechpartner, neues_telefon))
        elif neue_slot_anzahl < aktuelle_anzahl:
            for slot in [s for s in aktuelle_slots if int(s.replace("MA","")) > neue_slot_anzahl]:
                cursor.execute(f"DELETE FROM locations_spalte WHERE `{OBJECT_COLUMN_NAME}` = %s AND `{MA_SLOT_COLUMN_NAME}` = %s", (neuer_name, slot))
        
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
            ma = c2.selectbox("Mitarbeiter", [""]+df_ma['Mitarbeitername'].unique().tolist())
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
    obj = st.sidebar.selectbox("Objekt:", df_loc[OBJECT_COLUMN_NAME].unique())
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
    try: slots.sort(key=lambda x: int(x.replace("MA","")) if x.startswith("MA") and x[2:].isdigit() else x)
    except: pass
    
    df_saved = load_einsaetze_for_object(conn, obj)
    rng = pd.date_range(d_start, d_end).normalize().date
    df_plan = pd.DataFrame({'Datum': rng})
    # Wochentag im Datum Text für Anzeige
    df_plan['Datum_Tag'] = df_plan['Datum'].apply(lambda d: f"{d.strftime('%d.%m.%Y')} ({GERMAN_WEEKDAYS[d.weekday()]})")
    
    # FIX: Datum Spalte "medium" -> Mehr Platz für '01.01.2025 (Do)'
    col_cfg = {"Datum": None} 
    col_cfg['Datum_Tag'] = st.column_config.TextColumn(label="Datum", width="medium", disabled=True)

    ordered_cols = ['Datum', 'Datum_Tag']
    for slot in slots:
        # Daten auf Zeitraum filtern
        df_s = df_saved[(df_saved['MA_Slot']==slot) & (df_saved['Datum'] >= d_start) & (df_saved['Datum'] <= d_end)]
        df_s = df_s.rename(columns={'Anfang':f'{slot}_Anfang','Ende':f'{slot}_Ende','Pause':f'{slot}_Pause','Mitarbeiter':f'{slot}_Mitarbeiter', 'Zeit':f'{slot}_Zeit'})
        
        # Dynamic Autosize Logik:
        has_content = False
        if not df_s.empty:
            if df_s[f'{slot}_Mitarbeiter'].notna().any() or (df_s[f'{slot}_Anfang'] > 0).any():
                has_content = True
            
            df_s[f'{slot}_Anfang'] = df_s[f'{slot}_Anfang'].apply(float_to_input_str)
            df_s[f'{slot}_Ende'] = df_s[f'{slot}_Ende'].apply(float_to_input_str)
            df_plan = df_plan.merge(df_s[['Datum', f'{slot}_Mitarbeiter', f'{slot}_Anfang', f'{slot}_Ende', f'{slot}_Pause', f'{slot}_Zeit']], on='Datum', how='left')
        else:
            for c in [f'{slot}_Mitarbeiter', f'{slot}_Anfang', f'{slot}_Ende', f'{slot}_Pause', f'{slot}_Zeit']: df_plan[c] = None
        
        bold_header = to_bold(slot)
        
        # FIX: Autosize für Mitarbeiter (width=None wenn Daten, sonst medium)
        width_ma = None if has_content else "small" 
        width_time = "small" # Zeiten immer small
        
        col_cfg[f'{slot}_Mitarbeiter'] = st.column_config.SelectboxColumn(bold_header, options=MA_LIST, width=width_ma)
        col_cfg[f'{slot}_Anfang'] = st.column_config.TextColumn("Von", width=width_time, help="18, 18:30")
        col_cfg[f'{slot}_Ende'] = st.column_config.TextColumn("Bis", width=width_time)
        col_cfg[f'{slot}_Pause'] = st.column_config.NumberColumn("Pause", format="%.1f", min_value=0.0, max_value=24.0, width="small")
        col_cfg[f'{slot}_Zeit'] = st.column_config.NumberColumn("Zeit", format="%.2f", width="small", disabled=True)
        ordered_cols.extend([f'{slot}_Mitarbeiter', f'{slot}_Anfang', f'{slot}_Ende', f'{slot}_Pause', f'{slot}_Zeit'])

    st.markdown("---")
    
    # CONTAINER FÜR TABELLE (Stabilisierung)
    with st.container():
        edited = st.data_editor(
            df_plan[ordered_cols], # UNGESTYLT für Performance
            column_config=col_cfg, 
            height=700, 
            use_container_width=True, 
            hide_index=True
        )
    
    # --- FEHLERMELDUNGEN SAMMELN ---
    error_messages = []

    if st.button("💾 Plan Speichern", type="primary"):
        load_einsaetze_for_object.clear(); rows=[]; total=0.0
        
        for idx, row in edited.iterrows():
            d = df_plan.loc[idx, 'Datum']
            if isinstance(d, str):
                 try: d = datetime.strptime(d, DATE_FORMAT).date()
                 except: pass

            for s in slots:
                ma = row[f'{s}_Mitarbeiter']
                a_float = parse_user_time(row[f'{s}_Anfang'])
                e_float = parse_user_time(row[f'{s}_Ende'])
                p = row[f'{s}_Pause'] or 0.0
                
                if ma or a_float>0 or e_float>0:
                    if ma:
                        val, msg = validate_einsatz(df_uk, ma, d)
                        if not val: 
                            error_messages.append(f"{d.strftime('%d.%m.%Y')} - {ma}: {msg}")
                        
                        is_double, dbl_msg = check_double_booking(conn, ma, d, a_float, e_float, obj)
                        if is_double:
                             error_messages.append(f"{d.strftime('%d.%m.%Y')} - {ma}: ❌ {dbl_msg}")
                    
                    t = calculate_arbeitszeit(a_float, e_float, p)
                    total += t
                    rows.append({'Datum':d.strftime(DATE_FORMAT), 'Objekt':obj, 'MA_Slot':s, 'Anfang':a_float, 'Ende':e_float, 'Pause':p, 'Mitarbeiter':ma, 'Zeit':t})
        
        if error_messages:
            for err in error_messages:
                st.error(err)
            st.warning("❌ Speichern abgebrochen aufgrund von Konflikten. Bitte korrigieren.")
        else:
            try:
                if rows: save_einsaetze_to_db(conn, pd.DataFrame(rows), obj, d_start, d_end); st.success(f"Gespeichert! Total: {format_duration_str(total)}")
                else: st.info("Keine Einträge im aktuellen Monat.")
                time.sleep(1); st.rerun()
            except Exception as e: st.error(f"Fehler: {e}")
    
    # --- MONATSAUSWERTUNG ---
    mask = (df_saved['Datum'] >= d_start) & (df_saved['Datum'] <= d_end)
    df_period = df_saved.loc[mask]
    if not df_period.empty:
        sums = df_period.groupby('MA_Slot')['Zeit'].sum().reindex(slots, fill_value=0.0)
        sums_formatted = sums.apply(format_duration_str)
        df_sum = pd.DataFrame([sums_formatted.values], columns=[to_bold(s) for s in slots])
        st.markdown("#### Monatsauswertung")
        st.dataframe(df_sum, use_container_width=True, hide_index=True)

# --- NEUE FUNKTION: Aggregierte Daten für Auswertung laden ---
@st.cache_data(ttl=5)
def load_aggregated_data(_conn, selected_month_str):
    """Lädt Einsätze und Urlaubstage und kombiniert sie für die Auswertung."""
    # 1. Einsätze
    query_einsaetze = "SELECT Mitarbeiter, Zeit, Datum, Objekt, MA_Slot FROM einsaetze WHERE Mitarbeiter != ''"
    df_einsaetze = pd.read_sql(query_einsaetze, _conn)
    # 2. Urlaub/Krank
    query_uk = "SELECT Mitarbeiter, Status, Datum FROM urlaub_krank"
    df_uk = pd.read_sql(query_uk, _conn)
    
    # 3. Konvertieren
    for df in [df_einsaetze, df_uk]:
        if not df.empty:
            df['Datum'] = pd.to_datetime(df['Datum'])
            df['Monat'] = df['Datum'].dt.to_period('M').astype(str)
            
    # Filter
    df_e_monat = df_einsaetze[df_einsaetze['Monat'] == selected_month_str] if not df_einsaetze.empty else pd.DataFrame()
    df_uk_monat = df_uk[df_uk['Monat'] == selected_month_str] if not df_uk.empty else pd.DataFrame()
    
    return df_e_monat, df_uk_monat

def seite_mitarbeiter_uebersicht(conn):
    st.header("Auswertung")
    
    # Wir brauchen eine Liste aller verfügbaren Monate aus beiden Tabellen
    df_e_raw = pd.read_sql("SELECT Datum FROM einsaetze", conn)
    df_uk_raw = pd.read_sql("SELECT Datum FROM urlaub_krank", conn)
    
    all_dates = pd.concat([
        pd.to_datetime(df_e_raw['Datum']), 
        pd.to_datetime(df_uk_raw['Datum'])
    ]).dropna()
    
    if all_dates.empty:
        st.info("Keine Daten vorhanden."); return
        
    available_months = sorted(all_dates.dt.to_period('M').astype(str).unique(), reverse=True)
    monat = st.selectbox("Monat", available_months)
    
    # Lade kombinierte Daten
    df_work, df_absense = load_aggregated_data(conn, monat)
    
    # Aggregation Arbeitszeit
    res_work = pd.DataFrame()
    if not df_work.empty:
        # HIER DIE ÄNDERUNG: Group by Mitarbeiter UND Objekt/Slot um "Einteilung" zu erhalten
        # Wir wollen pro Mitarbeiter eine Liste der Einteilungen
        
        # 1. Stunden summieren
        res_work = df_work.groupby('Mitarbeiter')['Zeit'].sum().reset_index()
        res_work.rename(columns={'Zeit': 'Arbeitsstunden'}, inplace=True)
        res_work['Arbeitszeit_Format'] = res_work['Arbeitsstunden'].apply(format_duration_str)
        
        # 2. Einteilung (Objekt + Slot) aggregieren
        # Wir gruppieren nochmal, diesmal nur um die Strings zu sammeln
        einteilung = df_work.groupby('Mitarbeiter').apply(
            lambda x: ", ".join(sorted(set([f"{row['Objekt']} ({row['MA_Slot']})" for _, row in x.iterrows()])))
        ).reset_index(name='Einteilung')
        
        res_work = pd.merge(res_work, einteilung, on='Mitarbeiter')


    # Aggregation Abwesenheit (Zählen der Tage pro Status)
    res_absense = pd.DataFrame()
    if not df_absense.empty:
        # Pivot-Tabelle: Mitarbeiter als Zeilen, Status als Spalten, Werte sind Anzahl Tage
        res_absense = df_absense.groupby(['Mitarbeiter', 'Status']).size().unstack(fill_value=0).reset_index()
        
    # Zusammenführen (Full Outer Join, damit auch MA ohne Arbeit aber mit Urlaub erscheinen)
    if res_work.empty and res_absense.empty:
        st.warning("Keine Daten für diesen Monat.")
        return
        
    if not res_work.empty and not res_absense.empty:
        df_final = pd.merge(res_work, res_absense, on='Mitarbeiter', how='outer')
    elif not res_work.empty:
        df_final = res_work
    else:
        df_final = res_absense
        
    df_final = df_final.fillna(0)
    
    # Spalten sortieren und anzeigen
    st.subheader(f"Übersicht für {monat}")
    
    # Stelle sicher, dass Spalten existieren
    if 'Arbeitszeit_Format' not in df_final.columns: df_final['Arbeitszeit_Format'] = "0 Std 0 Min"
    if 'Einteilung' not in df_final.columns: df_final['Einteilung'] = "-"
    
    # Definiere Spaltenreihenfolge: Mitarbeiter | Einteilung | Zeit | ...Status...
    cols = ['Mitarbeiter', 'Einteilung', 'Arbeitszeit_Format']
    
    # Füge dynamische Status-Spalten hinzu (Urlaub, Krank...)
    status_cols = [c for c in df_final.columns if c not in ['Mitarbeiter', 'Arbeitsstunden', 'Arbeitszeit_Format', 'Einteilung']]
    cols.extend(status_cols)
    
    # DataFrame anzeigen
    st.dataframe(df_final[cols], use_container_width=True, hide_index=True)
    
    st.divider()
    
    # Detailansicht
    all_employees = df_final['Mitarbeiter'].unique()
    sel = st.selectbox("Details für Mitarbeiter:", all_employees)
    
    st.markdown(f"**Details für {sel}**")
    
    # Hole Details für Arbeit
    details_work = df_work[df_work['Mitarbeiter'] == sel].copy() if not df_work.empty else pd.DataFrame()
    if not details_work.empty:
        details_work['Typ'] = 'Arbeit'
        details_work['Info'] = details_work.apply(lambda x: f"{x['Objekt']} ({x['MA_Slot']})", axis=1)
        details_work['Dauer'] = details_work['Zeit'].apply(format_duration_str)
    
    # Hole Details für Abwesenheit
    details_absense = df_absense[df_absense['Mitarbeiter'] == sel].copy() if not df_absense.empty else pd.DataFrame()
    if not details_absense.empty:
        details_absense['Typ'] = details_absense['Status']
        details_absense['Info'] = '-'
        details_absense['Dauer'] = '1 Tag'
        
        # WICHTIG: Damit Urlaub nicht von 0-Stunden-Arbeit überschrieben wird, prüfen wir hier nicht weiter.
        # Aber im Combined DF (unten) werden beide angezeigt.
        # Wenn Sie wollen, dass Arbeitseinträge mit 0 Stunden NICHT angezeigt werden, filtern wir sie raus:
        details_work = details_work[details_work['Zeit'] > 0]
    
    # Kombinieren
    if not details_work.empty or not details_absense.empty:
        w_cols = details_work[['Datum', 'Typ', 'Info', 'Dauer']] if not details_work.empty else pd.DataFrame()
        a_cols = details_absense[['Datum', 'Typ', 'Info', 'Dauer']] if not details_absense.empty else pd.DataFrame()
        
        df_combined_details = pd.concat([w_cols, a_cols])
        
        if not df_combined_details.empty:
            df_combined_details = df_combined_details.sort_values('Datum')
            df_combined_details['Datum'] = df_combined_details['Datum'].dt.strftime('%d.%m.%Y')
            st.dataframe(df_combined_details, use_container_width=True, hide_index=True)
        else:
             st.info("Keine Details verfügbar.")
    else:
        st.info("Keine Details verfügbar.")

# --- MAIN ---

# LOGIN CHECK START
if check_login():
    conn = get_db_connection()
    if os.path.exists(LOGO_PATH):
        st.sidebar.image(LOGO_PATH, use_container_width=True)
    st.sidebar.write(f"Angemeldet als: **{st.session_state.get('username', 'User')}**")
    if st.sidebar.button("Logout"):
        logout()

    MA_LIST = [""] + load_data_from_db(conn, 'mitarbeiter_verzeichnis')['Mitarbeitername'].unique().tolist()
    pg = st.sidebar.radio("Menü", ["Einsatzplanung", "Auswertung", "Stammdaten"])
    if pg == "Einsatzplanung": seite_einsatzplanung(conn, load_data_from_db(conn, 'locations_spalte'), load_data_from_db(conn, 'urlaub_krank'), MA_LIST)
    elif pg == "Auswertung": seite_mitarbeiter_uebersicht(conn)
    elif pg == "Stammdaten": seite_stammdaten_verwaltung(conn)
