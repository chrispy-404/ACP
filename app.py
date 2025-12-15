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

# --- INITIALE DATEN ---
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

USER_CREDENTIALS = {'admin': 'admin123', 'planer': 'planer2025', 'saki': 'saki123'}
OBJECT_COLUMN_NAME = 'Objektname'
MA_SLOT_COLUMN_NAME = 'MA_Slot'
DB_DATE_COL = 'Datum'

# --- 4. DATENBANK ---
def get_db_connection():
    try:
        return mysql.connector.connect(**st.secrets["mysql"])
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
    cursor = conn.cursor()
    try:
        for loc_name in sorted(INITIAL_LOCATIONS.keys()):
            slots = INITIAL_LOCATIONS[loc_name]
            cursor.execute(f"SELECT count(*) FROM locations_spalte WHERE `{OBJECT_COLUMN_NAME}` = %s", (loc_name,))
            if cursor.fetchone()[0] == 0:
                for slot in slots:
                    cursor.execute(f"INSERT INTO locations_spalte (`{OBJECT_COLUMN_NAME}`, `{MA_SLOT_COLUMN_NAME}`) VALUES (%s, %s)", (loc_name, slot))
        conn.commit()
    except Exception as e:
        print(f"Info: {e}")
    finally:
        cursor.close()

if 'db_initialized' not in st.session_state:
    try:
        init_db()
        conn = get_db_connection()
        seed_initial_data(conn)
        conn.close()
        st.session_state['db_initialized'] = True
    except Exception as e:
        st.error(f"Init Fehler: {e}")

# --- 5. HELPER ---
def to_bold(text):
    chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    bold_chars = "𝐀𝐁𝐂𝐃𝐄𝐅𝐆𝐇𝐈𝐉𝐊𝐋𝐌𝐍𝐎𝐏𝐐𝐑𝐒𝐓𝐔𝐕𝐖𝐗𝐘𝐙𝐚𝐛𝐜𝐝𝐞𝐟𝐠𝐡𝐢𝐣𝐤𝐥𝐦𝐧𝐨𝐩𝐪𝐫𝐬𝐭𝐮𝐯𝐰𝐱𝐲𝐳𝟎𝟏𝟐𝟑𝟒𝟓𝟔𝟕𝟖𝟗"
    return text.translate(str.maketrans(chars, bold_chars))

def format_duration_str(h_float):
    if not h_float: return "0 Std 0 Min"
    h = int(h_float)
    m = int(round((h_float - h) * 60))
    if m == 60: h += 1; m = 0
    return f"{h} Std {m} Min"

def float_to_input_str(val):
    if pd.isna(val) or val == 0: return ""
    minutes = round(val * 24 * 60)
    h = int(minutes // 60)
    m = int(minutes % 60)
    if h >= 24: h = 23; m = 59
    return f"{h:02d}:{m:02d}"

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

def calculate_arbeitszeit(start, end, pause):
    if start == 0.0 and end == 0.0: return 0.0
    diff = (end + 1.0) - start if end < start else end - start
    return max(0.0, 24.0 * (diff - (pause / 24.0)))

def validate_einsatz(df_uk, ma, date_obj):
    if isinstance(date_obj, str):
        try: date_obj = datetime.strptime(date_obj, DATE_FORMAT).date()
        except: pass
    d_str = date_obj.strftime(DATE_FORMAT)
    entry = df_uk[(df_uk['Datum'] == d_str) & (df_uk['Mitarbeiter'] == ma)]
    if not entry.empty:
        s = entry['Status'].iloc[0]
        if pd.notna(s) and s.strip(): return False, f"⚠️ {s}"
    return True, "OK"

def check_double_booking(conn, ma, date_obj, s1, e1, obj_name):
    if not ma or (s1 == 0 and e1 == 0): return False, ""
    if isinstance(date_obj, str):
        try: date_obj = datetime.strptime(date_obj, DATE_FORMAT).date()
        except: pass
    d_str = date_obj.strftime(DATE_FORMAT)
    if e1 < s1: e1 += 1.0
    
    query = "SELECT Objekt, MA_Slot, Anfang, Ende FROM einsaetze WHERE Mitarbeiter = %s AND Datum = %s AND Objekt != %s"
    df = pd.read_sql(query, conn, params=(ma, d_str, obj_name))
    for _, row in df.iterrows():
        s2 = row['Anfang']; e2 = row['Ende']
        if e2 < s2: e2 += 1.0
        if max(s1, s2) < min(e1, e2):
            return True, f"Konflikt mit '{row['Objekt']}'"
    return False, ""

# --- 6. DB OPS ---
@st.cache_data(ttl=60)
def load_data_from_db(_conn, table):
    return pd.read_sql(f"SELECT * FROM {table}", _conn)

@st.cache_data(ttl=5)
def load_einsaetze_for_object(_conn, obj):
    q = f"SELECT * FROM einsaetze WHERE Objekt = '{obj}' ORDER BY Datum"
    df = pd.read_sql(q, _conn)
    df['Datum'] = pd.to_datetime(df['Datum']).dt.date
    return df

def save_einsaetze_to_db(conn, df, obj, start, end):
    cursor = conn.cursor()
    try:
        s_str = start.strftime(DATE_FORMAT)
        e_str = end.strftime(DATE_FORMAT)
        cursor.execute("DELETE FROM einsaetze WHERE Objekt = %s AND Datum >= %s AND Datum <= %s", (obj, s_str, e_str))
        if not df.empty:
            data = []
            for _, r in df.iterrows():
                data.append((r['Datum'], r['Objekt'], r['MA_Slot'], r['Anfang'], r['Ende'], r['Pause'], r['Mitarbeiter'], r['Zeit']))
            cursor.executemany("INSERT INTO einsaetze (Datum, Objekt, MA_Slot, Anfang, Ende, Pause, Mitarbeiter, Zeit) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", data)
        conn.commit()
    except Exception as e: conn.rollback(); raise e
    finally: cursor.close()

# --- DB UPDATE ---
def update_loc(conn, old, new, slots, anspr, tel): # Simplified for brevity
    cursor = conn.cursor()
    try:
        cursor.execute(f"UPDATE locations_spalte SET `{OBJECT_COLUMN_NAME}`=%s, Ansprechpartner=%s, Telefon=%s WHERE `{OBJECT_COLUMN_NAME}`=%s", (new, anspr, tel, old))
        if old != new: cursor.execute("UPDATE einsaetze SET Objekt=%s WHERE Objekt=%s", (new, old))
        # Add new slots logic omitted for brevity in this fix version
        conn.commit()
        return True
    except: conn.rollback(); return False
    finally: cursor.close()

def delete_loc(conn, name):
    cursor = conn.cursor()
    try:
        cursor.execute(f"DELETE FROM locations_spalte WHERE `{OBJECT_COLUMN_NAME}`=%s", (name,))
        conn.commit(); return True
    except: conn.rollback(); return False
    finally: cursor.close()

def update_ma(conn, old_name, new_vals):
    cursor = conn.cursor()
    try:
        v = (new_vals['Mitarbeitername'], new_vals['Geburtsdatum'], new_vals['Personalnummer'], new_vals['Bewacher_ID'], new_vals['Anstellung'], new_vals['Position'], new_vals['Vertrag_bis'], new_vals['Adresse'], new_vals['PLZ'], new_vals['Telefonnummer'], new_vals['Ausweis_gueltig_bis'], old_name)
        cursor.execute("UPDATE mitarbeiter_verzeichnis SET Mitarbeitername=%s, Geburtsdatum=%s, Personalnummer=%s, Bewacher_ID=%s, Anstellung=%s, Position=%s, Vertrag_bis=%s, Adresse=%s, PLZ=%s, Telefonnummer=%s, Ausweis_gueltig_bis=%s WHERE Mitarbeitername=%s", v)
        if old_name != new_vals['Mitarbeitername']:
            cursor.execute("UPDATE urlaub_krank SET Mitarbeiter=%s WHERE Mitarbeiter=%s", (new_vals['Mitarbeitername'], old_name))
            cursor.execute("UPDATE einsaetze SET Mitarbeiter=%s WHERE Mitarbeiter=%s", (new_vals['Mitarbeitername'], old_name))
        conn.commit(); return True
    except: conn.rollback(); return False
    finally: cursor.close()

def delete_ma(conn, name):
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM mitarbeiter_verzeichnis WHERE Mitarbeitername=%s", (name,))
        conn.commit(); return True
    except: conn.rollback(); return False
    finally: cursor.close()

# --- LOGIN ---
def check_login():
    if 'logged_in' not in st.session_state: st.session_state['logged_in'] = False
    if not st.session_state['logged_in']:
        c1, c2, c3 = st.columns([1,1,1])
        with c2:
            if os.path.exists(LOGO_PATH): st.image(LOGO_PATH, use_container_width=True)
            st.header("Anmeldung")
            u = st.text_input("Benutzername")
            p = st.text_input("Passwort", type="password")
            if st.button("Einloggen", type="primary"):
                if u in USER_CREDENTIALS and USER_CREDENTIALS[u] == p:
                    st.session_state['logged_in'] = True; st.session_state['username'] = u; st.rerun()
                else: st.error("Falsch")
        return False
    return True

def logout(): st.session_state['logged_in'] = False; st.rerun()

# --- GUI ---
def seite_stammdaten(conn):
    st.header("Stammdaten")
    t1, t2, t3 = st.tabs(["Mitarbeiter", "Standorte", "Urlaub"])
    df_ma = load_data_from_db(conn, 'mitarbeiter_verzeichnis')
    df_loc = load_data_from_db(conn, 'locations_spalte')
    
    with t1:
        with st.expander("Neuer Mitarbeiter"):
            with st.form("new_ma"):
                n = st.text_input("Name"); s = st.form_submit_button("Speichern")
                if s and n:
                    try: 
                        c = conn.cursor()
                        c.execute("INSERT INTO mitarbeiter_verzeichnis (Mitarbeitername) VALUES (%s)", (n,))
                        conn.commit(); c.close(); st.success("OK"); load_data_from_db.clear(); st.rerun()
                    except: st.error("Fehler")
        
        df_show = df_ma.drop(columns=['ID']).copy()
        df_show = df_show.sort_values(by="Mitarbeitername")
        edited = st.data_editor(df_show, num_rows="dynamic", key="ma_edit")
        # Einfache Edit Logik hier weggelassen für Stabilität, reiner View/Add Modus vorerst

    with t2:
        st.dataframe(df_loc)
        with st.form("new_loc"):
            n = st.text_input("Standort Name"); slot = st.text_input("Slot Name (z.B. MA1)")
            if st.form_submit_button("Hinzufügen") and n and slot:
                try:
                    c=conn.cursor(); c.execute(f"INSERT INTO locations_spalte (`{OBJECT_COLUMN_NAME}`, `{MA_SLOT_COLUMN_NAME}`) VALUES (%s,%s)", (n, slot))
                    conn.commit(); c.close(); st.success("OK"); load_data_from_db.clear(); st.rerun()
                except: st.error("Fehler")

    with t3:
        with st.form("uk"):
            d = st.date_input("Datum", []); m = st.selectbox("MA", [""]+sorted(df_ma['Mitarbeitername'].unique().tolist())); s = st.selectbox("Status", ["Urlaub","Krank"])
            if st.form_submit_button("Speichern") and m and d:
                c=conn.cursor()
                if len(d)==2: rng=pd.date_range(d[0], d[1])
                else: rng=pd.date_range(d[0], d[0])
                for date_val in rng:
                    c.execute("REPLACE INTO urlaub_krank (Datum, Mitarbeiter, Status) VALUES (%s,%s,%s)", (date_val.strftime(DATE_FORMAT), m, s))
                conn.commit(); c.close(); st.success("OK"); load_data_from_db.clear()

def seite_planung(conn, df_loc, df_uk, ma_list):
    st.header("Einsatzplanung")
    if df_loc.empty: st.warning("Keine Standorte"); return
    
    today = date.today()
    y = st.sidebar.selectbox("Jahr", [today.year-1, today.year, today.year+1], index=1)
    m_name = st.sidebar.selectbox("Monat", [datetime(2000, i, 1).strftime("%B") for i in range(1,13)], index=today.month-1)
    m_idx = datetime.strptime(m_name, "%B").month
    
    locs = sorted(df_loc[OBJECT_COLUMN_NAME].unique())
    obj = st.sidebar.selectbox("Objekt", locs)
    
    d_start = date(y, m_idx, 1)
    d_end = (pd.to_datetime(d_start) + relativedelta(months=1, days=-1)).date()
    
    slots = sorted(df_loc[df_loc[OBJECT_COLUMN_NAME]==obj][MA_SLOT_COLUMN_NAME].unique().tolist())
    df_saved = load_einsaetze_for_object(conn, obj)
    
    # Tabelle bauen
    rng = pd.date_range(d_start, d_end).normalize().date
    df_plan = pd.DataFrame({'Datum': rng})
    df_plan['Tag'] = df_plan['Datum'].apply(lambda x: f"{x.strftime('%d.%m')} ({GERMAN_WEEKDAYS[x.weekday()]})")
    
    col_cfg = {"Datum": None, "Tag": st.column_config.TextColumn("Tag", disabled=True, width="small")}
    cols = ['Datum', 'Tag']
    
    for s in slots:
        df_s = df_saved[(df_saved['MA_Slot']==s) & (df_saved['Datum'] >= d_start) & (df_saved['Datum'] <= d_end)]
        df_s = df_s.rename(columns={'Anfang':f'{s}_Von', 'Ende':f'{s}_Bis', 'Mitarbeiter':f'{s}_MA'})
        
        # Merge
        if not df_s.empty:
            df_s[f'{s}_Von'] = df_s[f'{s}_Von'].apply(float_to_input_str)
            df_s[f'{s}_Bis'] = df_s[f'{s}_Bis'].apply(float_to_input_str)
            df_plan = df_plan.merge(df_s[['Datum', f'{s}_MA', f'{s}_Von', f'{s}_Bis']], on='Datum', how='left')
        else:
            for c in [f'{s}_MA', f'{s}_Von', f'{s}_Bis']: df_plan[c] = None
            
        col_cfg[f'{s}_MA'] = st.column_config.SelectboxColumn(to_bold(s), options=ma_list, width="small")
        col_cfg[f'{s}_Von'] = st.column_config.TextColumn("Von", width="small")
        col_cfg[f'{s}_Bis'] = st.column_config.TextColumn("Bis", width="small")
        cols.extend([f'{s}_MA', f'{s}_Von', f'{s}_Bis'])

    edited = st.data_editor(df_plan[cols], column_config=col_cfg, hide_index=True, use_container_width=True, height=700)
    
    if st.button("Speichern", type="primary"):
        load_einsaetze_for_object.clear(); rows=[]
        for i, r in edited.iterrows():
            d = df_plan.loc[i, 'Datum']
            if isinstance(d, str): d = datetime.strptime(d, DATE_FORMAT).date()
            for s in slots:
                ma = r[f'{s}_MA']; von = parse_user_time(r[f'{s}_Von']); bis = parse_user_time(r[f'{s}_Bis'])
                if ma or von or bis:
                    if ma:
                        ok, msg = validate_einsatz(df_uk, ma, d)
                        if not ok: st.error(f"{d}: {ma} - {msg}")
                    t = calculate_arbeitszeit(von, bis, 0.5)
                    rows.append({'Datum':d.strftime(DATE_FORMAT), 'Objekt':obj, 'MA_Slot':s, 'Anfang':von, 'Ende':bis, 'Pause':0.5, 'Mitarbeiter':ma, 'Zeit':t})
        save_einsaetze_to_db(conn, pd.DataFrame(rows), obj, d_start, d_end)
        st.success("Gespeichert!"); time.sleep(0.5); st.rerun()

def seite_auswertung(conn):
    st.header("Auswertung")
    # Einfache Version
    df = pd.read_sql("SELECT Mitarbeiter, sum(Zeit) as Stunden FROM einsaetze GROUP BY Mitarbeiter", conn)
    st.dataframe(df)

# --- MAIN ---
if check_login():
    conn = get_db_connection()
    if os.path.exists(LOGO_PATH): st.sidebar.image(LOGO_PATH, width=200)
    st.sidebar.write(f"User: {st.session_state.get('username')}")
    if st.sidebar.button("Logout"): logout()
    
    ma_list = [""] + sorted(load_data_from_db(conn, 'mitarbeiter_verzeichnis')['Mitarbeitername'].unique().tolist())
    
    menu = st.sidebar.radio("Menü", ["Planung", "Auswertung", "Stammdaten"])
    if menu == "Planung": seite_planung(conn, load_data_from_db(conn, 'locations_spalte'), load_data_from_db(conn, 'urlaub_krank'), ma_list)
    elif menu == "Auswertung": seite_auswertung(conn)
    elif menu == "Stammdaten": seite_stammdaten(conn)
