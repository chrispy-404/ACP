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
GERMAN_MONTHS = ["Januar", "Februar", "März", "April", "Mai", "Juni", "Juli", "August", "September", "Oktober", "November", "Dezember"]
LOGO_PATH = 'acp_logo.png'

# --- BENUTZER & PASSWÖRTER (Admins) ---
ADMIN_CREDENTIALS = {
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
        conn = mysql.connector.connect(**st.secrets["mysql"])
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

if 'db_initialized' not in st.session_state:
    try:
        init_db()
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

def safe_get_value(val):
    if isinstance(val, (pd.Series, np.ndarray, list)):
        if len(val) > 0:
            if hasattr(val, 'iloc'): return val.iloc[0]
            if hasattr(val, 'item'): return val.item()
            return val[0]
        return None
    return val

def parse_user_time(val_str):
    val_str = safe_get_value(val_str)
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
    mitarbeiter_name = safe_get_value(mitarbeiter_name)
    if not mitarbeiter_name: return True, "OK"

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
    mitarbeiter = safe_get_value(mitarbeiter)
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
                ma = safe_get_value(row['Mitarbeiter'])
                data.append((row['Datum'], row['Objekt'], row['MA_Slot'], row['Anfang'], row['Ende'], row['Pause'], ma, row['Zeit']))
            
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
        st.session_state['role'] = None # 'admin' or 'mitarbeiter'
        st.session_state['username'] = None

    if not st.session_state['logged_in']:
        col1, col2, col3 = st.columns([1,1,1])
        with col2:
            if os.path.exists(LOGO_PATH):
                st.image(LOGO_PATH, use_container_width=True)
            st.header("Anmeldung")
            username = st.text_input("Benutzername", placeholder="Name (Admin oder Mitarbeiter)")
            password = st.text_input("Passwort", type="password", placeholder="Passwort oder Personalnummer")
            
            if st.button("Einloggen", type="primary"):
                # 1. Check Admin
                if username in ADMIN_CREDENTIALS and ADMIN_CREDENTIALS[username] == password:
                    st.session_state['logged_in'] = True
                    st.session_state['username'] = username
                    st.session_state['role'] = 'admin'
                    st.rerun()
                
                # 2. Check Mitarbeiter (DB)
                else:
                    try:
                        conn = get_db_connection()
                        cursor = conn.cursor()
                        # Suche Mitarbeiter mit diesem Namen
                        query = "SELECT Personalnummer FROM mitarbeiter_verzeichnis WHERE Mitarbeitername = %s"
                        cursor.execute(query, (username,))
                        result = cursor.fetchone()
                        cursor.close()
                        conn.close()

                        if result:
                            stored_pnr = result[0]
                            # Prüfe ob Personalnummer übereinstimmt (und nicht leer ist)
                            if stored_pnr and str(stored_pnr).strip() == str(password).strip():
                                st.session_state['logged_in'] = True
                                st.session_state['username'] = username
                                st.session_state['role'] = 'mitarbeiter'
                                st.rerun()
                            else:
                                st.error("Falsches Passwort (Personalnummer).")
                        else:
                            st.error("Benutzer nicht gefunden.")
                    except Exception as e:
                        st.error(f"Login Fehler: {e}")

        return False
    return True

def logout():
    st.session_state['logged_in'] = False
    st.session_state['role'] = None
    st.session_state['username'] = None
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
        st.info("ℹ️ Die Personalnummer ist das Passwort für den Mitarbeiter-Login!")
        c1,c2 = st.columns(2)
        geb=c1.date_input("Geburtsdatum", value=date(2000,1,1)); pnr=c1.text_input("Personalnr (Passwort) *"); anst=c1.text_input("Anstellung"); pos=c1.text_input("Position"); tel=c1.text_input("Tel")
        bid=c2.text_input("Bewacher ID"); vbis=c2.date_input("Vertrag bis", value=date(2026,1,1)); abis=c2.date_input("Ausweis bis", value=date(2027,1,1))
        adr=st.text_input("Adresse"); plz=st.text_input("PLZ")
        if st.form_submit_button("Speichern", type="primary"):
            if name and pnr:
                cursor = conn.cursor()
                try:
                    cursor.execute("INSERT INTO mitarbeiter_verzeichnis (Mitarbeitername, Geburtsdatum, Personalnummer, Bewacher_ID, Anstellung, Position, Vertrag_bis, Adresse, PLZ, Telefonnummer, Ausweis_gueltig_bis) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                                     (name, geb.strftime(DATE_FORMAT), pnr, bid, anst, pos, vbis.strftime(DATE_FORMAT), adr, plz, tel, abis.strftime(DATE_FORMAT)))
                    conn.commit(); st.success("OK"); load_data_from_db.clear(); time.sleep(0.5); st.rerun()
                except Exception as e: conn.rollback(); st.error(str(e))
                finally: cursor.close()
            else:
                st.error("Name und Personalnummer sind Pflicht!")

@st.dialog("Mitarbeiter bearbeiten")
def dialog_edit_mitarbeiter(conn, row):
    with st.form("edit_ma"):
        name = st.text_input("Name", value=row['Mitarbeitername'])
        def d(x): 
            try: return pd.to_datetime(x).date()
            except: return date(2000,1,1)
        c1,c2 = st.columns(2)
        geb=c1.date_input("Geb", value=d(row['Geburtsdatum'])); pnr=c1.text_input("Pnr (Passwort)", value=row['Personalnummer'] or ""); anst=c1.text_input("Anst", value=row['Anstellung'] or ""); pos=c1.text_input("Pos", value=row['Position'] or ""); tel=c1.text_input("Tel", value=row['Telefonnummer'] or "")
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

# --- NEUE SEITE: MEIN PLAN (Für Mitarbeiter) ---
def seite_mein_plan(conn, username):
    st.header(f"👋 Hallo {username}")
    st.subheader("Mein Einsatzplan")
    
    today = date.today()
    current_month_str = today.strftime("%B %Y")
    next_month_date = today + relativedelta(months=1)
    next_month_str = next_month_date.strftime("%B %Y")
    
    selected_month_disp = st.selectbox("Monat wählen:", [current_month_str, next_month_str])
    
    if selected_month_disp == current_month_str:
        calc_date = today
    else:
        calc_date = next_month_date
        
    start_date = date(calc_date.year, calc_date.month, 1)
    end_date = (pd.to_datetime(start_date) + relativedelta(months=1, days=-1)).date()
    
    query = """
        SELECT Datum, Objekt, MA_Slot, Anfang, Ende, Pause, Zeit 
        FROM einsaetze 
        WHERE Mitarbeiter = %s AND Datum >= %s AND Datum <= %s
        ORDER BY Datum
    """
    df = pd.read_sql(query, conn, params=(username, start_date, end_date))
    
    if not df.empty:
        df['Datum'] = pd.to_datetime(df['Datum'])
        df['Wochentag'] = df['Datum'].apply(lambda x: GERMAN_WEEKDAYS[x.weekday()])
        df['Datum'] = df['Datum'].dt.strftime('%d.%m.%Y')
        df['Von'] = df['Anfang'].apply(float_to_input_str)
        df['Bis'] = df['Ende'].apply(float_to_input_str)
        df['Stunden'] = df['Zeit'].apply(format_duration_str)
        
        display_df = df[['Datum', 'Wochentag', 'Objekt', 'MA_Slot', 'Von', 'Bis', 'Stunden']]
        display_df.columns = ['Datum', 'Tag', 'Objekt', 'Position', 'Von', 'Bis', 'Dauer']
        
        st.table(display_df)
        total_hours = df['Zeit'].sum()
        st.info(f"Gesamtstunden in diesem Monat: **{format_duration_str(total_hours)}**")
    else:
        st.info("Keine Einsätze in diesem Monat gefunden.")

# --- SEITEN (Admin) ---
def seite_stammdaten_verwaltung(conn):
    st.header("Stammdatenverwaltung")
    t1, t2, t3 = st.tabs(["Mitarbeiter", "Standorte", "Urlaub/Krank"])
    df_ma = load_data_from_db(conn, 'mitarbeiter_verzeichnis')
    df_loc = load_data_from_db(conn, 'locations_spalte')
    
    if 'ma_editor_key' not in st.session_state: st.session_state.ma_editor_key = 0
    if 'loc_editor_key' not in st.session_state: st.session_state.loc_editor_key = 0
    
    with t1:
        if st.button("➕ Neuer Mitarbeiter"): dialog_neuer_mitarbeiter(conn)
        search = st.text_input("Suche:", placeholder="Name...")
        df_show = df_ma.drop(columns=['ID']).copy()
        df_show.insert(0, "Auswahl", False)
        if search: df_show = df_show[df_show['Mitarbeitername'].str.contains(search, case=False, na=False)]
        df_show = df_show.sort_values(by="Mitarbeitername")
        col_config = {
            "Auswahl": st.column_config.CheckboxColumn("Edit", width="small"),
            "Mitarbeitername": st.column_config.TextColumn("Name", width="medium"),
            "Personalnummer": st.column_config.TextColumn("P-Nr (Login)", width="small")
        }
        edited_df = st.data_editor(df_show, column_config=col_config, disabled=df_show.columns.drop("Auswahl"), hide_index=True, use_container_width=True, key=f"editor_ma_{st.session_state.ma_editor_key}")
        selected_rows = edited_df[edited_df["Auswahl"]]
        if not selected_rows.empty:
            row = selected_rows.iloc[0]
            dialog_edit_mitarbeiter(conn, row)

    with t2:
        if st.button("➕ Neuer Standort"): dialog_neuer_standort(conn)
        with st.expander("⚠️ Verwaltungs-Tools (Datenbank bereinigen)"):
            st.warning("Achtung: Dies löscht alle Standorte aus der Datenbank!")
            if st.button("Alle Standorte löschen", type="primary"):
                cursor = conn.cursor()
                try:
                    cursor.execute(f"DELETE FROM locations_spalte")
                    conn.commit()
                    st.success("Alle Standorte wurden entfernt.")
                    time.sleep(1)
                    st.rerun()
                except Exception as e: st.error(str(e))
                finally: cursor.close()
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
            edited_grp = st.data_editor(df_grp, column_config=col_config_loc, disabled=df_grp.columns.drop("Auswahl"), hide_index=True, use_container_width=True, height=1000, key=f"editor_loc_{st.session_state.loc_editor_key}")
            sel_loc = edited_grp[edited_grp["Auswahl"]]
            if not sel_loc.empty:
                r = sel_loc.iloc[0]
                dialog_edit_standort(conn, r[OBJECT_COLUMN_NAME], r[MA_SLOT_COLUMN_NAME], r['Ansprechpartner'], r['Telefon'])
    
    with t3:
        with st.form("uk"):
            c1,c2,c3=st.columns(3)
            dates = c1.date_input("Zeitraum", [], help="Start & Ende wählen")
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

    slots = sorted(list(set(df_loc[df_loc[OBJECT_COLUMN_NAME]==obj][MA_SLOT_COLUMN_NAME].tolist())))
    df_saved = load_einsaetze_for_object(conn, obj)
    rng = pd.date_range(d_start, d_end).normalize().date
    df_plan = pd.DataFrame({'Datum': rng})
    df_plan['Datum_Tag'] = df_plan['Datum'].apply(lambda d: f"{'🟥 ' if d.weekday() >= 5 else ''}{d.strftime('%d.%m.%Y')} ({GERMAN_WEEKDAYS[d.weekday()]})")
    
    col_cfg = {"Datum": None} 
    col_cfg['Datum_Tag'] = st.column_config.TextColumn(label="Datum", width="medium", disabled=True)

    ordered_cols = ['Datum', 'Datum_Tag']
    for slot in slots:
        df_s = df_saved[(df_saved['MA_Slot']==slot) & (df_saved['Datum'] >= d_start) & (df_saved['Datum'] <= d_end)]
        df_s = df_s.rename(columns={'Anfang':f'{slot}_Anfang','Ende':f'{slot}_Ende','Pause':f'{slot}_Pause','Mitarbeiter':f'{slot}_Mitarbeiter', 'Zeit':f'{slot}_Zeit'})
        
        has_content = False
        if not df_s.empty:
            if df_s[f'{slot}_Mitarbeiter'].notna().any() or (df_s[f'{slot}_Anfang'] > 0).any(): has_content = True
            df_s[f'{slot}_Anfang'] = df_s[f'{slot}_Anfang'].apply(float_to_input_str)
            df_s[f'{slot}_Ende'] = df_s[f'{slot}_Ende'].apply(float_to_input_str)
            df_plan = df_plan.merge(df_s[['Datum', f'{slot}_Mitarbeiter', f'{slot}_Anfang', f'{slot}_Ende', f'{slot}_Pause', f'{slot}_Zeit']], on='Datum', how='left')
        else:
            for c in [f'{slot}_Mitarbeiter', f'{slot}_Anfang', f'{slot}_Ende', f'{slot}_Pause', f'{slot}_Zeit']: df_plan[c] = None
        
        bold_header = to_bold(slot)
        width_ma = None if has_content else "small" 
        width_time = "small"
        col_cfg[f'{slot}_Mitarbeiter'] = st.column_config.SelectboxColumn(bold_header, options=MA_LIST, width=width_ma)
        col_cfg[f'{slot}_Anfang'] = st.column_config.TextColumn("Von", width=width_time, help="18, 18:30")
        col_cfg[f'{slot}_Ende'] = st.column_config.TextColumn("Bis", width=width_time)
        col_cfg[f'{slot}_Pause'] = st.column_config.NumberColumn("Pause", format="%.1f", min_value=0.0, max_value=24.0, width="small")
        col_cfg[f'{slot}_Zeit'] = st.column_config.NumberColumn("Zeit", format="%.2f", width="small", disabled=True)
        ordered_cols.extend([f'{slot}_Mitarbeiter', f'{slot}_Anfang', f'{slot}_Ende', f'{slot}_Pause', f'{slot}_Zeit'])

    st.markdown("---")
    with st.form("planning_form"):
        edited = st.data_editor(df_plan[ordered_cols], column_config=col_cfg, height=700, use_container_width=True, hide_index=True)
        submit_btn = st.form_submit_button("💾 Plan Speichern", type="primary")
    
    # CSV Export Button for German Excel
    st.markdown("### Export")
    csv_data = df_plan.to_csv(sep=';', index=False, encoding='utf-8-sig')
    st.download_button(
        label="📥 Download als CSV für Excel",
        data=csv_data,
        file_name=f"Einsatzplan_{obj}_{selected_month_str}.csv",
        mime="text/csv"
    )
    
    error_messages = []
    if submit_btn:
        load_einsaetze_for_object.clear(); rows=[]; total=0.0
        for idx, row in edited.iterrows():
            d = df_plan.loc[idx, 'Datum']
            if isinstance(d, str):
                 try: d = datetime.strptime(d, DATE_FORMAT).date()
                 except: pass
            for s in slots:
                ma = safe_get_value(row[f'{s}_Mitarbeiter'])
                a_float = parse_user_time(row[f'{s}_Anfang'])
                e_float = parse_user_time(row[f'{s}_Ende'])
                p = safe_get_value(row[f'{s}_Pause']) or 0.0
                if ma or a_float>0 or e_float>0:
                    if ma:
                        val, msg = validate_einsatz(df_uk, ma, d)
                        if not val: error_messages.append(f"{d.strftime('%d.%m.%Y')} - {ma}: {msg}")
                        is_double, dbl_msg = check_double_booking(conn, ma, d, a_float, e_float, obj)
                        if is_double: error_messages.append(f"{d.strftime('%d.%m.%Y')} - {ma}: ❌ {dbl_msg}")
                    t = calculate_arbeitszeit(a_float, e_float, p)
                    total += t
                    rows.append({'Datum':d.strftime(DATE_FORMAT), 'Objekt':obj, 'MA_Slot':s, 'Anfang':a_float, 'Ende':e_float, 'Pause':p, 'Mitarbeiter':ma, 'Zeit':t})
        if error_messages:
            for err in error_messages: st.error(err)
            st.warning("❌ Speichern abgebrochen aufgrund von Konflikten. Bitte korrigieren.")
        else:
            try:
                if rows: save_einsaetze_to_db(conn, pd.DataFrame(rows), obj, d_start, d_end); st.success(f"Gespeichert! Total: {format_duration_str(total)}")
                else: st.info("Keine Einträge im aktuellen Monat.")
                time.sleep(1); st.rerun()
            except Exception as e: st.error(f"Fehler: {e}")
    
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
    query_einsaetze = "SELECT Mitarbeiter, Zeit, Datum, Objekt, MA_Slot, Anfang, Ende, Pause FROM einsaetze WHERE Mitarbeiter != ''"
    df_einsaetze = pd.read_sql(query_einsaetze, _conn)
    query_uk = "SELECT Mitarbeiter, Status, Datum FROM urlaub_krank"
    df_uk = pd.read_sql(query_uk, _conn)
    for df in [df_einsaetze, df_uk]:
        if not df.empty:
            df['Datum'] = pd.to_datetime(df['Datum'])
            df['Monat'] = df['Datum'].dt.to_period('M').astype(str)
    df_e_monat = df_einsaetze[df_einsaetze['Monat'] == selected_month_str] if not df_einsaetze.empty else pd.DataFrame()
    df_uk_monat = df_uk[df_uk['Monat'] == selected_month_str] if not df_uk.empty else pd.DataFrame()
    return df_e_monat, df_uk_monat

def format_month_display(yyyy_mm):
    y, m = yyyy_mm.split('-')
    return f"{GERMAN_MONTHS[int(m)-1]} {y}"

def seite_mitarbeiter_uebersicht(conn):
    st.header("Auswertung")
    df_e_raw = pd.read_sql("SELECT Datum FROM einsaetze", conn)
    df_uk_raw = pd.read_sql("SELECT Datum FROM urlaub_krank", conn)
    all_dates = pd.concat([pd.to_datetime(df_e_raw['Datum']), pd.to_datetime(df_uk_raw['Datum'])]).dropna()
    if all_dates.empty: st.info("Keine Daten vorhanden."); return
    
    available_months = sorted(all_dates.dt.to_period('M').astype(str).unique(), reverse=True)
    monat = st.selectbox("Monat", available_months, format_func=format_month_display)
    
    df_work, df_absense = load_aggregated_data(conn, monat)
    
    # Vorbereitung der Daten für detaillierte Übersicht (immer berechnen)
    y_str, m_str = monat.split('-')
    year = int(y_str)
    month = int(m_str)
    start_date = date(year, month, 1)
    end_date = (pd.to_datetime(start_date) + relativedelta(months=1, days=-1)).date()
    date_rng = pd.date_range(start_date, end_date)
    
    all_employees = sorted(load_data_from_db(conn, 'mitarbeiter_verzeichnis')['Mitarbeitername'].unique().tolist())
    
    # Cross Join: Alle Tage x Alle Mitarbeiter
    df_dates = pd.DataFrame({'Datum_Obj': date_rng})
    df_dates['Datum'] = df_dates['Datum_Obj'].dt.strftime('%Y-%m-%d')
    df_dates['Wochentag'] = df_dates['Datum_Obj'].apply(lambda x: GERMAN_WEEKDAYS[x.weekday()])
    df_users = pd.DataFrame({'Mitarbeiter': all_employees})
    df_full = df_dates.merge(df_users, how='cross')
    
    # Daten vorbereiten
    if not df_work.empty:
        df_work_merge = df_work.copy()
        df_work_merge['Datum'] = df_work_merge['Datum'].dt.strftime('%Y-%m-%d')
        df_work_merge = df_work_merge[['Datum', 'Mitarbeiter', 'Objekt', 'MA_Slot', 'Anfang', 'Ende', 'Pause', 'Zeit']]
    else:
        df_work_merge = pd.DataFrame(columns=['Datum', 'Mitarbeiter', 'Objekt', 'MA_Slot', 'Anfang', 'Ende', 'Pause', 'Zeit'])
        
    if not df_absense.empty:
        df_absense_merge = df_absense.copy()
        df_absense_merge['Datum'] = df_absense_merge['Datum'].dt.strftime('%Y-%m-%d')
        df_absense_merge = df_absense_merge[['Datum', 'Mitarbeiter', 'Status']]
    else:
        df_absense_merge = pd.DataFrame(columns=['Datum', 'Mitarbeiter', 'Status'])
    
    # Mergen
    df_merged = pd.merge(df_full, df_work_merge, on=['Datum', 'Mitarbeiter'], how='left')
    df_merged = pd.merge(df_merged, df_absense_merge, on=['Datum', 'Mitarbeiter'], how='left')
    
    rows = []
    for _, row in df_merged.iterrows():
        d_obj = datetime.strptime(row['Datum'], '%Y-%m-%d')
        datum_display = d_obj.strftime('%d.%m.%Y')
        prefix = "🟥 " if d_obj.weekday() >= 5 else ""
        datum_full = f"{prefix}{datum_display} ({row['Wochentag']})"
        
        if pd.notna(row['Objekt']):
            typ = "Arbeit"
            einteilung = f"{row['Objekt']} ({row['MA_Slot']})"
            von = float_to_input_str(row['Anfang'])
            bis = float_to_input_str(row['Ende'])
            pause = f"{row['Pause']:.2f}" if pd.notna(row['Pause']) else "-"
            dauer = format_duration_str(row['Zeit'])
        elif pd.notna(row['Status']):
            typ = row['Status']
            einteilung = "-"
            von = "-"
            bis = "-"
            pause = "-"
            dauer = "Tag"
        else:
            typ = "-"
            einteilung = "-"
            von = "-"
            bis = "-"
            pause = "-"
            dauer = "-"
        
        rows.append({
            "Datum": row['Datum_Obj'], 
            "Datum_Anzeige": datum_full,
            "Name": row['Mitarbeiter'],
            "Typ": typ,
            "Einteilung": einteilung,
            "Von": von,
            "Bis": bis,
            "Pause": pause,
            "Dauer": dauer
        })
        
    df_all = pd.DataFrame(rows)
    df_all = df_all.sort_values(by=['Datum', 'Name'])

    st.subheader(f"Übersicht für {format_month_display(monat)}")
    
    # Filter-Optionen für die Hauptansicht
    c_filter1, c_filter2 = st.columns(2)
    sel_ma = c_filter1.selectbox("Mitarbeiter filtern:", ["ALLE MITARBEITER"] + all_employees)
    filter_free = c_filter2.checkbox("Nur Verfügbare (-) anzeigen")
    
    # Filtern
    df_view = df_all.copy()
    if sel_ma != "ALLE MITARBEITER":
        df_view = df_view[df_view['Name'] == sel_ma]
    
    if filter_free:
        df_view = df_view[df_view['Typ'] == "-"]

    st.dataframe(
        df_view[['Datum_Anzeige', 'Name', 'Typ', 'Einteilung', 'Von', 'Bis', 'Pause', 'Dauer']], 
        hide_index=True, 
        use_container_width=True,
        column_config={"Datum_Anzeige": st.column_config.TextColumn("Datum")}
    )
    
    st.divider()
    
    # Kleine Statistik-Tabelle (ehemals Hauptansicht)
    with st.expander("Statistik (Stunden & Abwesenheit)"):
        # Aggregation Arbeitszeit
        res_work = pd.DataFrame()
        if not df_work.empty:
            res_work = df_work.groupby('Mitarbeiter')['Zeit'].sum().reset_index()
            res_work.rename(columns={'Zeit': 'Arbeitsstunden'}, inplace=True)
            res_work['Arbeitszeit_Format'] = res_work['Arbeitsstunden'].apply(format_duration_str)

        res_absense = pd.DataFrame()
        if not df_absense.empty:
            res_absense = df_absense.groupby(['Mitarbeiter', 'Status']).size().unstack(fill_value=0).reset_index()
            
        if not res_work.empty or not res_absense.empty:
            if not res_work.empty and not res_absense.empty:
                df_final = pd.merge(res_work, res_absense, on='Mitarbeiter', how='outer')
            elif not res_work.empty:
                df_final = res_work
            else:
                df_final = res_absense
            df_final = df_final.fillna(0)
            
            if 'Arbeitszeit_Format' not in df_final.columns: df_final['Arbeitszeit_Format'] = "0 Std 0 Min"
            cols = ['Mitarbeiter', 'Arbeitszeit_Format']
            status_cols = [c for c in df_final.columns if c not in ['Mitarbeiter', 'Arbeitsstunden', 'Arbeitszeit_Format']]
            cols.extend(status_cols)
            
            st.dataframe(df_final[cols], use_container_width=True, hide_index=True)
        else:
            st.info("Keine Daten für Statistik.")

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
    
    # ROLLE PRÜFEN
    role = st.session_state.get('role', 'mitarbeiter') # Fallback to mitarbeiter if undefined
    
    if role == 'admin':
        pg = st.sidebar.radio("Menü", ["Einsatzplanung", "Auswertung", "Stammdaten"])
        if pg == "Einsatzplanung": seite_einsatzplanung(conn, load_data_from_db(conn, 'locations_spalte'), load_data_from_db(conn, 'urlaub_krank'), MA_LIST)
        elif pg == "Auswertung": seite_mitarbeiter_uebersicht(conn)
        elif pg == "Stammdaten": seite_stammdaten_verwaltung(conn)
    else:
        # Menü für normale Mitarbeiter
        st.sidebar.info("Eingeschränkte Ansicht für Mitarbeiter")
        seite_mein_plan(conn, st.session_state.get('username'))
