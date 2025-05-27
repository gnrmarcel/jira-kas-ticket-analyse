import os
from jira import JIRA
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
import streamlit as st

# Streamlit-Anwendung konfigurieren
st.set_page_config(layout="wide")

# --- Zugriff auf die Secrets ---
jira_url = st.secrets["jira"]["url"]
jira_user = st.secrets["jira"]["user"]
jira_token = st.secrets["jira"]["token"]
pg_url = st.secrets["postgres"]["url"]

# --- Jira-Client initialisieren ---
jira = JIRA(server=jira_url, basic_auth=(jira_user, jira_token))

# --- Verbindung zur PostgreSQL-Datenbank herstellen ---
conn = psycopg2.connect(pg_url)
c = conn.cursor()

# Tabelle erstellen, falls sie nicht existiert
c.execute('''CREATE TABLE IF NOT EXISTS issues
             (key TEXT PRIMARY KEY, created_date DATE, closed_date DATE, title TEXT, status TEXT)''')

# Spalte "kas_category" hinzufügen, falls sie nicht existiert
c.execute('''ALTER TABLE issues ADD COLUMN IF NOT EXISTS kas_category TEXT''')

# Tabelle für den letzten Aktualisierungszeitpunkt erstellen, falls sie nicht existiert
c.execute('''CREATE TABLE IF NOT EXISTS last_update
             (id SERIAL PRIMARY KEY, timestamp TIMESTAMP)''')

# Funktion zum Abrufen und Speichern der Daten
def fetch_and_store_data():
    # Datum für die letzten 365 Tage berechnen
    date_365_days_ago = datetime.now() - timedelta(days=365)
    jql_query = f'project=KAS AND created >= "{date_365_days_ago.strftime("%Y-%m-%d")}"'

    # Abfrage für das KAS-Projekt der letzten 365 Tage
    issues = jira.search_issues(jql_query, maxResults=False)

    # Ausgabe der gefundenen Issues und Einfügen/Aktualisieren in der Datenbank
    for issue in issues:
        created_date = datetime.strptime(issue.fields.created, '%Y-%m-%dT%H:%M:%S.%f%z').date()
        closed_date = issue.fields.resolutiondate
        if closed_date:
            closed_date = datetime.strptime(closed_date, '%Y-%m-%dT%H:%M:%S.%f%z').date()
        else:
            closed_date = None
        title = issue.fields.summary
        status = issue.fields.status.name
        kas_category = ', '.join([option.value for option in issue.fields.customfield_10159]) if issue.fields.customfield_10159 else None  # 10159 = ID des benutzerdefinierten Feldes "Kategorie-KAS"
        c.execute('''INSERT INTO issues (key, created_date, closed_date, title, status, kas_category) 
                     VALUES (%s, %s, %s, %s, %s, %s)
                     ON CONFLICT (key) DO UPDATE 
                     SET created_date = EXCLUDED.created_date,
                         closed_date = EXCLUDED.closed_date,
                         title = EXCLUDED.title,
                         status = EXCLUDED.status,
                         kas_category = EXCLUDED.kas_category''', 
                  (issue.key, created_date, closed_date, title, status, kas_category))

    # Änderungen speichern
    conn.commit()

    # Aktualisierungszeitpunkt speichern
    last_update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    c.execute('''INSERT INTO last_update (timestamp) VALUES (%s)
                 ON CONFLICT (id) DO UPDATE 
                 SET timestamp = EXCLUDED.timestamp''', (last_update_time,))
    conn.commit()

# Letzten Aktualisierungszeitpunkt abrufen
c.execute('''SELECT timestamp FROM last_update ORDER BY id DESC LIMIT 1''')
last_update_row = c.fetchone()
if last_update_row:
    last_update = last_update_row[0]
else:
    last_update = 'Nie'

# Daten beim Start des Scripts abrufen und speichern
if st.button('Aktualisieren', key='fetch_data'):
    fetch_and_store_data()
    st.rerun()

# Auswahl der Anzahl der Tage
num_days = st.selectbox('Wählen Sie die Anzahl der Tage', [7, 30, 90, 180, 365])

# Datum für die letzten num_days Tage berechnen
date_num_days_ago = datetime.now() - timedelta(days=num_days)

# Abfrage der Daten aus der Datenbank für die letzten 365 Tage
date_365_days_ago = datetime.now() - timedelta(days=365)
c.execute('''SELECT created_date, closed_date FROM issues 
             WHERE created_date >= %s''', (date_365_days_ago,))
rows = c.fetchall()

# Daten in ein DataFrame umwandeln
tickets = [(row[0], row[1]) for row in rows]

# Liste der Tage innerhalb der letzten num_days Tage + 2 Tage in die Zukunft
days = [date_num_days_ago + timedelta(days=i) for i in range(num_days + 3)]

# Berechnung der offenen, neu erstellten und geschlossenen Tickets pro Tag
@st.cache_data
def calculate_ticket_data(days, tickets):
    data = []
    for day in days:
        day_date = day.date()  # Konvertiere datetime zu date
        open_tickets = sum(1 for created, closed in tickets if created <= day_date and (closed is None or closed > day_date))
        new_tickets = sum(1 for created, closed in tickets if created == day_date)
        closed_tickets = sum(1 for created, closed in tickets if closed == day_date)
        data.append((day_date, open_tickets, new_tickets, closed_tickets))
    return data

data = calculate_ticket_data(days, tickets)

# Daten in ein DataFrame umwandeln
df = pd.DataFrame(data, columns=['Datum', 'Offene Tickets', 'Neue Tickets', 'Geschlossene Tickets'])

# Streamlit-Anwendung
st.title(f'Ticket-Analyse ({num_days} Tage)')

# Anzeige des letzten Aktualisierungszeitpunkts
st.write(f'Datenstand: {last_update}')

# Liniendiagramm anzeigen
st.line_chart(df.set_index('Datum')[['Offene Tickets', 'Neue Tickets', 'Geschlossene Tickets']])

# Aktueller Stand von heute
today = datetime.now().date()
open_tickets_today = sum(1 for created, closed in tickets if created <= today and (closed is None or closed > today))
new_tickets_today = sum(1 for created, closed in tickets if created == today)
closed_tickets_today = sum(1 for created, closed in tickets if closed == today)
st.write(f'**Heutiger Stand:** Offene Tickets: {open_tickets_today}, Neue Tickets: {new_tickets_today}, Geschlossene Tickets: {closed_tickets_today}')

# Abfrage der offenen Tickets und deren KAS-Kategorien
c.execute('''SELECT kas_category FROM issues 
             WHERE created_date <= %s AND (closed_date IS NULL OR closed_date > %s)''', (today, today))
open_tickets_kas_categories = c.fetchall()

# KAS-Kategorien zusammenfassen und zählen
kas_category_counts = {}
for row in open_tickets_kas_categories:
    kas_category = row[0]
    if kas_category:
        categories = kas_category.split(', ')
        for category in categories:
            if category in kas_category_counts:
                kas_category_counts[category] += 1
            else:
                kas_category_counts[category] = 1

# Ausgabe der KAS-Kategorien und deren Häufigkeit
st.write('**Betroffene KAS-Kategorien:**')
for category, count in kas_category_counts.items():
    st.write(f'{category}: {count}')

# Diagramm für die KAS-Kategorien erstellen
if kas_category_counts:
    categories = list(kas_category_counts.keys())
    counts = list(kas_category_counts.values())

    # Daten in ein DataFrame umwandeln für die Darstellung
    kas_data = pd.DataFrame({'KAS-Kategorie': categories, 'Anzahl der betroffenen Tickets': counts})

    # Balkendiagramm in Streamlit anzeigen
    st.bar_chart(kas_data.set_index('KAS-Kategorie'))

# Aktuell offene Tickets anzeigen
c.execute('''SELECT key, title, status, kas_category FROM issues 
             WHERE closed_date IS NULL OR closed_date > %s
             ORDER BY created_date DESC''', (today,))
open_tickets = c.fetchall()

# DataFrame erstellen
open_tickets_df = pd.DataFrame(open_tickets, columns=['Ticket-Nr.', 'Titel', 'Status', 'KAS-Kategorie'])

# Ticket-URLs erstellen
open_tickets_df['Ticket-Link'] = open_tickets_df['Ticket-Nr.'].apply(lambda x: f"{jira_url}/browse/{x}")

# CSS für die Anpassung der Spaltenbreite hinzufügen
st.markdown(
    """
    <style>
    .dataframe th {
        width: 150px;  /* Breite für die Kopfzeilen */
    }
    .dataframe td {
        width: 150px;  /* Breite für die Zellen */
    }
    </style>
    """,
    unsafe_allow_html=True
)

# Sortierbare Tabelle mit benutzerdefinierten Spalten anzeigen
st.dataframe(
    data=open_tickets_df,
    column_config={
        "Ticket-Nr.": st.column_config.Column(
            "Ticket-Nr.",
            help="Klicken Sie auf die Ticket-Nummer, um zum Ticket zu gelangen"
        ),
        "Status": st.column_config.Column(
            "Status",
            help="Aktueller Status des Tickets",
            width=350  # Breite für die Spalte "Status"
        ),
        "KAS-Kategorie": st.column_config.Column(
            "KAS-Kategorie",
            help="Kategorie des Tickets",
            width=400  # Breite für die Spalte "KAS-Kategorie"
        ),
        "Ticket-Link": st.column_config.LinkColumn(
            "Ticket-Link"
        )
    },
    hide_index=True,
    height=len(open_tickets_df) * 35 + 38,  # Dynamische Höhe basierend auf Anzahl der Zeilen
)

# Verbindung schließen
conn.close()
