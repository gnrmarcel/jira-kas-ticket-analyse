import streamlit as st
import pandas as pd
from jira import JIRA
import psycopg2
from datetime import datetime, timedelta

# Seite breit darstellen
st.set_page_config(layout="wide")

# ---- Secrets laden ----
jira_url = st.secrets["jira"]["url"]
jira_user = st.secrets["jira"]["user"]
jira_token = st.secrets["jira"]["token"]
pg_url = st.secrets["postgres"]["url"]

# ---- Jira-Client initialisieren ----
jira = JIRA(server=jira_url, basic_auth=(jira_user, jira_token))

# ---- PostgreSQL-Verbindung aufbauen ----
conn = psycopg2.connect(pg_url)
c = conn.cursor()

# ---- Tabellen erstellen, falls nicht vorhanden ----
c.execute('''CREATE TABLE IF NOT EXISTS issues
             (key TEXT PRIMARY KEY, created_date DATE, closed_date DATE, title TEXT, status TEXT, kas_category TEXT)''')
c.execute('''CREATE TABLE IF NOT EXISTS last_update
             (id SERIAL PRIMARY KEY, timestamp TIMESTAMP)''')
conn.commit()

# ---- Funktion: Daten aus Jira abrufen & speichern (BATCHES!) ----
def fetch_and_store_data():
    date_365_days_ago = datetime.now() - timedelta(days=365)
    jql_query = f'project=KAS AND created >= "{date_365_days_ago.strftime("%Y-%m-%d")}"'

    start_at = 0
    max_results = 100
    total = 1  # Dummywert für die Schleife

    while start_at < total:
        issues = jira.search_issues(jql_query, startAt=start_at, maxResults=max_results)
        total = issues.total
        for issue in issues:
            created_date = datetime.strptime(issue.fields.created, '%Y-%m-%dT%H:%M:%S.%f%z').date()
            closed_date = issue.fields.resolutiondate
            if closed_date:
                closed_date = datetime.strptime(closed_date, '%Y-%m-%dT%H:%M:%S.%f%z').date()
            else:
                closed_date = None
            title = issue.fields.summary
            status = issue.fields.status.name
            # Passe ggf. die customfield-ID an deine Jira-Instanz an!
            kas_category = ', '.join([option.value for option in getattr(issue.fields, 'customfield_10159', [])]) if getattr(issue.fields, 'customfield_10159', None) else None
            c.execute('''INSERT INTO issues (key, created_date, closed_date, title, status, kas_category) 
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (key) DO UPDATE 
                        SET created_date = EXCLUDED.created_date,
                            closed_date = EXCLUDED.closed_date,
                            title = EXCLUDED.title,
                            status = EXCLUDED.status,
                            kas_category = EXCLUDED.kas_category''', 
                    (issue.key, created_date, closed_date, title, status, kas_category))
        conn.commit()
        start_at += max_results

    last_update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    c.execute('''INSERT INTO last_update (timestamp) VALUES (%s)''', (last_update_time,))
    conn.commit()

# ---- Letzten Aktualisierungszeitpunkt abrufen ----
c.execute('''SELECT timestamp FROM last_update ORDER BY id DESC LIMIT 1''')
last_update_row = c.fetchone()
last_update = last_update_row[0] if last_update_row else 'Nie'

# ---- Button für Datenaktualisierung ----
if st.button('Aktualisieren'):
    with st.spinner("Daten werden von Jira geladen und gespeichert..."):
        fetch_and_store_data()
        st.success("Daten wurden erfolgreich aktualisiert.")
    st.rerun()

# ---- Auswahl der Tage ----
num_days = st.selectbox('Wähle die Anzahl der Tage', [7, 30, 90, 180, 365])

date_num_days_ago = datetime.now() - timedelta(days=num_days)
date_365_days_ago = datetime.now() - timedelta(days=365)
c.execute('''SELECT created_date, closed_date FROM issues WHERE created_date >= %s''', (date_365_days_ago,))
rows = c.fetchall()
tickets = [(row[0], row[1]) for row in rows]

days = [date_num_days_ago + timedelta(days=i) for i in range(num_days + 3)]

@st.cache_data
def calculate_ticket_data(days, tickets):
    data = []
    for day in days:
        day_date = day.date()
        open_tickets = sum(1 for created, closed in tickets if created <= day_date and (closed is None or closed > day_date))
        new_tickets = sum(1 for created, closed in tickets if created == day_date)
        closed_tickets = sum(1 for created, closed in tickets if closed == day_date)
        data.append((day_date, open_tickets, new_tickets, closed_tickets))
    return data

data = calculate_ticket_data(days, tickets)
df = pd.DataFrame(data, columns=['Datum', 'Offene Tickets', 'Neue Tickets', 'Geschlossene Tickets'])

# ---- Streamlit-UI ----
st.title(f'Ticket-Analyse ({num_days} Tage)')
st.write(f'Datenstand: {last_update}')

st.line_chart(df.set_index('Datum')[['Offene Tickets', 'Neue Tickets', 'Geschlossene Tickets']])

today = datetime.now().date()
open_tickets_today = sum(1 for created, closed in tickets if created <= today and (closed is None or closed > today))
new_tickets_today = sum(1 for created, closed in tickets if created == today)
closed_tickets_today = sum(1 for created, closed in tickets if closed == today)
st.write(f'**Heutiger Stand:** Offene Tickets: {open_tickets_today}, Neue Tickets: {new_tickets_today}, Geschlossene Tickets: {closed_tickets_today}')

# ---- KAS-Kategorien aus offenen Tickets ----
c.execute('''SELECT kas_category FROM issues 
             WHERE created_date <= %s AND (closed_date IS NULL OR closed_date > %s)''', (today, today))
open_tickets_kas_categories = c.fetchall()
kas_category_counts = {}
for row in open_tickets_kas_categories:
    kas_category = row[0]
    if kas_category:
        categories = kas_category.split(', ')
        for category in categories:
            kas_category_counts[category] = kas_category_counts.get(category, 0) + 1

st.write('**Betroffene KAS-Kategorien:**')
for category, count in kas_category_counts.items():
    st.write(f'{category}: {count}')

if kas_category_counts:
    kas_data = pd.DataFrame({'KAS-Kategorie': list(kas_category_counts.keys()), 'Anzahl der betroffenen Tickets': list(kas_category_counts.values())})
    st.bar_chart(kas_data.set_index('KAS-Kategorie'))

# ---- Offene Tickets anzeigen ----
c.execute('''SELECT key, title, status, kas_category FROM issues 
             WHERE closed_date IS NULL OR closed_date > %s
             ORDER BY created_date DESC''', (today,))
open_tickets = c.fetchall()
open_tickets_df = pd.DataFrame(open_tickets, columns=['Ticket-Nr.', 'Titel', 'Status', 'KAS-Kategorie'])
open_tickets_df['Ticket-Link'] = open_tickets_df['Ticket-Nr.'].apply(lambda x: f"{jira_url}/browse/{x}")

st.markdown(
    """
    <style>
    .dataframe th, .dataframe td {
        width: 150px;
    }
    </style>
    """,
    unsafe_allow_html=True
)

st.dataframe(
    data=open_tickets_df,
    column_config={
        "Ticket-Nr.": st.column_config.Column(
            "Ticket-Nr.",
            help="Klick auf die Ticket-Nummer führt direkt zum Ticket"
        ),
        "Status": st.column_config.Column(
            "Status",
            help="Aktueller Status des Tickets",
            width=350
        ),
        "KAS-Kategorie": st.column_config.Column(
            "KAS-Kategorie",
            help="Kategorie des Tickets",
            width=400
        ),
        "Ticket-Link": st.column_config.LinkColumn("Ticket-Link")
    },
    hide_index=True,
    height=len(open_tickets_df) * 35 + 38
)

# ---- Verbindung schließen ----
conn.close()
