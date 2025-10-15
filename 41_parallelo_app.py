# 41° Parallelo — App personale gestione prenotazioni (Streamlit + SQLite)
# --------------------------------------------------------------
# Istruzioni rapide
# 1) Installa i pacchetti:  pip install streamlit pandas python-dateutil
# 2) Avvia l'app:           streamlit run 41_parallelo_app.py
#    (rinomina questo file in 41_parallelo_app.py se necessario)
# 3) Il database SQLite verrà creato come file locale "bookings_41_parallelo.db" nella stessa cartella.
# --------------------------------------------------------------

import sqlite3
from contextlib import closing
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from typing import List, Optional, Tuple

import pandas as pd
import streamlit as st

DB_PATH = "bookings_41_parallelo.db"

# ---------------------- DB LAYER ----------------------

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS bookings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    guest_name TEXT NOT NULL,
    email TEXT,
    phone TEXT,
    source TEXT DEFAULT 'Booking.com',          -- Diretta, Booking, Airbnb, Expedia, Altro
    room TEXT DEFAULT 'Camera 1',           -- nome/identificativo alloggio
    status TEXT DEFAULT 'Confermata',       -- Confermata, In attesa, Annullata
    check_in DATE NOT NULL,
    check_out DATE NOT NULL,
    guests INTEGER DEFAULT 1,
    price REAL DEFAULT 0,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_room_dates ON bookings(room, check_in, check_out)",
    "CREATE INDEX IF NOT EXISTS idx_status ON bookings(status)",
]


def get_conn():
    return sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)


def init_db():
    with closing(get_conn()) as conn, conn:
        conn.execute(CREATE_TABLE_SQL)
        for q in CREATE_INDEXES:
            conn.execute(q)


def insert_booking(data: dict) -> int:
    with closing(get_conn()) as conn, conn:
        cur = conn.execute(
            """
            INSERT INTO bookings(
                guest_name, email, phone, source, room, status,
                check_in, check_out, guests, price, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                data["guest_name"], data.get("email"), data.get("phone"), data.get("source"),
                data.get("room"), data.get("status"),
                data["check_in"], data["check_out"], data.get("guests", 1),
                data.get("price", 0.0), data.get("notes"),
            ),
        )
        return cur.lastrowid


def update_booking(booking_id: int, data: dict):
    keys = [
        "guest_name","email","phone","source","room","status",
        "check_in","check_out","guests","price","notes"
    ]
    sets = ", ".join([f"{k} = ?" for k in keys]) + ", updated_at = CURRENT_TIMESTAMP"
    values = [data.get(k) for k in keys]
    values.append(booking_id)
    with closing(get_conn()) as conn, conn:
        conn.execute(f"UPDATE bookings SET {sets} WHERE id = ?", values)


def delete_booking(booking_id: int):
    with closing(get_conn()) as conn, conn:
        conn.execute("DELETE FROM bookings WHERE id = ?", (booking_id,))


def fetch_bookings(
    start: Optional[date] = None,
    end: Optional[date] = None,
    status: Optional[str] = None,
    room: Optional[str] = None,
    search: Optional[str] = None,
) -> pd.DataFrame:
    query = "SELECT id, guest_name, email, phone, source, room, status, check_in, check_out, guests, price, notes, created_at, updated_at FROM bookings WHERE 1=1"
    params: List = []
    if start:
        query += " AND date(check_out) > date(?)"  # partenza dopo inizio periodo (sovrapposizione)
        params.append(start)
    if end:
        query += " AND date(check_in) < date(?)"   # arrivo prima di fine periodo
        params.append(end)
    if status and status != "Tutte":
        query += " AND status = ?"
        params.append(status)
    if room and room != "Tutte":
        query += " AND room = ?"
        params.append(room)
    if search:
        query += " AND (guest_name LIKE ? OR email LIKE ? OR phone LIKE ? OR notes LIKE ?)"
        like = f"%{search}%"
        params.extend([like, like, like, like])
    query += " ORDER BY check_in ASC"

    with closing(get_conn()) as conn:
        df = pd.read_sql_query(query, conn, params=params, parse_dates=["check_in","check_out","created_at","updated_at"])  # type: ignore
    return df


def rooms_list() -> List[str]:
    # Raccoglie i nomi stanze esistenti + alcune predefinite
    with closing(get_conn()) as conn:
        rows = conn.execute("SELECT DISTINCT room FROM bookings ORDER BY room").fetchall()
    existing = [r[0] for r in rows]
    defaults = ["Maestrale", "Libeccio", "Scirocco", "Grecale"]
    for d in defaults:
        if d not in existing:
            existing.append(d)
    return existing


def has_overlap(check_in: date, check_out: date, room: str, exclude_id: Optional[int] = None) -> Tuple[bool, pd.DataFrame]:
    # sovrapposizione: (A.start < B.end) and (A.end > B.start)
    query = """
        SELECT * FROM bookings
        WHERE room = ?
          AND status != 'Annullata'
          AND date(check_in) < date(?)
          AND date(check_out) > date(?)
    """
    params: List = [room, check_out, check_in]
    if exclude_id is not None:
        query += " AND id != ?"
        params.append(exclude_id)
    with closing(get_conn()) as conn:
        df = pd.read_sql_query(query, conn, params=params, parse_dates=["check_in","check_out","created_at","updated_at"])  # type: ignore
    return (len(df) > 0, df)

# ---------------------- UI HELPERS ----------------------

def daterange(start: date, end: date):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def month_bounds(ref: date) -> Tuple[date, date]:
    start = ref.replace(day=1)
    end = (start + relativedelta(months=1)) - timedelta(days=1)
    return start, end


def occupancy_matrix(period_start: date, period_end: date, rooms: List[str]) -> pd.DataFrame:
    # Costruisce una matrice con date come righe e stanze come colonne: True/False occupato
    df = fetch_bookings(period_start, period_end, status=None, room=None, search=None)
    idx = pd.date_range(period_start, period_end, freq="D")
    mat = pd.DataFrame(index=idx, columns=rooms)
    mat[:] = 0
    for _, b in df.iterrows():
        # contrassegna le notti occupate (check_in ... check_out-1)
        rng = pd.date_range(b.check_in, b.check_out - pd.Timedelta(days=1), freq="D")
        if b.room in mat.columns:
            mat.loc[rng, b.room] = 1
    mat.index.name = "Data"
    return mat


# ---------------------- STREAMLIT APP ----------------------

st.set_page_config(page_title="41° Parallelo — Prenotazioni", page_icon="📘", layout="wide")
init_db()

st.title("📘 41° Parallelo — Gestione prenotazioni")
st.caption("MVP locale con Streamlit + SQLite. Dati salvati nel file bookings_41_parallelo.db")

with st.sidebar:
    st.header("➕ Nuova prenotazione")
    with st.form("new_booking_form", clear_on_submit=True):
        guest_name = st.text_input("Ospite *", placeholder="Nome e cognome", max_chars=120)
        colA, colB = st.columns(2)
        with colA:
            email = st.text_input("Email", placeholder="es. mario.rossi@example.com")
        with colB:
            phone = st.text_input("Telefono", placeholder="es. +39 ...")
        source = st.selectbox("Canale", ["Diretta","Booking","Airbnb","Expedia","Altro"])
        room = st.selectbox("Alloggio", rooms_list())
        status = st.selectbox("Stato", ["Confermata","In attesa","Annullata"], index=0)
        col1, col2 = st.columns(2)
        with col1:
            check_in = st.date_input("Check‑in *", value=date.today())
        with col2:
            check_out = st.date_input("Check‑out *", value=date.today() + timedelta(days=1))
        guests = st.number_input("N. ospiti", min_value=1, max_value=10, value=2)
        price = st.number_input("Prezzo totale (€)", min_value=0.0, step=1.0, value=0.0)
        notes = st.text_area("Note")
        submitted = st.form_submit_button("Salva prenotazione")

    if submitted:
        if not guest_name:
            st.error("Il nome dell'ospite è obbligatorio.")
        elif check_out <= check_in:
            st.error("Il check‑out deve essere successivo al check‑in.")
        else:
            overlap, conflicts = has_overlap(check_in, check_out, room)
            if overlap and status != "Annullata":
                with st.expander("⚠️ Conflitto con prenotazioni esistenti (clicca per vedere)"):
                    st.dataframe(conflicts[["id","guest_name","room","status","check_in","check_out"]])
                st.warning("C'è una sovrapposizione per questa stanza nel periodo selezionato.")
            booking_id = insert_booking({
                "guest_name": guest_name,
                "email": email,
                "phone": phone,
                "source": source,
                "room": room,
                "status": status,
                "check_in": check_in,
                "check_out": check_out,
                "guests": guests,
                "price": float(price),
                "notes": notes,
            })
            st.success(f"Prenotazione salvata (ID {booking_id}).")

# Filtri globali
st.subheader("📅 Calendario & elenco prenotazioni")
colf1, colf2, colf3, colf4, colf5 = st.columns([1,1,1,1,2])
with colf1:
    ref_month = st.date_input("Mese di riferimento", value=date.today().replace(day=1))
with colf2:
    status_filter = st.selectbox("Stato", ["Tutte","Confermata","In attesa","Annullata"], index=0)
with colf3:
    room_filter = st.selectbox("Alloggio", ["Tutte"] + rooms_list())
with colf4:
    text_filter = st.text_input("Cerca (nome/mail/tel/nota)")
with colf5:
    days_range = st.slider("Finestra giorni", min_value=7, max_value=62, value=31, step=1)

period_start = ref_month
period_end = ref_month + timedelta(days=days_range-1)

# Tabs principali
calendar_tab, list_tab, export_tab, edit_tab = st.tabs(["🗓️ Calendario","📄 Elenco","⬇️ Esporta","✏️ Modifica/Elimina"])

with calendar_tab:
    st.write(f"Periodo: **{period_start.strftime('%d/%m/%Y')} – {period_end.strftime('%d/%m/%Y')}**")
    mat = occupancy_matrix(period_start, period_end, rooms_list())
    # Applica filtri su stanze se necessario
    if room_filter != "Tutte":
        mat = mat[[room_filter]]
    st.caption("1 = notte occupata, 0 = libera")
    st.dataframe(mat)

with list_tab:
    df = fetch_bookings(period_start, period_end, status=None if status_filter=="Tutte" else status_filter,
                        room=None if room_filter=="Tutte" else room_filter,
                        search=text_filter or None)
    if df.empty:
        st.info("Nessuna prenotazione nel periodo/filtri selezionati.")
    else:
        # colonne più leggibili
        show = df.copy()
        show["notti"] = (show["check_out"] - show["check_in"]).dt.days
        show = show[["id","guest_name","room","status","check_in","check_out","notti","guests","source","price","email","phone","notes","updated_at"]]
        st.dataframe(show, use_container_width=True)

with export_tab:
    df = fetch_bookings()
    if df.empty:
        st.info("Niente da esportare.")
    else:
        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button("Scarica CSV di tutte le prenotazioni", data=csv, file_name="prenotazioni_41_parallelo.csv", mime="text/csv")
        st.caption("Il CSV contiene tutte le colonne del database")

with edit_tab:
    st.write("Seleziona una prenotazione per modificarla o cancellarla.")
    df = fetch_bookings()
    if df.empty:
        st.info("Nessuna prenotazione presente.")
    else:
        sel_id = st.selectbox("ID prenotazione", options=df["id"].tolist())
        row = df.loc[df["id"] == sel_id].iloc[0]
        with st.form("edit_form"):
            col1, col2 = st.columns(2)
            with col1:
                guest_name = st.text_input("Ospite *", value=row.guest_name)
                email = st.text_input("Email", value=row.email or "")
                phone = st.text_input("Telefono", value=row.phone or "")
                source = st.selectbox("Canale", ["Diretta","Booking","Airbnb","Expedia","Altro"], index=["Diretta","Booking","Airbnb","Expedia","Altro"].index(row.source or "Diretta"))
                room = st.selectbox("Alloggio", rooms_list(), index=max(rooms_list().index(row.room) if row.room in rooms_list() else 0, 0))
            with col2:
                status = st.selectbox("Stato", ["Confermata","In attesa","Annullata"], index=["Confermata","In attesa","Annullata"].index(row.status or "Confermata"))
                check_in = st.date_input("Check‑in *", value=pd.to_datetime(row.check_in).date())
                check_out = st.date_input("Check‑out *", value=pd.to_datetime(row.check_out).date())
                guests = st.number_input("N. ospiti", min_value=1, max_value=10, value=int(row.guests or 1))
                price = st.number_input("Prezzo totale (€)", min_value=0.0, step=1.0, value=float(row.price or 0.0))
            notes = st.text_area("Note", value=row.notes or "")

            c1, c2, c3 = st.columns(3)
            do_update = c1.form_submit_button("💾 Salva modifiche")
            do_delete = c3.form_submit_button("🗑️ Elimina", help="Operazione irreversibile")

        if do_update:
            if not guest_name:
                st.error("Il nome dell'ospite è obbligatorio.")
            elif check_out <= check_in:
                st.error("Il check‑out deve essere successivo al check‑in.")
            else:
                overlap, conflicts = has_overlap(check_in, check_out, room, exclude_id=int(sel_id))
                if overlap and status != "Annullata":
                    with st.expander("⚠️ Conflitto con prenotazioni esistenti (clicca per vedere)"):
                        st.dataframe(conflicts[["id","guest_name","room","status","check_in","check_out"]])
                    st.warning("C'è una sovrapposizione per questa stanza nel periodo selezionato.")
                update_booking(int(sel_id), {
                    "guest_name": guest_name,
                    "email": email,
                    "phone": phone,
                    "source": source,
                    "room": room,
                    "status": status,
                    "check_in": check_in,
                    "check_out": check_out,
                    "guests": guests,
                    "price": float(price),
                    "notes": notes,
                })
                st.success("Prenotazione aggiornata.")

        if do_delete:
            delete_booking(int(sel_id))
            st.success("Prenotazione eliminata.")

# ---------------------- FOOTER ----------------------
st.markdown("---")
st.caption(
    "Suggerimenti: usa 'Alloggio' nel filtro per vedere la disponibilità stanza per stanza; "
    "esporta il CSV per condividere i dati con il commercialista o importare in Excel."
)
