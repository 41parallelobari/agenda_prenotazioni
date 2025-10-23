# 41° Parallelo — App personale gestione prenotazioni (Streamlit + SQLite + iCal Booking)
# ---------------------------------------------------------------------------------
# Istruzioni rapide (Windows/Mac/Linux)
# 1) requirements.txt (per Streamlit Cloud):
#    streamlit
pandas
python-dateutil
requests
ics
# 2) Avvio in locale:  streamlit run 41_parallelo_app.py
#    (rinomina il file in 41_parallelo_app.py se necessario)
# 3) Il database locale è "bookings_41_parallelo.db" nella stessa cartella.
# ---------------------------------------------------------------------------------

from __future__ import annotations

import re
import sqlite3
from contextlib import closing
from datetime import date, datetime, timedelta
from typing import List, Optional, Tuple, Dict

import pandas as pd
import streamlit as st
from dateutil.relativedelta import relativedelta
import requests
from ics import Calendar

DB_PATH = "bookings_41_parallelo.db"

# ============================== DB LAYER ==============================

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS bookings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    guest_name TEXT NOT NULL,
    email TEXT,
    phone TEXT,
    source TEXT DEFAULT 'Diretta',
    room TEXT DEFAULT 'Camera 1',
    status TEXT DEFAULT 'Confermata',
    check_in DATE NOT NULL,
    check_out DATE NOT NULL,
    guests INTEGER DEFAULT 1,
    price REAL DEFAULT 0,
    notes TEXT,
    external_source TEXT,
    external_uid TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_room_dates ON bookings(room, check_in, check_out)",
    "CREATE INDEX IF NOT EXISTS idx_status ON bookings(status)",
    "CREATE INDEX IF NOT EXISTS idx_external_uid ON bookings(external_source, external_uid)",
]


def get_conn() -> sqlite3.Connection:
    return sqlite3.connect(
        DB_PATH,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
    )


def init_db() -> None:
    with closing(get_conn()) as conn, conn:
        conn.execute(CREATE_TABLE_SQL)
        for q in CREATE_INDEXES:
            conn.execute(q)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ical_endpoints (
                room TEXT PRIMARY KEY,
                url TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )


def insert_booking(data: dict) -> int:
    with closing(get_conn()) as conn, conn:
        cur = conn.execute(
            """
            INSERT INTO bookings(
                guest_name, email, phone, source, room, status,
                check_in, check_out, guests, price, notes,
                external_source, external_uid
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                data["guest_name"], data.get("email"), data.get("phone"), data.get("source"),
                data.get("room"), data.get("status"),
                data["check_in"], data["check_out"], data.get("guests", 1),
                data.get("price", 0.0), data.get("notes"),
                data.get("external_source"), data.get("external_uid"),
            ),
        )
        return cur.lastrowid


def update_booking(booking_id: int, data: dict) -> None:
    keys = [
        "guest_name", "email", "phone", "source", "room", "status",
        "check_in", "check_out", "guests", "price", "notes",
        "external_source", "external_uid",
    ]
    sets = ", ".join([f"{k} = ?" for k in keys]) + ", updated_at = CURRENT_TIMESTAMP"
    values = [data.get(k) for k in keys]
    values.append(booking_id)
    with closing(get_conn()) as conn, conn:
        conn.execute(f"UPDATE bookings SET {sets} WHERE id = ?", values)


def delete_booking(booking_id: int) -> None:
    with closing(get_conn()) as conn, conn:
        conn.execute("DELETE FROM bookings WHERE id = ?", (booking_id,))


def fetch_bookings(
    start: Optional[date] = None,
    end: Optional[date] = None,
    status: Optional[str] = None,
    room: Optional[str] = None,
    search: Optional[str] = None,
) -> pd.DataFrame:
    query = (
        "SELECT id, guest_name, email, phone, source, room, status, check_in, check_out, "
        "guests, price, notes, external_source, external_uid, created_at, updated_at "
        "FROM bookings WHERE 1=1"
    )
    params: List = []
    if start:
        query += " AND date(check_out) > date(?)"
        params.append(start)
    if end:
        query += " AND date(check_in) < date(?)"
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
        df = pd.read_sql_query(
            query,
            conn,
            params=params,
            parse_dates=["check_in", "check_out", "created_at", "updated_at"],
        )
    return df


def rooms_list() -> List[str]:
    with closing(get_conn()) as conn:
        rows = conn.execute("SELECT DISTINCT room FROM bookings ORDER BY room").fetchall()
    existing = [r[0] for r in rows]
    defaults = ["Camera 1", "Camera 2", "Appartamento"]
    for d in defaults:
        if d not in existing:
            existing.append(d)
    return existing


def has_overlap(check_in: date, check_out: date, room: str, exclude_id: Optional[int] = None) -> Tuple[bool, pd.DataFrame]:
    query = (
        "SELECT * FROM bookings WHERE room = ? AND status != 'Annullata' "
        "AND date(check_in) < date(?) AND date(check_out) > date(?)"
    )
    params: List = [room, check_out, check_in]
    if exclude_id is not None:
        query += " AND id != ?"
        params.append(exclude_id)
    with closing(get_conn()) as conn:
        df = pd.read_sql_query(
            query,
            conn,
            params=params,
            parse_dates=["check_in", "check_out", "created_at", "updated_at"],
        )
    return (len(df) > 0, df)

# ===================== BOOKING.COM INTEGRAZIONE ICAL =====================


def upsert_ical_endpoint(room: str, url: str) -> None:
    with closing(get_conn()) as conn, conn:
        conn.execute(
            "INSERT INTO ical_endpoints(room, url) VALUES(?, ?) "
            "ON CONFLICT(room) DO UPDATE SET url=excluded.url, updated_at=CURRENT_TIMESTAMP",
            (room, url),
        )


def get_ical_map() -> Dict[str, str]:
    with closing(get_conn()) as conn:
        rows = conn.execute("SELECT room, url FROM ical_endpoints").fetchall()
    return {r[0]: r[1] for r in rows}


def parse_guest_from_summary(summary: str) -> str:
    if not summary:
        return "Ospite Booking"
    s = re.sub(r"Prenotazione|Reservation|Booking|Guest|Ospite|#", "", summary, flags=re.I).strip()
    return s or "Ospite Booking"


def import_ics_for_room(room: str, url: str) -> int:
    """Scarica e importa eventi da un feed iCal per una stanza. Deduplica via (external_source, external_uid)."""
    try:
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
    except Exception as e:
        st.error(f"Errore nel download iCal per {room}: {e}")
        return 0

    try:
        cal = Calendar(resp.text)
    except Exception as e:
        st.error(f"Errore nel parsing iCal per {room}: {e}")
        return 0

    created = 0
    with closing(get_conn()) as conn, conn:
        for ev in cal.events:
            uid = getattr(ev, "uid", None) or f"{room}-{getattr(ev, 'begin', '')}-{getattr(ev, 'end', '')}"
            start = ev.begin.date() if hasattr(ev, "begin") and ev.begin else None
            end = ev.end.date() if hasattr(ev, "end") and ev.end else None
            if not (start and end):
                continue
            guest = parse_guest_from_summary(getattr(ev, "name", "") or "")
            row = conn.execute(
                "SELECT id FROM bookings WHERE external_source=? AND external_uid=?",
                ("booking_com_ical", uid),
            ).fetchone()
            if row:
                continue
            conn.execute(
                """
                INSERT INTO bookings(
                    guest_name, source, room, status, check_in, check_out, guests, price, notes,
                    external_source, external_uid
                ) VALUES (?, 'Booking', ?, 'Confermata', ?, ?, 2, 0, 'Import iCal', 'booking_com_ical', ?)
                """,
                (guest, room, start, end, uid),
            )
            created += 1
    return created

# ============================== UI HELPERS ==============================


def month_bounds(ref: date) -> Tuple[date, date]:
    start = ref.replace(day=1)
    end = (start + relativedelta(months=1)) - timedelta(days=1)
    return start, end


def occupancy_matrix(period_start: date, period_end: date, rooms: List[str]) -> pd.DataFrame:
    df = fetch_bookings(period_start, period_end, status=None, room=None, search=None)
    idx = pd.date_range(period_start, period_end, freq="D")
    mat = pd.DataFrame(index=idx, columns=rooms)
    mat[:] = 0
    for _, b in df.iterrows():
        rng = pd.date_range(b.check_in, b.check_out - pd.Timedelta(days=1), freq="D")
        if b.room in mat.columns:
            mat.loc[rng, b.room] = 1
    mat.index.name = "Data"
    return mat

# ============================== STREAMLIT APP ==============================

st.set_page_config(page_title="41° Parallelo — Prenotazioni", page_icon="📘", layout="wide")
init_db()

st.title("📘 41° Parallelo — Gestione prenotazioni")
st.caption("MVP con Streamlit + SQLite. Dati locali in bookings_41_parallelo.db — iCal Booking opzionale.")

with st.sidebar:
    st.header("➕ Nuova prenotazione")
    with st.form("new_booking_form", clear_on_submit=True):
        guest_name = st.text_input("Ospite *", placeholder="Nome e cognome", max_chars=120)
        colA, colB = st.columns(2)
        with colA:
            email = st.text_input("Email", placeholder="es. mario.rossi@example.com")
        with colB:
            phone = st.text_input("Telefono", placeholder="es. +39 ...")
        source = st.selectbox("Canale", ["Diretta", "Booking", "Airbnb", "Expedia", "Altro"])
        room = st.selectbox("Alloggio", rooms_list())
        status = st.selectbox("Stato", ["Confermata", "In attesa", "Annullata"], index=0)
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
                    st.dataframe(conflicts[["id", "guest_name", "room", "status", "check_in", "check_out"]])
                st.warning("C'è una sovrapposizione per questa stanza nel periodo selezionato.")
            booking_id = insert_booking(
                {
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
                }
            )
            st.success(f"Prenotazione salvata (ID {booking_id}).")

    st.markdown("---")
    st.header("🔗 Collega Booking.com (iCal)")
    st.caption("Incolla gli URL iCal delle stanze da Booking.com → Calendario → Sincronizza calendario.")
    ical_map = get_ical_map()
    available_rooms = rooms_list()
    with st.form("ical_form"):
        url_inputs: Dict[str, str] = {}
        for r in available_rooms:
            url_inputs[r] = st.text_input(f"URL iCal — {r}", value=ical_map.get(r, ""))
        save_urls = st.form_submit_button("💾 Salva URL iCal")
    if save_urls:
        for r, u in url_inputs.items():
            if u.strip():
                upsert_ical_endpoint(r, u.strip())
        st.success("URL iCal salvati.")
    if st.button("🔄 Sincronizza ora da Booking (iCal)"):
        count_total = 0
        for r, u in get_ical_map().items():
            count_total += import_ics_for_room(r, u)
        st.success(f"Sincronizzazione completata. Prenotazioni importate: {count_total}")

# ============================ FILTRI GLOBALI ============================

st.subheader("📅 Calendario & elenco prenotazioni")
colf1, colf2, colf3, colf4, colf5 = st.columns([1, 1, 1, 1, 2])
with colf1:
    ref_month = st.date_input("Mese di riferimento", value=date.today().replace(day=1))
with colf2:
    status_filter = st.selectbox("Stato", ["Tutte", "Confermata", "In attesa", "Annullata"], index=0)
with colf3:
    room_filter = st.selectbox("Alloggio", ["Tutte"] + rooms_list())
with colf4:
    text_filter = st.text_input("Cerca (nome/mail/tel/nota)")
with colf5:
    days_range = st.slider("Finestra giorni", min_value=7, max_value=62, value=31, step=1)

period_start = ref_month
period_end = ref_month + timedelta(days=days_range - 1)

calendar_tab, list_tab, export_tab, edit_tab = st.tabs(["🗓️ Calendario", "📄 Elenco", "⬇️ Esporta", "✏️ Modifica/Elimina"])

with calendar_tab:
    st.write(f"Periodo: **{period_start.strftime('%d/%m/%Y')} – {period_end.strftime('%d/%m/%Y')}**")
    mat = occupancy_matrix(period_start, period_end, rooms_list())
    if room_filter != "Tutte":
        mat = mat[[room_filter]]
    st.caption("1 = notte occupata, 0 = libera")
    st.dataframe(mat)

with list_tab:
    df = fetch_bookings(
        period_start,
        period_end,
        status=None if status_filter == "Tutte" else status_filter,
        room=None if room_filter == "Tutte" else room_filter,
        search=text_filter or None,
    )
    if df.empty:
        st.info("Nessuna prenotazione nel periodo/filtri selezionati.")
    else:
        show = df.copy()
        show["notti"] = (show["check_out"] - show["check_in"]).dt.days
        show = show[
            [
                "id",
                "guest_name",
                "room",
                "status",
                "check_in",
                "check_out",
                "notti",
                "guests",
                "source",
                "price",
                "email",
                "phone",
                "notes",
                "updated_at",
            ]
        ]
        st.dataframe(show, use_container_width=True)

with export_tab:
    df_all = fetch_bookings()
    if df_all.empty:
        st.info("Niente da esportare.")
    else:
        csv = df_all.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Scarica CSV di tutte le prenotazioni",
            data=csv,
            file_name="prenotazioni_41_parallelo.csv",
            mime="text/csv",
        )
        st.caption("Il CSV contiene tutte le colonne del database")

with edit_tab:
    st.write("Seleziona una prenotazione per modificarla o cancellarla.")
    df_all = fetch_bookings()
    if df_all.empty:
        st.info("Nessuna prenotazione presente.")
    else:
        sel_id = st.selectbox("ID prenotazione", options=df_all["id"].tolist())
        row = df_all.loc[df_all["id"] == sel_id].iloc[0]
        with st.form("edit_form"):
            col1, col2 = st.columns(2)
            with col1:
                guest_name = st.text_input("Ospite *", value=row.guest_name)
                email = st.text_input("Email", value=row.email or "")
                phone = st.text_input("Telefono", value=row.phone or "")
                source = st.selectbox(
                    "Canale",
                    ["Diretta", "Booking", "Airbnb", "Expedia", "Altro"],
                    index=["Diretta", "Booking", "Airbnb", "Expedia", "Altro"].index(row.source or "Diretta"),
                )
                all_rooms = rooms_list()
                room_index = all_rooms.index(row.room) if row.room in all_rooms else 0
                room = st.selectbox("Alloggio", all_rooms, index=room_index)
            with col2:
                status = st.selectbox(
                    "Stato",
                    ["Confermata", "In attesa", "Annullata"],
                    index=["Confermata", "In attesa", "Annullata"].index(row.status or "Confermata"),
                )
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
                        st.dataframe(conflicts[["id", "guest_name", "room", "status", "check_in", "check_out"]])
                    st.warning("C'è una sovrapposizione per questa stanza nel periodo selezionato.")
                update_booking(
                    int(sel_id),
                    {
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
                    },
                )
                st.success("Prenotazione aggiornata.")

        if do_delete:
            delete_booking(int(sel_id))
            st.success("Prenotazione eliminata.")

# ================================ FOOTER ================================

st.markdown("---")
st.caption(
    "Suggerimenti: usa il filtro 'Alloggio' per vedere disponibilità stanza per stanza; "
    "esporta il CSV per condivisione e report. iCal Booking importa nuove prenotazioni; "
    "per aggiornamenti/annullamenti avanzati posso estendere la logica su richiesta."
)
