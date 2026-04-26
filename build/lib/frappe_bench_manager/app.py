#!/usr/bin/env python3
"""
Frappe Bench Manager — TUI
Usage: python3 app.py
"""

import json, subprocess
from pathlib import Path
import mysql.connector
from textual.app import App, ComposeResult
from textual.widgets import (
    DataTable, Header, Footer, Button, Label,
    Input, Static, TabbedContent, TabPane, LoadingIndicator
)
from textual.screen import ModalScreen
from textual.containers import Horizontal, Container, Vertical
from textual import on, work

MYSQL_USER  = "root"
SEARCH_ROOT = str(Path.home())   # Change if benches live elsewhere

# ── Data Layer ────────────────────────────────────────────────────────────────

def get_connection(password: str):
    return mysql.connector.connect(host="localhost", user=MYSQL_USER, password=password)

def find_benches(on_progress=None) -> list[Path]:
    benches = []
    for apps_dir in Path(SEARCH_ROOT).rglob("apps"):
        if (apps_dir / "frappe").is_dir():
            benches.append(apps_dir.parent)
            if on_progress:
                on_progress(f"Found bench: {apps_dir.parent.name}")
    return benches

def db_size(cursor, db_name: str) -> str:
    cursor.execute(
        "SELECT ROUND(SUM(data_length+index_length)/1024/1024, 2) "
        "FROM information_schema.tables WHERE table_schema=%s",
        (db_name,)
    )
    row = cursor.fetchone()
    return f"{row[0]} MB" if row and row[0] else "0 MB"

def load_data(cursor, on_progress=None):
    sites, known_dbs = [], set()

    if on_progress:
        on_progress("Scanning for benches...")

    benches = find_benches(on_progress)

    if on_progress:
        on_progress(f"Found {len(benches)} bench(es). Reading site configs...")

    for bench_path in benches:
        sites_dir = bench_path / "sites"
        if not sites_dir.exists():
            continue
        for site_dir in sites_dir.iterdir():
            if not site_dir.is_dir() or site_dir.name in ("assets",):
                continue
            config = site_dir / "site_config.json"
            if not config.exists():
                continue
            try:
                data    = json.loads(config.read_text())
                db_name = data.get("db_name", "")
                if not db_name:
                    continue
                if on_progress:
                    on_progress(f"Reading site: {site_dir.name} …")
                known_dbs.add(db_name)
                sites.append({
                    "bench":      bench_path.name,
                    "bench_path": str(bench_path),
                    "site":       site_dir.name,
                    "db_name":    db_name,
                    "size":       db_size(cursor, db_name),
                })
            except Exception:
                continue

    if on_progress:
        on_progress("Checking for orphaned databases...")

    cursor.execute("SHOW DATABASES")
    orphaned = []
    for (db,) in cursor.fetchall():
        if db.startswith("_") and db not in known_dbs:
            orphaned.append({"db_name": db, "size": db_size(cursor, db)})

    return sites, orphaned


# ── Modal Screens ─────────────────────────────────────────────────────────────

class PasswordScreen(ModalScreen):
    def compose(self) -> ComposeResult:
        yield Container(
            Label("🔐  MySQL root password", classes="dialog-title"),
            Input(password=True, id="pw-input"),
            Button("Connect", variant="primary", id="btn-connect"),
            id="pw-dialog"
        )

    @on(Button.Pressed, "#btn-connect")
    def submit(self):
        self.dismiss(self.query_one("#pw-input", Input).value)

    def on_input_submitted(self, event: Input.Submitted):
        self.dismiss(event.value)


class ConfirmScreen(ModalScreen):
    def __init__(self, message: str, confirm_label: str = "Confirm"):
        super().__init__()
        self._message = message
        self._label   = confirm_label

    def compose(self) -> ComposeResult:
        yield Container(
            Label(self._message, id="confirm-msg"),
            Horizontal(
                Button(self._label, variant="error",   id="btn-yes"),
                Button("Cancel",    variant="default", id="btn-no"),
            ),
            id="confirm-dialog"
        )

    @on(Button.Pressed, "#btn-yes")
    def yes(self): self.dismiss(True)

    @on(Button.Pressed, "#btn-no")
    def no(self):  self.dismiss(False)


# ── Main App ──────────────────────────────────────────────────────────────────

class FrappeManager(App):
    TITLE = "Frappe Bench Manager"

    CSS = """
    Screen { background: $background; }

    /* ── Dialogs ── */
    PasswordScreen, ConfirmScreen { align: center middle; }

    #pw-dialog, #confirm-dialog {
        width: 60; height: auto;
        background: $surface;
        border: thick $primary;
        padding: 2 4;
    }
    .dialog-title { text-align: center; margin-bottom: 1; color: $accent; }
    #confirm-msg  { text-align: center; margin-bottom: 1; }
    Horizontal    { align: center middle; height: auto; margin-top: 1; }
    Button        { margin: 0 1; }

    /* ── Loading overlay ── */
    #loading-overlay {
        width: 1fr; height: 1fr;
        align: center middle;
        background: $background 70%;
        display: none;
        layer: overlay;
    }
    #loading-overlay.visible { display: block; }

    #loading-box {
        width: 50; height: 7;
        background: $surface;
        border: tall $primary;
        padding: 1 2;
        align: center middle;
    }
    #loading-label {
        text-align: center;
        color: $accent;
        margin-top: 1;
    }

    /* ── Layout ── */
    DataTable { height: 1fr; }

    #actions {
        height: 3;
        align: left middle;
        padding: 0 1;
        background: $surface;
    }
    #status {
        height: 1;
        padding: 0 2;
        background: $panel;
        color: $text-muted;
    }
    """

    BINDINGS = [
        ("d", "drop_selected", "Drop"),
        ("r", "refresh",       "Refresh"),
        ("q", "quit",          "Quit"),
    ]

    def __init__(self):
        super().__init__()
        self.conn       = None
        self.cursor     = None
        self.mysql_pass = ""
        self.sites: list    = []
        self.orphaned: list = []

    # ── Compose ──────────────────────────────────────────────────

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with TabbedContent(id="tabs"):
            with TabPane("🖥  Active Sites",  id="tab-sites"):
                yield DataTable(id="sites-table",   cursor_type="row")
            with TabPane("⚠  Orphaned DBs", id="tab-orphaned"):
                yield DataTable(id="orphaned-table", cursor_type="row")
        with Horizontal(id="actions"):
            yield Button("⛔ Drop Selected", variant="error",   id="btn-drop")
            yield Button("🔄 Refresh",       variant="default", id="btn-refresh")
        yield Static("", id="status")

        # Loading overlay — sits on top of everything
        with Container(id="loading-overlay"):
            with Vertical(id="loading-box"):
                yield LoadingIndicator()
                yield Label("Please wait…", id="loading-label")

        yield Footer()

    # ── Loading helpers ───────────────────────────────────────────

    def _show_loading(self, msg: str = "Please wait…"):
        overlay = self.query_one("#loading-overlay")
        overlay.add_class("visible")
        self.query_one("#loading-label", Label).update(msg)

    def _hide_loading(self):
        self.query_one("#loading-overlay").remove_class("visible")

    def _loading_progress(self, msg: str):
        """Called from worker thread — must use call_from_thread."""
        self.call_from_thread(self._show_loading, msg)

    # ── Lifecycle ─────────────────────────────────────────────────

    def on_mount(self):
        self.push_screen(PasswordScreen(), self._on_password)

    def _on_password(self, password: str):
        self._show_loading("Connecting to MySQL…")
        self._connect_and_load(password)

    @work(thread=True)
    def _connect_and_load(self, password: str):
        try:
            self.call_from_thread(self._show_loading, "Connecting to MySQL…")
            conn   = get_connection(password)
            cursor = conn.cursor()
            self.call_from_thread(self._set_connection, conn, cursor, password)
            self._load_in_thread(cursor)
        except mysql.connector.Error as e:
            self.call_from_thread(self._hide_loading)
            self.call_from_thread(self._status, f"[red]MySQL connect failed: {e}[/red]")

    def _set_connection(self, conn, cursor, password):
        self.conn       = conn
        self.cursor     = cursor
        self.mysql_pass = password

    # ── Load ──────────────────────────────────────────────────────

    @work(thread=True)
    def _load(self):
        if not self.cursor:
            return
        self._load_in_thread(self.cursor)

    def _load_in_thread(self, cursor):
        try:
            sites, orphaned = load_data(cursor, on_progress=self._loading_progress)
            self.call_from_thread(self._apply_data, sites, orphaned)
        except Exception as e:
            self.call_from_thread(self._hide_loading)
            self.call_from_thread(self._status, f"[red]Load error: {e}[/red]")

    def _apply_data(self, sites, orphaned):
        self.sites    = sites
        self.orphaned = orphaned
        self._fill_sites()
        self._fill_orphaned()
        self._hide_loading()
        wasted = sum(
            float(o["size"].replace(" MB", "") or 0)
            for o in self.orphaned
        )
        self._status(
            f"  Sites: [green]{len(self.sites)}[/green]  │  "
            f"Orphaned DBs: [yellow]{len(self.orphaned)}[/yellow]  │  "
            f"Wasted Space: [red]{wasted:.1f} MB[/red]"
        )

    def _fill_sites(self):
        t = self.query_one("#sites-table", DataTable)
        t.clear(columns=True)
        t.add_columns("Bench", "Site", "DB Name", "Size")
        for s in self.sites:
            t.add_row(s["bench"], s["site"], s["db_name"], s["size"])

    def _fill_orphaned(self):
        t = self.query_one("#orphaned-table", DataTable)
        t.clear(columns=True)
        t.add_columns("DB Name", "Size", "Reason")
        for o in self.orphaned:
            t.add_row(o["db_name"], o["size"], "bench folder deleted / never had a site")

    def _status(self, msg: str):
        self.query_one("#status", Static).update(msg)

    # ── Actions ───────────────────────────────────────────────────

    def action_refresh(self):
        if self.cursor:
            self._load()

    @on(Button.Pressed, "#btn-refresh")
    def on_refresh(self): self.action_refresh()

    @on(Button.Pressed, "#btn-drop")
    def on_drop(self): self.action_drop_selected()

    def action_drop_selected(self):
        active = self.query_one("#tabs", TabbedContent).active

        if active == "tab-sites":
            idx = self.query_one("#sites-table", DataTable).cursor_row
            if not (0 <= idx < len(self.sites)):
                return
            site = self.sites[idx]
            msg  = (
                f"Drop site [bold]{site['site']}[/bold]\n"
                f"Bench : {site['bench']}\n"
                f"DB    : {site['db_name']}  ({site['size']})\n\n"
                f"[red]This will run  bench drop-site  — irreversible![/red]"
            )
            self.push_screen(
                ConfirmScreen(msg, "Drop Site"),
                lambda ok: self._drop_site(ok, site)
            )

        elif active == "tab-orphaned":
            idx = self.query_one("#orphaned-table", DataTable).cursor_row
            if not (0 <= idx < len(self.orphaned)):
                return
            db  = self.orphaned[idx]
            msg = (
                f"Drop orphaned DB [bold]{db['db_name']}[/bold]\n"
                f"Size : {db['size']}\n\n"
                f"[red]No bench — just  DROP DATABASE.  Irreversible![/red]"
            )
            self.push_screen(
                ConfirmScreen(msg, "Drop DB"),
                lambda ok: self._drop_orphan_db(ok, db["db_name"])
            )

    # ── Drop Handlers ─────────────────────────────────────────────

    def _drop_site(self, confirmed: bool, site: dict):
        if not confirmed:
            return
        self._show_loading(f"Dropping site {site['site']}…")
        self._run_drop_site(site)

    @work(thread=True)
    def _run_drop_site(self, site: dict):
        try:
            self.call_from_thread(
                self._show_loading, f"Running bench drop-site {site['site']}…"
            )
            result = subprocess.run(
                [
                    "bench", "drop-site", site["site"],
                    "--root-password", self.mysql_pass,
                    "--force", "--no-backup",
                ],
                cwd=site["bench_path"],
                capture_output=True, text=True
            )
            if result.returncode == 0:
                self.call_from_thread(
                    self._status, f"[green]✓ Dropped site {site['site']}[/green]"
                )
            else:
                err = (result.stderr or result.stdout)[:120]
                self.call_from_thread(self._status, f"[red]bench error: {err}[/red]")
        except Exception as e:
            self.call_from_thread(self._status, f"[red]Failed: {e}[/red]")
        finally:
            self._load_in_thread(self.cursor)

    def _drop_orphan_db(self, confirmed: bool, db_name: str):
        if not confirmed:
            return
        self._show_loading(f"Dropping database {db_name}…")
        self._run_drop_orphan(db_name)

    @work(thread=True)
    def _run_drop_orphan(self, db_name: str):
        try:
            self.call_from_thread(
                self._show_loading, f"Dropping database {db_name}…"
            )
            self.cursor.execute(f"DROP DATABASE `{db_name}`")
            self.conn.commit()
            self.call_from_thread(
                self._status, f"[green]✓ Dropped orphaned DB {db_name}[/green]"
            )
        except Exception as e:
            self.call_from_thread(self._status, f"[red]Failed: {e}[/red]")
        finally:
            self._load_in_thread(self.cursor)


def main():
    FrappeManager().run()


if __name__ == "__main__":
    main()