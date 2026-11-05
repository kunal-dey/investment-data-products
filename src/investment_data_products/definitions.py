import os
import shutil
import sqlite3
import stat
import time
from pathlib import Path

from dagster import Definitions, definitions, in_process_executor, load_from_defs_folder

_PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _ensure_dagster_home() -> None:
    """DbtProjectComponent writes defs-state into instance SQLite (history/runs.db).

    ``dg dev`` without DAGSTER_HOME uses a throwaway ``.tmp_dagster_home_*`` that
    the code server cannot open on Windows (sqlite3: unable to open database file).
    Keep a project-local home unless the process already has a real one (EC2).
    """
    configured = os.environ.get("DAGSTER_HOME")
    local = _PROJECT_ROOT / ".dagster_home"
    home = Path(configured).expanduser() if configured else local
    try:
        resolved = home.resolve()
        ephemeral = home.name.startswith(".tmp_dagster_home")
        if resolved == Path.home().resolve() or ephemeral:
            home = local
    except OSError:
        home = local
    home.mkdir(parents=True, exist_ok=True)
    (home / "history" / "runs").mkdir(parents=True, exist_ok=True)
    (home / "storage").mkdir(parents=True, exist_ok=True)
    (home / "schedules").mkdir(parents=True, exist_ok=True)
    os.environ["DAGSTER_HOME"] = str(home)
    _tune_sqlite(home / "history" / "runs.db")
    _tune_sqlite(home / "history" / "runs" / "index.db")
    _tune_sqlite(home / "schedules" / "schedules.db")


def _tune_sqlite(path: Path) -> None:
    """WAL + long busy timeout so webserver/daemon/run don't lock on Windows."""
    if not path.exists():
        return
    conn = sqlite3.connect(str(path), timeout=60)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=60000;")
        conn.commit()
    finally:
        conn.close()


def _ensure_dbt_on_path() -> None:
    for scripts in (_PROJECT_ROOT / ".venv" / "Scripts", _PROJECT_ROOT / ".venv" / "bin"):
        if (scripts / "dbt.exe").exists() or (scripts / "dbt").exists():
            os.environ["PATH"] = str(scripts) + os.pathsep + os.environ.get("PATH", "")
            break


def _patch_dbt_state_on_windows() -> None:
    """DbtProjectComponent copies ./dbt into defs/.local_defs_state, including .git.

    Git object files are read-only on Windows, so shutil.rmtree raises WinError 5
    on the next dg dev / code-location reload. Skip .git on copy and chmod then
    unlink when rmtree hits a read-only file.
    """
    from dagster_dbt import dbt_project_manager as mgr

    skip = {".git", ".venv", "logs", "dbt_packages"}
    orig_ignore = mgr._ignore_nested_dest

    def ignore_dest(dest: Path):
        inner = orig_ignore(dest)

        def _ignore(src_dir: str, names: list[str]) -> set[str]:
            return inner(src_dir, names) | {n for n in names if n in skip}

        return _ignore

    mgr._ignore_nested_dest = ignore_dest

    orig_rmtree = shutil.rmtree

    def rmtree(path, *args, **kwargs):
        def onexc(func, p, exc):
            os.chmod(p, stat.S_IWRITE)
            for delay in (0.2, 0.5, 1.0):
                try:
                    func(p)
                    return
                except PermissionError:
                    time.sleep(delay)
            func(p)

        kwargs.setdefault("onexc", onexc)
        return orig_rmtree(path, *args, **kwargs)

    # Only dbt copy/cleanup — do not patch global shutil (dlt pipeline wipe uses it).
    mgr.shutil.rmtree = rmtree


def _patch_windows_run_spawn() -> None:
    """DefaultRunLauncher starts a spawn Process; on Windows the child Queue hits WinError 6.

    Run the job in a thread inside the gRPC code server instead.
    """
    if os.name != "nt":
        return

    import queue as queue_mod
    import threading

    from dagster._grpc.server import DagsterApiServicer

    orig_init = DagsterApiServicer.__init__

    class _ThreadCtx:
        def Queue(self):
            return queue_mod.Queue()

        def Event(self):
            return threading.Event()

        def Process(self, target=None, args=(), kwargs=None):
            thread = threading.Thread(
                target=target, args=args, kwargs=kwargs or {}, daemon=True
            )

            class _Proc:
                exitcode = None

                def start(_self):
                    thread.start()

                def is_alive(_self):
                    alive = thread.is_alive()
                    if not alive and _self.exitcode is None:
                        _self.exitcode = 0
                    return alive

                def join(_self, timeout=None):
                    thread.join(timeout)
                    if not thread.is_alive():
                        _self.exitcode = 0

                def terminate(_self):
                    pass

            return _Proc()

    def __init__(self, *args, **kwargs):
        orig_init(self, *args, **kwargs)
        self._mp_ctx = _ThreadCtx()

    DagsterApiServicer.__init__ = __init__


_ensure_dagster_home()
_ensure_dbt_on_path()
_patch_dbt_state_on_windows()
_patch_windows_run_spawn()


@definitions
def defs():
    loaded = load_from_defs_folder(path_within_project=Path(__file__).parent)
    # Windows spawn + many dlt steps hits WinError 6 (invalid handle); dbt then skips.
    if os.name == "nt":
        return Definitions.merge(loaded, Definitions(executor=in_process_executor))
    return loaded
