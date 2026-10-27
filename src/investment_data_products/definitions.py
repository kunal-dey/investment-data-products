import os
import shutil
import stat
from pathlib import Path

from dagster import AssetKey, Definitions, definitions, in_process_executor, load_from_defs_folder

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
            func(p)

        kwargs.setdefault("onexc", onexc)
        return orig_rmtree(path, *args, **kwargs)

    shutil.rmtree = rmtree
    mgr.shutil.rmtree = rmtree


_ensure_dagster_home()
_ensure_dbt_on_path()
_patch_dbt_state_on_windows()


def _etf_bars_depends_on_watch(spec):
    database = os.environ.get("GLUE_DATABASE")
    if database and spec.key == AssetKey([database, "etf_bars"]):
        return spec.replace_attributes(deps=[AssetKey([database, "nse_etfs"])])
    return spec


@definitions
def defs():
    loaded = load_from_defs_folder(path_within_project=Path(__file__).parent)
    loaded = loaded.map_asset_specs(func=_etf_bars_depends_on_watch)
    # Windows spawn + many dlt steps hits WinError 6 (invalid handle); dbt then skips.
    if os.name == "nt":
        return Definitions.merge(loaded, Definitions(executor=in_process_executor))
    return loaded
