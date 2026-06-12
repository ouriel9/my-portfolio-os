"""Portfolio OS launcher.

Architecture:
  • Streamlit installs SIGINT/SIGTERM handlers, which can only run on the
    MAIN thread of an interpreter. pywebview's Chromium window also needs
    the main thread (Windows COM/WinForms). Running both in one process
    is impossible — so we spawn ourselves as a child subprocess that runs
    Streamlit, and use this process for pywebview.
  • The bundled EXE re-launches itself with a `--streamlit-server` flag.
    The child process runs Streamlit's bootstrap and exits when killed.
    The parent process opens the pywebview window pointed at the child's
    URL. When the user closes the window the parent kills the child.
"""
from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from urllib import request as urlrequest


CHILD_FLAG = "--streamlit-server"

# An always-on Streamlit server is started at logon (Startup folder) on this
# port. If it's already up, the window opens INSTANTLY against it — no cold
# start — and it always serves the latest app.py (synced from GitHub), so the
# EXE is never stale. Only if it's down do we spawn our own bundled server.
PERSISTENT_PORT = 8501


def _find_app_root() -> Path:
    """Resolve the directory that contains app.py + the data files."""
    if getattr(sys, "frozen", False):
        meipass = getattr(sys, "_MEIPASS", None)
        if meipass:
            return Path(meipass)
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_streamlit(url: str, timeout_s: float = 90.0) -> bool:
    """Poll the Streamlit healthcheck until ready."""
    deadline = time.time() + timeout_s
    health = url.rstrip("/") + "/_stcore/health"
    while time.time() < deadline:
        try:
            with urlrequest.urlopen(health, timeout=2) as resp:
                if resp.status == 200 and resp.read().strip() in (b"ok", b"OK"):
                    return True
        except Exception:
            pass
        time.sleep(0.5)
    return False


def _run_streamlit_main(app_path: Path, port: int) -> int:
    """Run Streamlit on THIS process's main thread.
    Called only when the launcher is invoked with `--streamlit-server`."""
    os.environ["STREAMLIT_BROWSER_GATHER_USAGE_STATS"] = "false"
    os.environ["STREAMLIT_SERVER_HEADLESS"] = "true"
    os.environ["STREAMLIT_SERVER_PORT"] = str(port)
    os.environ["STREAMLIT_SERVER_ADDRESS"] = "127.0.0.1"
    try:
        from streamlit.web import bootstrap  # type: ignore
        from streamlit import config as st_config  # type: ignore
        st_config.set_option("server.port", port)
        st_config.set_option("server.address", "127.0.0.1")
        st_config.set_option("server.headless", True)
        st_config.set_option("browser.gatherUsageStats", False)
        st_config.set_option("server.runOnSave", False)
        st_config.set_option("global.developmentMode", False)
        st_config.set_option("server.fileWatcherType", "none")
        bootstrap.run(str(app_path), is_hello=False, args=[], flag_options={})
        return 0
    except SystemExit as exc:
        return int(exc.code or 0)
    except Exception as exc:
        sys.stderr.write(f"[streamlit-server] crashed: {exc}\n")
        return 3


def _spawn_streamlit_child(app_path: Path, port: int) -> subprocess.Popen:
    """Re-spawn THIS executable (or python+launcher.py in dev mode) with
    the --streamlit-server flag so the child runs the server on its
    main thread. Hides any console window on Windows."""
    if getattr(sys, "frozen", False):
        # Bundled EXE: re-launch self
        cmd = [sys.executable, CHILD_FLAG, str(app_path), str(port)]
    else:
        # Dev: re-launch python launcher.py
        cmd = [sys.executable, str(Path(__file__).resolve()), CHILD_FLAG, str(app_path), str(port)]

    creationflags = 0
    if sys.platform == "win32":
        creationflags = 0x08000000  # CREATE_NO_WINDOW

    return subprocess.Popen(
        cmd, creationflags=creationflags,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        cwd=str(app_path.parent),
    )


def _open_window(url: str) -> None:
    import webview

    webview.create_window(
        title="Portfolio OS",
        url=url,
        width=1440, height=900,
        min_size=(900, 600),
        resizable=True,
        background_color="#0f172a",
        text_select=True,
    )
    webview.start(debug=False, http_server=False)


def main() -> int:
    # Child-process branch: just run Streamlit on our main thread
    if len(sys.argv) >= 2 and sys.argv[1] == CHILD_FLAG:
        if len(sys.argv) < 4:
            sys.stderr.write("[streamlit-server] missing app_path or port arg\n")
            return 4
        return _run_streamlit_main(Path(sys.argv[2]), int(sys.argv[3]))

    # Parent (window) branch

    # ── FAST PATH: reuse the always-on server if it's already healthy ──
    # This opens the window in <1s instead of cold-starting a bundled server,
    # and shows the latest app.py the user runs locally.
    persistent_url = f"http://127.0.0.1:{PERSISTENT_PORT}/"
    if _wait_for_streamlit(persistent_url, timeout_s=1.5):
        _open_window(persistent_url)
        return 0

    # ── FALLBACK: no always-on server → spawn our own bundled Streamlit ──
    app_root = _find_app_root()
    app_path = app_root / "app.py"
    if not app_path.exists():
        sys.stderr.write(f"[launcher] app.py not found at {app_path}\n")
        return 1

    port = _free_port()
    url = f"http://127.0.0.1:{port}/"

    proc = _spawn_streamlit_child(app_path, port)
    try:
        if not _wait_for_streamlit(url, timeout_s=90.0):
            sys.stderr.write("[launcher] Streamlit failed to start within 90s\n")
            try:
                proc.terminate()
            except Exception:
                pass
            return 2

        _open_window(url)
    finally:
        # User closed window → kill child
        try:
            proc.terminate()
            proc.wait(timeout=5)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
