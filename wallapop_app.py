from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
APP_FILE_CANDIDATES = [
    BASE_DIR / "wallapop_monitor_base.py",
    BASE_DIR / "scrapper" / "wallapop_monitor_base.py",
    BASE_DIR / "´scrapper" / "wallapop_monitor_base.py",
    BASE_DIR / "Â´scrapper" / "wallapop_monitor_base.py",
]
APP_FILE = next((path for path in APP_FILE_CANDIDATES if path.exists()), APP_FILE_CANDIDATES[0])
if not APP_FILE.exists():
    raise RuntimeError(
        "No se encontró wallapop_monitor_base.py. Revisadas rutas: "
        + ", ".join(str(path) for path in APP_FILE_CANDIDATES)
    )

SPEC = spec_from_file_location("wallapop_monitor_base", APP_FILE)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"No se pudo cargar la app desde: {APP_FILE}")

MODULE = module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
app = MODULE.app


if __name__ == "__main__":
    MODULE.init_db()
    MODULE.start_background_monitor_once()
    MODULE.app.run(host="0.0.0.0", port=MODULE.PORT, debug=False)
