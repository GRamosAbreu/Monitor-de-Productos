from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path


APP_FILE = Path(__file__).resolve().parent / "´scrapper" / "wallapop_monitor_base.py"
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
