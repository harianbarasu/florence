import importlib
import threading


def test_browser_tool_import_is_safe_on_worker_thread():
    errors: list[BaseException] = []

    def worker() -> None:
        try:
            module = importlib.import_module("tools.browser_tool")
            importlib.reload(module)
        except BaseException as exc:  # pragma: no cover - diagnostic collection
            errors.append(exc)

    thread = threading.Thread(target=worker, name="browser-signal-test")
    thread.start()
    thread.join()

    assert errors == []
