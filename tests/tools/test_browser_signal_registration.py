import threading

from tools.browser_tool import _should_register_signal_handlers


def test_browser_tool_signal_registration_allowed_on_main_thread():
    assert isinstance(_should_register_signal_handlers(), bool)


def test_browser_tool_signal_registration_disabled_on_worker_thread():
    result: list[bool] = []

    def worker() -> None:
        result.append(_should_register_signal_handlers())

    thread = threading.Thread(target=worker, name="browser-signal-test")
    thread.start()
    thread.join()

    assert result == [False]
