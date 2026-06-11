from pathlib import Path

from playwright.sync_api import Playwright, sync_playwright
import socket
import subprocess
import sys
import time

BASE_URL = "http://127.0.0.1:8000"

import socket
import subprocess
import time


def is_server_running(host: str = "127.0.0.1", port: int = 8000) -> bool:
    """
    Return True if the local FastAPI server is already accepting connections.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(1.0)
        return sock.connect_ex((host, port)) == 0


def ensure_server_running() -> subprocess.Popen | None:
    """
    Start the local FastAPI server if it is not already running.

    Returns:
        Popen object if this script started the server.
        None if the server was already running.
    """
    if is_server_running():
        print("Local server already running.")
        return None

    print("Starting local FastAPI server...")

    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "biochar_app.scripts.app:app",
            "--host",
            "127.0.0.1",
            "--port",
            "8000",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    for _ in range(30):
        if is_server_running():
            print("Local server started.")
            return proc
        time.sleep(1)

    proc.terminate()
    raise RuntimeError("Local FastAPI server did not start within 30 seconds.")

def new_page_with_console_capture(p: Playwright):
    console_errors: list[str] = []

    browser = p.chromium.launch(headless=True)
    page = browser.new_page(accept_downloads=True)

    page.on(
        "console",
        lambda msg: console_errors.append(msg.text)
        if msg.type == "error"
        else None,
    )

    return browser, page, console_errors


def assert_no_console_errors(console_errors: list[str]) -> None:
    assert not console_errors, (
        "Console errors found:\n"
        + "\n".join(console_errors)
    )


def assert_filename_contains(
    filename: str,
    expected_parts: list[str],
) -> None:
    filename_lower = filename.lower()

    for part in expected_parts:
        assert part.lower() in filename_lower, (
            f"Expected '{part}' in filename: {filename}"
        )


def download_filename(download) -> str:
    filename = download.suggested_filename
    print(f"Downloaded: {filename}")
    return filename


def assert_download_exists(download) -> Path:
    path = Path(download.path())

    assert path.exists(), (
        f"Downloaded file does not exist: {path}"
    )

    return path


def open_bulk_downloads_tab(page) -> None:
    page.get_by_role("tab", name="Bulk Downloads").click()
    page.locator("#bulk-pane").wait_for(state="visible", timeout=10_000)


def test_home_page() -> None:
    with sync_playwright() as p:
        browser, page, console_errors = new_page_with_console_capture(p)

        page.goto(BASE_URL, wait_until="networkidle")

        print("Title:", page.title())

        assert "Biochar" in page.content()
        assert_no_console_errors(console_errors)

        browser.close()


def test_soil_biology_bulk_download() -> None:
    with sync_playwright() as p:
        browser, page, console_errors = new_page_with_console_capture(p)

        page.goto(BASE_URL, wait_until="networkidle")
        open_bulk_downloads_tab(page)

        button = page.locator("#bulk-download-soil-bio")
        button.wait_for(state="visible", timeout=10_000)

        page.wait_for_function(
            """() => {
                const btn = document.querySelector("#bulk-download-soil-bio");
                return btn && !btn.disabled;
            }""",
            timeout=10_000,
        )

        with page.expect_download() as download_info:
            button.click()

        download = download_info.value
        filename = download_filename(download)

        assert_filename_contains(
            filename,
            ["soil", "bio", "all", ".zip"],
        )
        assert_download_exists(download)
        assert_no_console_errors(console_errors)

        browser.close()


def test_logger_bulk_download() -> None:
    with sync_playwright() as p:
        browser, page, console_errors = new_page_with_console_capture(p)

        page.goto(BASE_URL, wait_until="networkidle")
        open_bulk_downloads_tab(page)

        page.locator("#bulk-year").select_option("2025")
        page.locator("#bulk-granularity").select_option("daily")

        page.wait_for_function(
            """() => {
                const btn = document.querySelector("#bulk-download-loggers");
                return btn && !btn.disabled;
            }""",
            timeout=10_000,
        )

        with page.expect_download() as download_info:
            page.locator("#bulk-download-loggers").click()

        download = download_info.value
        filename = download_filename(download)

        assert_filename_contains(
            filename,
            ["loggers", "2025", "daily", ".zip"],
        )
        assert_download_exists(download)
        assert_no_console_errors(console_errors)

        browser.close()


def test_plot_raw_download() -> None:
    with sync_playwright() as p:
        browser, page, console_errors = new_page_with_console_capture(p)

        page.goto(BASE_URL, wait_until="networkidle")

        page.locator("#monitoringDropdown").click()
        page.locator("#main-tab").click()
        page.locator("#main").wait_for(state="visible", timeout=10_000)

        page.locator("#main-year").select_option("2025")
        page.locator("#main-granularity").select_option("daily")
        page.locator("#main-variable").select_option("VWC")
        page.locator("#main-strip").select_option("S1")
        page.locator("#main-loggerLocation").select_option("T")
        page.locator("#main-depth").select_option("1")
        page.locator("#main-traceOption").select_option("depth")

        page.locator("#update-plots").click()
        page.wait_for_load_state("networkidle")

        page.locator("#downloadDataDropdown").click()

        with page.expect_download() as download_info:
            page.get_by_role("link", name="Raw Data (CSV)").click()

        download = download_info.value
        filename = download_filename(download)

        assert_filename_contains(
            filename,
            ["raw", "vwc", "s1", "loggert", "daily", "2025", ".zip"],
        )
        assert_download_exists(download)
        assert_no_console_errors(console_errors)

        browser.close()


if __name__ == "__main__":
    server_proc = ensure_server_running()

    try:
        test_home_page()
        test_soil_biology_bulk_download()
        test_logger_bulk_download()
        test_plot_raw_download()
        print("✅ Playwright smoke test completed")

    finally:
        if server_proc is not None:
            server_proc.terminate()
            server_proc.wait(timeout=10)
            print("Local server stopped.")