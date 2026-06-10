from playwright.sync_api import sync_playwright


BASE_URL = "http://127.0.0.1:8000"


def test_soil_biology_bulk_download() -> None:
    console_errors: list[str] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(accept_downloads=True)

        page.on(
            "console",
            lambda msg: console_errors.append(msg.text)
            if msg.type == "error"
            else None,
        )

        page.goto(BASE_URL, wait_until="networkidle")

        page.locator("#bulk-tab").click()

        button = page.locator("#bulk-download-soil-bio")
        button.wait_for(state="visible", timeout=10_000)

        # The button starts disabled until the bulk manifest finishes loading.
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
        filename = download.suggested_filename

        print("Downloaded:", filename)

        assert filename.endswith(".zip")
        assert "soil" in filename.lower()
        assert "bio" in filename.lower()

        assert not console_errors, (
            "Console errors found:\n" + "\n".join(console_errors)
        )

        browser.close()
        
def test_logger_bulk_download() -> None:
    console_errors: list[str] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(accept_downloads=True)

        page.on(
            "console",
            lambda msg: console_errors.append(msg.text)
            if msg.type == "error"
            else None,
        )

        page.goto(BASE_URL, wait_until="networkidle")

        page.get_by_role("tab", name="Bulk Downloads").click()
        page.locator("#bulk-pane").wait_for(state="visible", timeout=10_000)

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
        filename = download.suggested_filename

        print("Downloaded:", filename)

        assert filename.endswith(".zip")
        assert "loggers" in filename.lower()
        assert "2025" in filename
        assert "daily" in filename.lower()

        assert not console_errors, (
            "Console errors found:\n" + "\n".join(console_errors)
        )

        browser.close()

def test_home_page() -> None:
    console_errors: list[str] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        page.on(
            "console",
            lambda msg: console_errors.append(msg.text)
            if msg.type == "error"
            else None,
        )

        page.goto(BASE_URL, wait_until="networkidle")
        print("Title:", page.title())
        assert "Biochar" in page.content()
        assert not console_errors, (
            "Console errors found:\n" + "\n".join(console_errors)
        )
        browser.close()


if __name__ == "__main__":
    test_home_page()
    test_soil_biology_bulk_download()
    test_logger_bulk_download()
    print("✅ Playwright smoke test completed")