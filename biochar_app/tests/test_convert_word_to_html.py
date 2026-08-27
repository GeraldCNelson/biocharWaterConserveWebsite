from bs4 import BeautifulSoup

from biochar_app.markdown.tools.convert_word_to_html import (
    _normalize_captions,
    _promote_table_cell_figures,
)


def test_table_cell_images_become_numbered_figures_with_source_links() -> None:
    soup = BeautifulSoup(
        """
        <figure>
          <img src="first.png">
          <figcaption><p>First figure</p></figcaption>
        </figure>
        <table><tr>
          <th>
            <p><img src="second.png"></p>
            <p>. Lignin structure. Source:
              <a href="https://example.com/lignin">ScienceDirect</a>.
            </p>
          </th>
          <th>
            <p><img src="third.png"></p>
            <p>. Biochar structure. Source:
              <a href="https://example.com/biochar">Chemistry Group</a>.
            </p>
          </th>
        </tr></table>
        """,
        "html.parser",
    )

    _promote_table_cell_figures(soup)
    _normalize_captions(soup)

    table = soup.find("table")
    assert table is not None
    assert "figure-grid" in table.get("class", [])

    captions = soup.find_all("figcaption")
    assert [caption.get_text(" ", strip=True) for caption in captions] == [
        "Figure 1. First figure",
        "Figure 2. Lignin structure. Source: ScienceDirect .",
        "Figure 3. Biochar structure. Source: Chemistry Group .",
    ]
    assert captions[1].find("a", href="https://example.com/lignin") is not None
    assert captions[2].find("a", href="https://example.com/biochar") is not None

