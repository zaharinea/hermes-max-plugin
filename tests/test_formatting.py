"""Rendering and splitting: 3999/4000/4001, fences, links, escaping."""

from __future__ import annotations

import pytest

from formatting import MAX_TEXT_LENGTH, render_chunks, render_html, split_source


def test_renders_bold_italic_code_link():
    html = render_html("**жирный** *курсив* `code` [ссылка](https://example.com)")
    assert "<b>жирный</b>" in html
    assert "<i>курсив</i>" in html
    assert "<code>code</code>" in html
    assert '<a href="https://example.com">ссылка</a>' in html


def test_escapes_html_in_user_text():
    html = render_html("<script>alert(1)</script> & co")
    assert "<script>" not in html
    assert "&lt;script&gt;" in html
    assert "&amp;" in html


def test_code_fence_becomes_pre():
    html = render_html("```python\nprint('<x>')\n```")
    assert "<pre>" in html
    assert "</code></pre>" in html
    assert "&lt;x&gt;" in html


def test_heading_and_bullets():
    html = render_html("# Заголовок\n- пункт один\n1. первый")
    assert "<b>Заголовок</b>" in html
    assert "• пункт один" in html
    assert "1. первый" in html


@pytest.mark.parametrize("fmt", ["html", "markdown", "plain"])
@pytest.mark.parametrize("size", [3999, 4000, 4001])
def test_chunks_respect_limit(fmt, size):
    chunks = render_chunks("а" * size, fmt, MAX_TEXT_LENGTH)
    assert chunks
    assert all(len(chunk) <= MAX_TEXT_LENGTH for chunk in chunks)
    assert sum(len(chunk) for chunk in chunks) >= size * 0.9


def test_long_text_splits_into_several_messages():
    chunks = render_chunks("слово " * 3000, "html", MAX_TEXT_LENGTH)
    assert len(chunks) > 1
    assert all(len(chunk) <= MAX_TEXT_LENGTH for chunk in chunks)


def test_fence_stays_balanced_across_split():
    source = "```\n" + "\n".join(f"line {i}" for i in range(600)) + "\n```"
    chunks = split_source(source, 1000)
    assert len(chunks) > 1
    for chunk in chunks:
        assert chunk.count("```") % 2 == 0


def test_rendered_html_survives_split_with_tags():
    source = "\n".join(f"**bold {i}** " + "x" * 80 for i in range(80))
    chunks = render_chunks(source, "html", MAX_TEXT_LENGTH)
    assert all(len(chunk) <= MAX_TEXT_LENGTH for chunk in chunks)
    assert all(chunk.count("<b>") == chunk.count("</b>") for chunk in chunks)
