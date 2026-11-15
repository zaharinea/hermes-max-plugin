"""HTML rendering and source-level splitting for MAX messages.

MAX accepts ``format: "html"`` and counts the *rendered* text against the 4000
character limit, so messages are split on the source markdown first and each
chunk is rendered separately. ``truncate_message`` is deliberately not used: it
understands Markdown fences, not HTML tags.
"""

from __future__ import annotations

import html
import re
from typing import Iterable

MAX_TEXT_LENGTH = 4000
# Leave room for the tags a chunk gains during rendering.
_HTML_SAFETY = 0.9

_FENCE_RE = re.compile(r"^\s*```")
_BOLD_RE = re.compile(r"\*\*(.+?)\*\*", re.S)
_ITALIC_RE = re.compile(r"(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)", re.S)
_CODE_RE = re.compile(r"`([^`\n]+)`")
_LINK_RE = re.compile(r"\[([^\]\n]+)\]\((https?://[^)\s]+)\)")
_HEADING_RE = re.compile(r"^\s{0,3}(#{1,6})\s+(.*)$")
_BULLET_RE = re.compile(r"^\s*[-*+]\s+(.*)$")
_NUMBERED_RE = re.compile(r"^\s*(\d+)[.)]\s+(.*)$")
_MEDIA_REF_RE = re.compile(r"^MEDIA:\S+", re.M)


def _render_inline(line: str) -> str:
    text = html.escape(line, quote=False)
    text = _CODE_RE.sub(lambda m: f"<code>{m.group(1)}</code>", text)
    text = _LINK_RE.sub(lambda m: f'<a href="{m.group(2)}">{m.group(1)}</a>', text)
    text = _BOLD_RE.sub(lambda m: f"<b>{m.group(1)}</b>", text)
    text = _ITALIC_RE.sub(lambda m: f"<i>{m.group(1)}</i>", text)
    return text


def render_html(content: str) -> str:
    """Render a markdown-ish chunk as the HTML subset MAX understands."""
    out: list[str] = []
    in_fence = False
    for line in (content or "").splitlines():
        if _FENCE_RE.match(line):
            if not in_fence:
                in_fence = True
                lang = line.strip()[3:].strip()
                if lang:
                    out.append(f'<pre><code class="language-{html.escape(lang)}">')
                else:
                    out.append("<pre>")
            else:
                out.append("</code></pre>")
                in_fence = False
            continue
        if in_fence:
            out.append(html.escape(line, quote=False))
            continue
        heading = _HEADING_RE.match(line)
        if heading:
            out.append(f"<b>{_render_inline(heading.group(2).strip())}</b>")
            continue
        bullet = _BULLET_RE.match(line)
        if bullet:
            out.append(f"• {_render_inline(bullet.group(1))}")
            continue
        numbered = _NUMBERED_RE.match(line)
        if numbered:
            out.append(f"{numbered.group(1)}. {_render_inline(numbered.group(2))}")
            continue
        out.append(_render_inline(line))
    if in_fence:
        out.append("</code></pre>")
    return "\n".join(out)


def _split_plain(content: str, limit: int) -> list[str]:
    """Split at paragraph/line boundaries; never inside a fenced block."""
    if limit <= 0:
        return [content]
    chunks: list[str] = []
    current: list[str] = []
    size = 0
    fence_open = False

    def flush() -> None:
        nonlocal current, size
        if current:
            chunks.append("\n".join(current).rstrip())
            current = []
            size = 0

    for line in (content or "").splitlines():
        line_len = len(line) + 1
        if _FENCE_RE.match(line):
            if fence_open:
                fence_open = False
            else:
                # A fence cannot span chunks; start it on a fresh one if needed.
                if not current:
                    fence_open = True
                elif size + line_len > limit:
                    # close this chunk before the fence, reopen text afterwards
                    flush()
                    fence_open = True
                else:
                    fence_open = True
        if current and size + line_len > limit and not fence_open:
            flush()
        if line_len > limit and len(line) > limit:
            # A single pathological line: hard-split it.
            flush()
            chunks.extend(line[i : i + limit] for i in range(0, len(line), limit))
            continue
        current.append(line)
        size += line_len
    flush()
    return chunks or [""]


def split_source(content: str, limit: int) -> list[str]:
    """Chunk source text without cutting through an open code fence."""
    text = content or ""
    if len(text) <= limit:
        return [text]
    raw = _split_plain(text, max(1, limit))
    # Reassemble so that fences stay balanced across the boundary.
    balanced: list[str] = []
    carry_open = False
    for chunk in raw:
        fences = len(_FENCE_RE.findall(chunk)) if hasattr(_FENCE_RE, "findall") else 0
        del fences  # counting via lines below keeps it simple
        lines = chunk.splitlines()
        opens = sum(1 for line in lines if _FENCE_RE.match(line))
        if carry_open:
            chunk = "```\n" + chunk
            opens += 1
        if opens % 2 == 1:
            chunk = chunk + "\n```"
            carry_open = True
        else:
            carry_open = False
        balanced.append(chunk)
    if carry_open and balanced:
        balanced[-1] = balanced[-1] + "\n```"
    return balanced


def render_chunks(content: str, fmt: str, limit: int = MAX_TEXT_LENGTH) -> list[str]:
    """Return chunks that respect ``limit`` in the rendered representation."""
    text = _MEDIA_REF_RE.sub("", content or "").strip() or (content or "").strip()
    if not text:
        return []
    fmt = (fmt or "html").lower()
    if fmt in {"plain", "markdown"}:
        return split_source(text, limit)
    target = max(64, int(limit * _HTML_SAFETY))
    chunks = split_source(text, target)
    rendered: list[str] = []
    for chunk in chunks:
        html_chunk = render_html(chunk)
        if len(html_chunk) <= limit:
            rendered.append(html_chunk)
            continue
        # Shrink the source until the rendered form fits.
        piece_size = max(64, int(len(chunk) * limit / max(1, len(html_chunk)) * 0.9))
        for piece in split_source(chunk, piece_size):
            rendered_piece = render_html(piece)
            if len(rendered_piece) <= limit:
                rendered.append(rendered_piece)
            else:
                rendered.append(rendered_piece[:limit])
    return rendered


def iter_media_refs(content: str) -> Iterable[str]:
    """Absolute paths named by ``MEDIA:/path`` markers (agent media contract)."""
    for match in re.finditer(r"(?m)^MEDIA:(\S+)\s*$", content or ""):
        yield match.group(1)
