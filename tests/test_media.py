"""Attachment extraction: voice mapping, nested containers, no stray URLs."""

from __future__ import annotations

from media import collect_attachments, collect_media_any


def test_audio_type_maps_to_voice_pipeline():
    message = {
        "body": {
            "attachments": [{"type": "audio", "payload": {"url": "https://cdn.test/a.ogg", "token": "t"}}]
        }
    }
    found = collect_attachments(message)
    assert len(found) == 1
    assert found[0].kind == "audio"


def test_explicit_voice_type_is_audio():
    message = {"body": {"attachments": [{"type": "voice", "payload": {"url": "https://cdn.test/v.ogg"}}]}}
    found = collect_attachments(message)
    assert found[0].kind == "audio"


def test_audio_file_sent_as_document_is_promoted():
    message = {
        "body": {
            "attachments": [
                {
                    "type": "file",
                    "payload": {
                        "url": "https://cdn.test/voice.m4a",
                        "filename": "voice.m4a",
                        "mime": "audio/mp4",
                    },
                }
            ]
        }
    }
    found = collect_attachments(message)
    assert found[0].kind == "audio"
    assert found[0].mime == "audio/mp4"


def test_image_and_file_shapes():
    message = {
        "body": {
            "attachments": [
                {"type": "image", "payload": {"url": "https://cdn.test/i.jpg"}},
                {"type": "file", "payload": {"url": "https://cdn.test/d.pdf", "filename": "d.pdf"}},
            ]
        }
    }
    found = collect_attachments(message)
    assert [item.kind for item in found] == ["image", "file"]


def test_nested_media_is_found_outside_body_attachments():
    update = {
        "message": {
            "voice": {"url": "https://cdn.test/voice.ogg", "token": "tok"},
            "body": {"text": ""},
        }
    }
    found = collect_media_any(update)
    assert len(found) == 1
    assert found[0].kind == "audio"


def test_stray_urls_are_not_downloaded():
    update = {
        "message": {
            "body": {"text": "смотри https://example.com/secret.png"},
            "link": {"url": "https://cdn.test/link", "type": "forward"},
        }
    }
    assert collect_media_any(update) == []


def test_forwarded_attachment_shape():
    update = {
        "message": {
            "link": {
                "type": "forward",
                "message": {
                    "body": {"attachments": [{"type": "image", "payload": {"url": "https://cdn.test/f.jpg"}}]}
                },
            }
        }
    }
    found = collect_media_any(update)
    assert len(found) == 1
    assert found[0].kind == "image"
