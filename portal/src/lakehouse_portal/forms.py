"""Dependency-free URL-encoded and multipart form parser for Starlette."""

from __future__ import annotations

from dataclasses import dataclass, field
from email.parser import BytesParser
from email.policy import HTTP
from urllib.parse import parse_qs

from starlette.requests import Request


@dataclass
class ParsedForm:
    """Parsed HTML form fields and uploaded files."""

    fields: dict[str, str] = field(default_factory=dict)
    files: dict[str, tuple[str, bytes, str]] = field(default_factory=dict)


async def parse_request_form(request: Request) -> ParsedForm:
    """Parse ``application/x-www-form-urlencoded`` or ``multipart/form-data`` bodies."""
    content_type = request.headers.get("content-type", "")
    body = await request.body()
    result = ParsedForm()

    if content_type.startswith("application/x-www-form-urlencoded"):
        parsed = parse_qs(body.decode("utf-8"), keep_blank_values=True)
        for key, values in parsed.items():
            if values:
                result.fields[key] = values[0]
        return result

    if content_type.startswith("multipart/form-data"):
        raw = b"Content-Type: " + content_type.encode("latin-1") + b"\r\n\r\n" + body
        message = BytesParser(policy=HTTP).parsebytes(raw)
        if not message.is_multipart():
            return result
        for part in message.iter_parts():
            name = part.get_param("name", header="content-disposition")
            if not isinstance(name, str):
                continue
            filename = part.get_filename()
            payload = part.get_payload(decode=True)
            payload_bytes = payload if isinstance(payload, bytes) else b""
            if filename is not None:
                part_ct = part.get_content_type() or "application/octet-stream"
                result.files[name] = (filename, payload_bytes, part_ct)
            else:
                result.fields[name] = payload_bytes.decode("utf-8", errors="replace")
        return result

    return result
