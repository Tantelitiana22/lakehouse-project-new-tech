"""Scoped S3 SigV4 proxy enforcing per-user, per-bucket permissions."""

from __future__ import annotations

import hashlib
import hmac
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from urllib.parse import parse_qsl, quote, unquote

from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from starlette.requests import Request
from starlette.responses import Response

from lakehouse_portal.db import User, get_active_s3_key, user_bucket_level
from lakehouse_portal.s3_client import S3StorageError, S3StorageService

S3_XMLNS = "http://s3.amazonaws.com/doc/2006-03-01/"
AUTH_HEADER_RE = re.compile(
    r"^AWS4-HMAC-SHA256\s+"
    r"Credential=(?P<credential>[^,\s]+),\s*"
    r"SignedHeaders=(?P<signed_headers>[^,\s]+),\s*"
    r"Signature=(?P<signature>[0-9a-fA-F]+)$"
)


@dataclass(frozen=True)
class SigV4Params:
    """Parsed AWS SigV4 credential and signature components."""

    access_key_id: str
    date_stamp: str
    region: str
    service: str
    amz_date: str
    signed_headers: list[str]
    signature: str
    is_presigned: bool


@dataclass(frozen=True)
class _AuthenticatedPrincipal:
    user: User
    is_admin: bool
    bucket_levels: dict[str, str]


def _s3_error(code: str, message: str, status_code: int, resource: str = "/") -> Response:
    root = ET.Element("Error")
    ET.SubElement(root, "Code").text = code
    ET.SubElement(root, "Message").text = message
    ET.SubElement(root, "Resource").text = resource
    body = b'<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(root, encoding="utf-8")
    return Response(content=body, status_code=status_code, media_type="application/xml")


def _aws_uri_encode(value: str, *, encode_slash: bool = True) -> str:
    safe = "-._~" if encode_slash else "/-._~"
    return quote(value, safe=safe)


def _hmac_sha256(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def _parse_header_sigv4(request: Request, auth_header: str) -> SigV4Params | None:
    match = AUTH_HEADER_RE.match(auth_header)
    amz_date = request.headers.get("x-amz-date", "")
    if not match or not amz_date:
        return None
    cred_parts = match.group("credential").split("/")
    if len(cred_parts) != 5 or cred_parts[4] != "aws4_request":
        return None
    return SigV4Params(
        access_key_id=cred_parts[0],
        date_stamp=cred_parts[1],
        region=cred_parts[2],
        service=cred_parts[3],
        amz_date=amz_date,
        signed_headers=[h.strip().lower() for h in match.group("signed_headers").split(";") if h.strip()],
        signature=match.group("signature").lower(),
        is_presigned=False,
    )


def _parse_query_sigv4(request: Request) -> SigV4Params | None:
    query = request.query_params
    if query.get("X-Amz-Algorithm") != "AWS4-HMAC-SHA256":
        return None
    cred_parts = query.get("X-Amz-Credential", "").split("/")
    amz_date = query.get("X-Amz-Date", "")
    signed_raw = query.get("X-Amz-SignedHeaders", "")
    signature = query.get("X-Amz-Signature", "")
    if len(cred_parts) != 5 or not amz_date or not signed_raw or not signature:
        return None
    return SigV4Params(
        access_key_id=cred_parts[0],
        date_stamp=cred_parts[1],
        region=cred_parts[2],
        service=cred_parts[3],
        amz_date=amz_date,
        signed_headers=[h.strip().lower() for h in signed_raw.split(";") if h.strip()],
        signature=signature.lower(),
        is_presigned=True,
    )


def _parse_sigv4(request: Request) -> SigV4Params | None:
    auth_header = request.headers.get("authorization", "").strip()
    if auth_header:
        return _parse_header_sigv4(request, auth_header)
    return _parse_query_sigv4(request)


def _canonical_uri(raw_path: str) -> str:
    path = raw_path
    if path == "/s3":
        path = "/"
    elif path.startswith("/s3/"):
        path = path[3:]
    if not path.startswith("/"):
        path = "/" + path
    return _aws_uri_encode(unquote(path), encode_slash=False)


def _canonical_query(query_string: str, *, exclude_signature: bool) -> str:
    if not query_string:
        return ""
    pairs = parse_qsl(query_string, keep_blank_values=True)
    if exclude_signature:
        pairs = [(k, v) for k, v in pairs if k != "X-Amz-Signature"]
    encoded = [(_aws_uri_encode(k), _aws_uri_encode(v)) for k, v in pairs]
    encoded.sort(key=lambda item: (item[0], item[1]))
    return "&".join(f"{k}={v}" for k, v in encoded)


def _decode_aws_chunked_body(raw_body: bytes) -> bytes:
    """Decode AWS SigV4 streaming chunked payload into raw object bytes."""
    pos = 0
    chunks: list[bytes] = []
    while pos < len(raw_body):
        line_end = raw_body.find(b"\r\n", pos)
        if line_end == -1:
            break
        size_hex = raw_body[pos:line_end].decode("ascii", errors="replace").split(";", 1)[0].strip()
        chunk_size = int(size_hex, 16)
        pos = line_end + 2
        if chunk_size == 0:
            break
        chunks.append(raw_body[pos : pos + chunk_size])
        pos += chunk_size + 2
    return b"".join(chunks)


def _derive_signing_key(secret_access_key: str, params: SigV4Params) -> bytes:
    k_date = _hmac_sha256(f"AWS4{secret_access_key}".encode(), params.date_stamp)
    k_region = _hmac_sha256(k_date, params.region)
    k_service = _hmac_sha256(k_region, params.service)
    return _hmac_sha256(k_service, "aws4_request")


def _build_canonical_request(request: Request, params: SigV4Params, body: bytes) -> str:
    raw_path_bytes = request.scope.get("raw_path")
    raw_path = raw_path_bytes.decode("utf-8") if isinstance(raw_path_bytes, bytes) else request.url.path
    query_bytes = request.scope.get("query_string", b"")
    query_str = query_bytes.decode("utf-8") if isinstance(query_bytes, bytes) else str(request.url.query)

    sorted_headers = sorted(params.signed_headers)
    canonical_headers = "".join(
        f"{name}:{' '.join(request.headers.get(name, '').strip().split())}\n" for name in sorted_headers
    )
    payload_hash = (
        "UNSIGNED-PAYLOAD"
        if params.is_presigned
        else (request.headers.get("x-amz-content-sha256") or hashlib.sha256(body).hexdigest())
    )
    return "\n".join(
        [
            request.method.upper(),
            _canonical_uri(raw_path),
            _canonical_query(query_str, exclude_signature=params.is_presigned),
            canonical_headers,
            ";".join(sorted_headers),
            payload_hash,
        ]
    )


def _verify_sigv4(request: Request, params: SigV4Params, secret_access_key: str, body: bytes) -> bool:
    canonical_request = _build_canonical_request(request, params, body)
    scope = f"{params.date_stamp}/{params.region}/{params.service}/aws4_request"
    string_to_sign = "\n".join(
        [
            "AWS4-HMAC-SHA256",
            params.amz_date,
            scope,
            hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
        ]
    )
    signing_key = _derive_signing_key(secret_access_key, params)
    expected_sig = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected_sig, params.signature)


def _xml_response(root: ET.Element, status_code: int = 200) -> Response:
    body = b'<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(root, encoding="utf-8")
    return Response(content=body, status_code=status_code, media_type="application/xml")


def _list_buckets_xml(user: User, s3: S3StorageService) -> Response:
    root = ET.Element("ListAllMyBucketsResult", xmlns=S3_XMLNS)
    owner = ET.SubElement(root, "Owner")
    ET.SubElement(owner, "ID").text = str(user.id)
    ET.SubElement(owner, "DisplayName").text = user.username
    buckets_el = ET.SubElement(root, "Buckets")
    for bucket in s3.list_buckets():
        if user_bucket_level(user, bucket) in {"read", "read_write"}:
            bucket_el = ET.SubElement(buckets_el, "Bucket")
            ET.SubElement(bucket_el, "Name").text = bucket
            ET.SubElement(bucket_el, "CreationDate").text = "2026-01-01T00:00:00Z"
    return _xml_response(root)


def _list_objects_xml(bucket: str, prefix: str, delimiter: str, max_keys: int, s3: S3StorageService) -> Response:
    result = s3.list_objects(bucket, prefix=prefix, delimiter=delimiter, max_keys=max_keys)
    root = ET.Element("ListBucketResult", xmlns=S3_XMLNS)
    ET.SubElement(root, "Name").text = bucket
    ET.SubElement(root, "Prefix").text = prefix
    ET.SubElement(root, "MaxKeys").text = str(max_keys)
    if delimiter:
        ET.SubElement(root, "Delimiter").text = delimiter
    ET.SubElement(root, "IsTruncated").text = "false"
    ET.SubElement(root, "KeyCount").text = str(len(result.objects) + len(result.prefixes))
    for item in result.objects:
        contents = ET.SubElement(root, "Contents")
        ET.SubElement(contents, "Key").text = item.key
        ET.SubElement(contents, "LastModified").text = item.last_modified
        ET.SubElement(contents, "ETag").text = f'"{item.etag}"'
        ET.SubElement(contents, "Size").text = str(item.size)
        ET.SubElement(contents, "StorageClass").text = "STANDARD"
    for sub_prefix in result.prefixes:
        cp = ET.SubElement(root, "CommonPrefixes")
        ET.SubElement(cp, "Prefix").text = sub_prefix
    return _xml_response(root)


def _handle_batch_delete(bucket: str, body: bytes, s3: S3StorageService) -> Response:
    try:
        req_xml = ET.fromstring(body)
    except ET.ParseError:
        return _s3_error("MalformedXML", "The XML you provided was not well-formed.", 400, f"/{bucket}")
    root = ET.Element("DeleteResult", xmlns=S3_XMLNS)
    for obj_el in req_xml.findall(".//Object"):
        key_el = obj_el.find("Key")
        if key_el is not None and key_el.text:
            s3.delete_object(bucket, key_el.text)
            deleted = ET.SubElement(root, "Deleted")
            ET.SubElement(deleted, "Key").text = key_el.text
    return _xml_response(root)


def _authenticate_s3_request(
    request: Request,
    engine: Engine,
    body: bytes,
) -> tuple[_AuthenticatedPrincipal | None, Response | None]:
    params = _parse_sigv4(request)
    if params is None:
        return None, _s3_error("AccessDenied", "Missing or invalid AWS SigV4 credentials.", 403, request.url.path)

    with Session(engine, expire_on_commit=False) as session:
        key_record = get_active_s3_key(session, params.access_key_id)
        if key_record is None:
            return None, _s3_error(
                "InvalidAccessKeyId",
                "The AWS Access Key Id you provided does not exist in our records.",
                403,
                request.url.path,
            )
        if not _verify_sigv4(request, params, key_record.secret_access_key, body):
            return None, _s3_error(
                "SignatureDoesNotMatch",
                "The request signature we calculated does not match the signature you provided.",
                403,
                request.url.path,
            )
        user = key_record.user
        if not user.is_active:
            return None, _s3_error("AccessDenied", "User account is disabled.", 403, request.url.path)
        levels = {perm.bucket_name: perm.permission_level for perm in user.bucket_permissions}
        return _AuthenticatedPrincipal(user=user, is_admin=user.is_admin, bucket_levels=levels), None


def _handle_bucket_create(bucket: str, is_admin: bool, s3: S3StorageService) -> Response:
    if not is_admin:
        return _s3_error("AccessDenied", f"Admin privileges required to create bucket {bucket}.", 403)
    s3.create_bucket(bucket)
    return Response(status_code=200)


def _handle_bucket_operation(
    request: Request,
    bucket: str,
    effective_level: str | None,
    is_admin: bool,
    body: bytes,
    s3: S3StorageService,
) -> Response:
    method = request.method.upper()
    if method == "PUT":
        return _handle_bucket_create(bucket, is_admin, s3)
    if effective_level not in {"read", "read_write"}:
        return _s3_error("AccessDenied", f"Access Denied for bucket {bucket}.", 403, f"/{bucket}")
    if method == "HEAD":
        return Response(status_code=200 if s3.bucket_exists(bucket) else 404)
    if method == "GET":
        return _list_objects_xml(
            bucket,
            request.query_params.get("prefix", ""),
            request.query_params.get("delimiter", ""),
            int(request.query_params.get("max-keys", "1000")),
            s3,
        )
    if method == "POST" and "delete" in request.query_params:
        return (
            _handle_batch_delete(bucket, body, s3)
            if effective_level == "read_write"
            else _s3_error("AccessDenied", f"Write permission required on bucket {bucket}.", 403)
        )
    return _s3_error("MethodNotAllowed", f"Unsupported bucket operation {method}.", 405, f"/{bucket}")


def _handle_object_read(
    method: str, bucket: str, key: str, effective_level: str | None, s3: S3StorageService
) -> Response:
    if effective_level not in {"read", "read_write"}:
        return _s3_error("AccessDenied", f"Read permission required on bucket {bucket}.", 403, f"/{bucket}/{key}")
    if method == "HEAD":
        size, content_type, etag = s3.head_object(bucket, key)
        return Response(
            status_code=200,
            headers={"Content-Length": str(size), "Content-Type": content_type, "ETag": f'"{etag}"'},
        )
    data, content_type = s3.get_object(bucket, key)
    return Response(content=data, status_code=200, media_type=content_type)


def _handle_object_write(
    request: Request,
    bucket: str,
    key: str,
    effective_level: str | None,
    body: bytes,
    s3: S3StorageService,
) -> Response:
    if effective_level != "read_write":
        return _s3_error("AccessDenied", f"Write permission required on bucket {bucket}.", 403, f"/{bucket}/{key}")
    if request.method.upper() == "DELETE":
        s3.delete_object(bucket, key)
        return Response(status_code=204)
    payload = (
        _decode_aws_chunked_body(body)
        if request.headers.get("x-amz-content-sha256") == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
        else body
    )
    etag = s3.put_object(
        bucket, key, payload, content_type=request.headers.get("content-type", "application/octet-stream")
    )
    return Response(status_code=200, headers={"ETag": f'"{etag}"'})


def _handle_object_operation(
    request: Request,
    bucket: str,
    key: str,
    effective_level: str | None,
    body: bytes,
    s3: S3StorageService,
) -> Response:
    method = request.method.upper()
    if method in {"GET", "HEAD"}:
        return _handle_object_read(method, bucket, key, effective_level, s3)
    if method in {"PUT", "DELETE"}:
        return _handle_object_write(request, bucket, key, effective_level, body, s3)
    return _s3_error("MethodNotAllowed", f"Unsupported method {method}.", 405, f"/{bucket}/{key}")


async def handle_s3_proxy_request(request: Request, engine: Engine, s3: S3StorageService) -> Response:
    """Authenticate an AWS SigV4 request and enforce bucket-level RBAC."""
    body = await request.body()
    principal, error_response = _authenticate_s3_request(request, engine, body)
    if error_response is not None or principal is None:
        return error_response or _s3_error("AccessDenied", "Access Denied.", 403, request.url.path)

    raw_path = request.url.path
    rel_path = raw_path[3:] if raw_path.startswith("/s3") else raw_path
    trimmed = rel_path.lstrip("/")
    if not trimmed:
        if request.method.upper() != "GET":
            return _s3_error("MethodNotAllowed", "Only GET is allowed on service root.", 405, "/")
        return _list_buckets_xml(principal.user, s3)

    bucket, key = trimmed.split("/", 1) if "/" in trimmed else (trimmed, "")
    effective_level = "read_write" if principal.is_admin else principal.bucket_levels.get(bucket)

    try:
        if not key:
            return _handle_bucket_operation(request, bucket, effective_level, principal.is_admin, body, s3)
        return _handle_object_operation(request, bucket, key, effective_level, body, s3)
    except S3StorageError as exc:
        return _s3_error(exc.code, exc.message, exc.status_code, f"/{bucket}/{key}")
