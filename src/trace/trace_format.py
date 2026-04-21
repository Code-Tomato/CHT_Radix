#!/usr/bin/env python3

import hashlib
import json
import struct
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, Optional, Tuple

MAGIC = 0x4C4C4D54  # "LLMT"
VERSION = 2

OP_INSERT = 0
OP_LOOKUP = 1
OP_REMOVE = 2

# v2 header layout:
#   uint32 magic
#   uint32 version
#   uint64 num_ops
#   uint32 provenance_len
#   uint32 reserved = 0
#   provenance_len bytes of UTF-8 JSON (padded to 8-byte boundary)
#   records...
#
# v1 header (legacy, readable only):
#   uint32 magic
#   uint32 version = 1
#   uint64 num_ops
#   records...

TRACE_V1_HEADER = struct.Struct("<IIQ")
TRACE_V2_PROVENANCE = struct.Struct("<II")
TRACE_RECORD = struct.Struct("<B7xQQ")


def _aligned_len(n: int, align: int = 8) -> int:
  pad = (align - (n % align)) % align
  return n + pad


def write_trace(
    path: Path,
    ops: Iterable[Tuple[int, int, int]],
    provenance: Optional[Dict[str, Any]] = None,
) -> int:
  ops_list = list(ops)
  payload = json.dumps(provenance or {}, sort_keys=True).encode("utf-8")
  padded_len = _aligned_len(len(payload))
  padding = b"\x00" * (padded_len - len(payload))

  path.parent.mkdir(parents=True, exist_ok=True)
  with path.open("wb") as f:
    f.write(TRACE_V1_HEADER.pack(MAGIC, VERSION, len(ops_list)))
    f.write(TRACE_V2_PROVENANCE.pack(padded_len, 0))
    f.write(payload)
    f.write(padding)
    for op_type, key, value in ops_list:
      f.write(TRACE_RECORD.pack(op_type, key, value))
  return len(ops_list)


def read_header(path: Path) -> Dict[str, Any]:
  with path.open("rb") as f:
    hdr = f.read(TRACE_V1_HEADER.size)
    if len(hdr) != TRACE_V1_HEADER.size:
      raise ValueError("trace header is truncated")
    magic, version, num_ops = TRACE_V1_HEADER.unpack(hdr)
    if magic != MAGIC:
      raise ValueError(f"unexpected magic 0x{magic:08x}")
    provenance: Dict[str, Any] = {}
    if version == 2:
      prov = f.read(TRACE_V2_PROVENANCE.size)
      if len(prov) != TRACE_V2_PROVENANCE.size:
        raise ValueError("provenance header is truncated")
      padded_len, _reserved = TRACE_V2_PROVENANCE.unpack(prov)
      payload = f.read(padded_len)
      if len(payload) != padded_len:
        raise ValueError("provenance payload is truncated")
      try:
        provenance = json.loads(payload.rstrip(b"\x00").decode("utf-8") or "{}")
      except Exception:
        provenance = {"_parse_error": True}
    elif version != 1:
      raise ValueError(f"unsupported trace version {version}")
  return {
      "magic": magic,
      "version": version,
      "num_ops": num_ops,
      "provenance": provenance,
  }


def iter_trace(path: Path) -> Iterator[Tuple[int, int, int]]:
  with path.open("rb") as f:
    hdr = f.read(TRACE_V1_HEADER.size)
    if len(hdr) != TRACE_V1_HEADER.size:
      raise ValueError("trace header is truncated")
    magic, version, num_ops = TRACE_V1_HEADER.unpack(hdr)
    if magic != MAGIC:
      raise ValueError(f"unexpected magic 0x{magic:08x}")
    if version == 2:
      prov = f.read(TRACE_V2_PROVENANCE.size)
      if len(prov) != TRACE_V2_PROVENANCE.size:
        raise ValueError("provenance header is truncated")
      padded_len, _reserved = TRACE_V2_PROVENANCE.unpack(prov)
      f.seek(padded_len, 1)
    elif version != 1:
      raise ValueError(f"unsupported trace version {version}")
    for _ in range(num_ops):
      rec = f.read(TRACE_RECORD.size)
      if len(rec) != TRACE_RECORD.size:
        raise ValueError("trace body is truncated")
      yield TRACE_RECORD.unpack(rec)


def content_sha256(path: Path) -> str:
  """SHA256 over just the record content, skipping the provenance header.

  We deliberately skip the v2 provenance blob because it contains a
  `generated_at` timestamp that makes every extraction byte-distinct.
  What reviewers actually want to check is 'did I get the same *data*',
  so we hash num_ops followed by the packed op records.
  """
  h = hashlib.sha256()
  with path.open("rb") as f:
    hdr = f.read(TRACE_V1_HEADER.size)
    if len(hdr) != TRACE_V1_HEADER.size:
      raise ValueError("trace header is truncated")
    magic, version, num_ops = TRACE_V1_HEADER.unpack(hdr)
    if magic != MAGIC:
      raise ValueError(f"unexpected magic 0x{magic:08x}")
    if version == 2:
      prov = f.read(TRACE_V2_PROVENANCE.size)
      if len(prov) != TRACE_V2_PROVENANCE.size:
        raise ValueError("provenance header is truncated")
      padded_len, _reserved = TRACE_V2_PROVENANCE.unpack(prov)
      f.seek(padded_len, 1)
    elif version != 1:
      raise ValueError(f"unsupported trace version {version}")
    # Hash num_ops first so truncation is detectable even if remaining body
    # is padded to the same shape somehow.
    h.update(num_ops.to_bytes(8, "little"))
    remaining = num_ops * TRACE_RECORD.size
    while remaining > 0:
      chunk = f.read(min(1 << 20, remaining))
      if not chunk:
        raise ValueError("trace body is truncated")
      h.update(chunk)
      remaining -= len(chunk)
  return h.hexdigest()
