"""IBT (iRacing telemetry) parser.

Format notes (from iRacing SDK):
- Telemetry header is 112 bytes (28 int32s).
- Variable headers are 144 bytes each.
- Disk header is 32 bytes (int64 + 2x float64 + 2x int32).
"""
from __future__ import annotations

from dataclasses import dataclass
import os
import struct
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple


# iRacing var types
VAR_TYPE_CHAR = 0
VAR_TYPE_BOOL = 1
VAR_TYPE_INT = 2
VAR_TYPE_BITFIELD = 3
VAR_TYPE_FLOAT = 4
VAR_TYPE_DOUBLE = 5

VAR_TYPE_FORMATS = {
    VAR_TYPE_CHAR: ("c", 1),
    VAR_TYPE_BOOL: ("?", 1),
    VAR_TYPE_INT: ("i", 4),
    VAR_TYPE_BITFIELD: ("I", 4),
    VAR_TYPE_FLOAT: ("f", 4),
    VAR_TYPE_DOUBLE: ("d", 8),
}


@dataclass(frozen=True)
class VarBuf:
    tick_count: int
    buf_offset: int
    pad1: int
    pad2: int


@dataclass(frozen=True)
class TelemetryHeader:
    version: int
    status: int
    tick_rate: int
    session_info_update: int
    session_info_len: int
    session_info_offset: int
    num_vars: int
    var_header_offset: int
    num_buf: int
    buf_len: int
    pad1: int
    pad2: int
    var_bufs: Tuple[VarBuf, VarBuf, VarBuf, VarBuf]


@dataclass(frozen=True)
class DiskHeader:
    start_time: int
    session_start_time: float
    session_end_time: float
    session_lap_count: int
    record_count: int


@dataclass(frozen=True)
class VarHeader:
    var_type: int
    offset: int
    count: int
    count_as_time: int
    name: str
    desc: str
    unit: str


class IBTReader:
    def __init__(self, path: str) -> None:
        self.path = path
        self.header: Optional[TelemetryHeader] = None
        self.disk_header: Optional[DiskHeader] = None
        self.var_headers: List[VarHeader] = []
        self.var_by_name: Dict[str, VarHeader] = {}
        self.session_info: Optional[str] = None

    def read(self) -> "IBTReader":
        with open(self.path, "rb") as f:
            self.header = self._read_header(f)
            self.disk_header = self._read_disk_header(f)
            self.var_headers = self._read_var_headers(f, self.header)
            self.var_by_name = {vh.name: vh for vh in self.var_headers}
            self.session_info = self._read_session_info(f, self.header)
        return self

    def get_var(self, name: str) -> VarHeader:
        if not self.var_by_name:
            raise ValueError("IBTReader.read() must be called before accessing variables")
        try:
            return self.var_by_name[name]
        except KeyError as exc:
            raise KeyError(f"Unknown variable: {name}") from exc

    def iter_record_bytes(self) -> Iterator[bytes]:
        if not self.header or not self.disk_header:
            raise ValueError("IBTReader.read() must be called before iterating records")
        var_buf = self.header.var_bufs[0]
        buf_len = self.header.buf_len
        record_count = self.disk_header.record_count
        with open(self.path, "rb") as f:
            f.seek(var_buf.buf_offset)
            for _ in range(record_count):
                chunk = f.read(buf_len)
                if len(chunk) != buf_len:
                    break
                yield chunk

    def iter_records(self, channels: Optional[Sequence[str]] = None) -> Iterator[Dict[str, object]]:
        if not self.var_by_name:
            raise ValueError("IBTReader.read() must be called before iterating records")
        if channels is None:
            channel_vars = self.var_headers
        else:
            channel_vars = [self.get_var(name) for name in channels]

        parsers = [_build_parser(vh) for vh in channel_vars]
        names = [vh.name for vh in channel_vars]

        for record in self.iter_record_bytes():
            values = {}
            for name, parser in zip(names, parsers):
                values[name] = parser(record)
            yield values

    def read_channel(self, name: str) -> List[object]:
        values: List[object] = []
        for record in self.iter_records([name]):
            values.append(record[name])
        return values

    def _read_header(self, f) -> TelemetryHeader:
        raw = f.read(112)
        if len(raw) != 112:
            raise ValueError("File too small to contain telemetry header")
        ints = struct.unpack("<28i", raw)
        (version, status, tick_rate, session_info_update, session_info_len,
         session_info_offset, num_vars, var_header_offset, num_buf, buf_len,
         pad1, pad2) = ints[:12]
        var_bufs = []
        idx = 12
        for _ in range(4):
            tick_count, buf_offset, pad1b, pad2b = ints[idx:idx+4]
            var_bufs.append(VarBuf(tick_count, buf_offset, pad1b, pad2b))
            idx += 4
        return TelemetryHeader(
            version=version,
            status=status,
            tick_rate=tick_rate,
            session_info_update=session_info_update,
            session_info_len=session_info_len,
            session_info_offset=session_info_offset,
            num_vars=num_vars,
            var_header_offset=var_header_offset,
            num_buf=num_buf,
            buf_len=buf_len,
            pad1=pad1,
            pad2=pad2,
            var_bufs=tuple(var_bufs),
        )

    def _read_disk_header(self, f) -> DiskHeader:
        raw = f.read(32)
        if len(raw) != 32:
            raise ValueError("File too small to contain disk header")
        start_time, session_start_time, session_end_time, session_lap_count, record_count = struct.unpack("<qddii", raw)
        return DiskHeader(
            start_time=start_time,
            session_start_time=session_start_time,
            session_end_time=session_end_time,
            session_lap_count=session_lap_count,
            record_count=record_count,
        )

    def _read_var_headers(self, f, header: TelemetryHeader) -> List[VarHeader]:
        f.seek(header.var_header_offset)
        vars_out = []
        for _ in range(header.num_vars):
            raw = f.read(144)
            if len(raw) != 144:
                raise ValueError("Unexpected end of file while reading variable headers")
            var_type, offset, count, count_as_time = struct.unpack("<4i", raw[:16])
            name = raw[16:48].split(b"\x00", 1)[0].decode("ascii", "ignore")
            desc = raw[48:112].split(b"\x00", 1)[0].decode("ascii", "ignore")
            unit = raw[112:144].split(b"\x00", 1)[0].decode("ascii", "ignore")
            vars_out.append(VarHeader(
                var_type=var_type,
                offset=offset,
                count=count,
                count_as_time=count_as_time,
                name=name,
                desc=desc,
                unit=unit,
            ))
        return vars_out

    def _read_session_info(self, f, header: TelemetryHeader) -> str:
        f.seek(header.session_info_offset)
        raw = f.read(header.session_info_len)
        return raw.decode("utf-8", "ignore").rstrip("\x00")


def _build_parser(vh: VarHeader):
    fmt, size = VAR_TYPE_FORMATS.get(vh.var_type, (None, None))
    if fmt is None:
        raise ValueError(f"Unknown var type: {vh.var_type} for {vh.name}")

    if vh.var_type == VAR_TYPE_CHAR:
        byte_len = vh.count * size
        def parse(record: bytes):
            raw = record[vh.offset:vh.offset + byte_len]
            return raw.split(b"\x00", 1)[0].decode("ascii", "ignore")
        return parse

    count = vh.count
    struct_fmt = f"<{count}{fmt}" if count > 1 else f"<{fmt}"
    parser = struct.Struct(struct_fmt)

    def parse(record: bytes):
        data = parser.unpack_from(record, vh.offset)
        if count == 1:
            return data[0]
        return list(data)

    return parse
