#!/usr/bin/env python3
"""
Shared Access-pipeline helpers used across download, extraction, dictionary,
and harmonization stages.

Reads:
- environment configuration such as `IPEDSDB_ROOT`
- table names, column names, and file paths passed in by stage scripts

Writes:
- no durable project artifacts directly

Focus:
- external data-root layout
- naming normalization
- metadata-table classification
- small file and subprocess helpers used by multiple stages
"""
from __future__ import annotations

import csv
import hashlib
import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

DEFAULT_IPEDSDB_ROOT = Path("/Users/markjaysonfarol13/Projects/IPEDSDB_Paneling")
REPO_ROOT = Path(__file__).resolve().parents[1]

NULL_LIKE_TOKENS = {"", "nan", "none", "<na>", "na", "nat"}
VAR_NUMBER_CANDIDATES = {
    "varnumber",
    "var_num",
    "number",
    "variable_number",
    "var no",
    "varno",
}
VAR_NAME_CANDIDATES = {
    "varname",
    "var_name",
    "name",
    "variable",
    "variable_name",
}
VAR_TITLE_CANDIDATES = {
    "vartitle",
    "var_title",
    "var title",
    "title",
    "label",
    "variable_label",
    "variable title",
}
LONG_DESC_CANDIDATES = {
    "longdescription",
    "long description",
    "description",
    "description_text",
    "definition",
}
CODE_VALUE_CANDIDATES = {
    "codevalue",
    "code value",
    "code",
    "value",
}
VALUE_LABEL_CANDIDATES = {
    "valuelabel",
    "value label",
    "label",
    "description",
}
DATA_TABLE_CANDIDATES = {
    "table",
    "tablename",
    "table_name",
    "data_table",
    "data table",
    "source_table",
    "source table",
    "file",
    "filename",
    "file_name",
}
IMPUTATION_CANDIDATES = {
    "imputationvar",
    "imputation var",
    "imputation_var",
    "impvar",
}


@dataclass(frozen=True)
class DataRootLayout:
    root: Path
    raw_access: Path
    dictionary: Path
    cross_sections: Path
    panels: Path
    checks: Path
    build: Path


def repo_root() -> Path:
    return REPO_ROOT


def data_root() -> Path:
    return Path(os.environ.get("IPEDSDB_ROOT", str(DEFAULT_IPEDSDB_ROOT))).expanduser()


def data_layout(root: str | Path | None = None) -> DataRootLayout:
    base = Path(root).expanduser() if root is not None else data_root()
    return DataRootLayout(
        root=base,
        raw_access=base / "Raw_Access_Databases",
        dictionary=base / "Dictionary",
        cross_sections=base / "Cross_sections",
        panels=base / "Panels",
        checks=base / "Checks",
        build=base / "build",
    )


def ensure_data_layout(root: str | Path | None = None) -> DataRootLayout:
    layout = data_layout(root)
    for path in (
        layout.root,
        layout.raw_access,
        layout.dictionary,
        layout.cross_sections,
        layout.panels,
        layout.checks,
        layout.build,
    ):
        path.mkdir(parents=True, exist_ok=True)
    return layout


def parse_years(spec: str) -> list[int]:
    if ":" in spec:
        start, end = spec.split(":", 1)
        return list(range(int(start), int(end) + 1))
    return [int(part.strip()) for part in spec.split(",") if part.strip()]


def academic_year_to_start_year(label: str) -> int:
    match = re.fullmatch(r"\s*(\d{4})\s*[-/]\s*(\d{2,4})\s*", str(label))
    if not match:
        raise ValueError(f"Unsupported academic year label: {label}")
    return int(match.group(1))


def start_year_to_academic_label(year: int) -> str:
    return f"{int(year)}-{(int(year) + 1) % 100:02d}"


def normalize_token(value: object) -> str:
    txt = str(value or "").strip().upper()
    txt = re.sub(r"[^A-Z0-9]+", "_", txt)
    txt = re.sub(r"_+", "_", txt).strip("_")
    return txt


def normalize_text_key(value: object) -> str:
    txt = str(value or "").strip().lower()
    txt = re.sub(r"[^a-z0-9]+", "_", txt)
    txt = re.sub(r"_+", "_", txt).strip("_")
    return txt


def normalize_varnumber(val: object) -> str:
    if val is None:
        return ""
    txt = re.sub(r"\s+", "", str(val))
    if txt.lower() in NULL_LIKE_TOKENS:
        return ""
    if txt.isdigit():
        return txt.zfill(8)
    return txt


def clean_source_label(label: str) -> str:
    txt = str(label or "")
    txt = re.sub(r"(?i)\bfile documentation for (the )?\b", "", txt)
    txt = re.sub(r"\b20\d{2}\s*[-/]\s*\d{2}\b", "", txt)
    txt = re.sub(r"\s+", " ", txt).strip(" ,")
    return txt


def normalize_table_name(name: str) -> str:
    return normalize_token(Path(str(name)).stem)


def canonical_source_file(name: str) -> str:
    norm = normalize_table_name(name)
    if not norm:
        return ""
    if "GR200" in norm:
        return "GR200"
    stripped = re.sub(r"\d+", "", norm)
    stripped = re.sub(r"_+", "_", stripped).strip("_")
    if not stripped:
        stripped = norm
    compact = stripped.replace("_", "")
    alias_map = {
        "HD": "HD",
        "IC": "IC",
        "ICAY": "IC_AY",
        "ICPY": "IC_PY",
        "ICCAMPUSES": "IC_CAMPUSES",
        "ICPCCAMPUSES": "IC_PCCAMPUSES",
        "ADM": "ADM",
        "AL": "AL",
        "CA": "C_A",
        "CB": "C_B",
        "CC": "C_C",
        "CDEP": "CDEP",
        "COST": "COST",
        "EAP": "EAP",
        "EFA": "EFA",
        "EFADIST": "EFA_DIST",
        "EFB": "EFB",
        "EFC": "EFC",
        "EFCP": "EFCP",
        "EFFY": "EFFY",
        "EFFYDIST": "EFFY_DIST",
        "EFIA": "EFIA",
        "EFIB": "EFIB",
        "EFIC": "EFIC",
        "EFID": "EFID",
        "FF": "F_F",
        "FFA": "F_FA",
        "FFAF": "F_FA_F",
        "FFAG": "F_FA_G",
        "GR": "GR",
        "GRPELLSSL": "GR_PELL_SSL",
        "OM": "OM",
        "SALA": "SAL_A",
        "SALALT": "SAL_A_LT",
        "SALB": "SAL_B",
        "SALFACULTY": "SAL_FACULTY",
        "SALIS": "SAL_IS",
        "SABD": "S_ABD",
        "SCN": "S_CN",
        "SF": "S_F",
        "SG": "S_G",
        "SIS": "S_IS",
        "SNH": "S_NH",
        "SOC": "S_OC",
        "SSIS": "S_SIS",
        "SFA": "SFA",
        "SFAV": "SFAV",
        "DRVADM": "DRVADM",
        "DRVAL": "DRVAL",
        "DRVC": "DRVC",
        "DRVEF": "DRVEF",
        "DRVEF12": "DRVEF12",
        "DRVF": "DRVF",
        "DRVGR": "DRVGR",
        "DRVHR": "DRVHR",
        "DRVIC": "DRVIC",
        "DRVOM": "DRVOM",
    }
    return alias_map.get(compact, stripped)


def compute_file_metadata(path: str | Path) -> tuple[str, str]:
    fp = Path(path)
    if not fp.exists():
        return "", ""
    sha256 = hashlib.sha256()
    size = 0
    with fp.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            size += len(chunk)
            sha256.update(chunk)
    return str(size), sha256.hexdigest()


def safe_table_filename(table_name: str) -> str:
    txt = str(table_name).replace("/", "_").replace("\x00", "")
    txt = txt.replace(":", "_")
    return txt


def csv_header_and_rowcount(path: str | Path) -> tuple[list[str], int]:
    fp = Path(path)
    if not fp.exists():
        return [], 0
    with fp.open("r", newline="", encoding="utf-8", errors="replace") as fh:
        reader = csv.reader(fh)
        header = next(reader, [])
        rows = sum(1 for _ in reader)
    return header, rows


def pick_column(columns: Iterable[str], candidates: set[str]) -> str | None:
    lookup = {normalize_text_key(col): col for col in columns}
    for candidate in candidates:
        key = normalize_text_key(candidate)
        if key in lookup:
            return lookup[key]
    return None


def has_any_column(columns: Iterable[str], candidates: set[str]) -> bool:
    return pick_column(columns, candidates) is not None


def table_role_capabilities(table_name: str, columns: Iterable[str]) -> dict[str, bool | str]:
    col_list = list(columns)
    norm_cols = {normalize_text_key(col) for col in col_list}
    name_key = normalize_text_key(table_name)
    return {
        "table_name_key": name_key,
        "has_unitid": "unitid" in norm_cols,
        "has_varnumber": has_any_column(col_list, VAR_NUMBER_CANDIDATES),
        "has_varname": has_any_column(col_list, VAR_NAME_CANDIDATES),
        "has_vartitle": has_any_column(col_list, VAR_TITLE_CANDIDATES),
        "has_longdesc": has_any_column(col_list, LONG_DESC_CANDIDATES),
        "has_code": has_any_column(col_list, CODE_VALUE_CANDIDATES),
        "has_label": has_any_column(col_list, VALUE_LABEL_CANDIDATES),
        "has_imputation": "imput" in name_key or any("imput" in col for col in norm_cols),
    }


def can_serve_metadata_role(role: str, table_name: str, columns: Iterable[str]) -> bool:
    caps = table_role_capabilities(table_name, columns)
    return can_serve_metadata_role_from_capabilities(role, caps)


def can_serve_metadata_role_from_capabilities(role: str, capabilities: dict[str, object]) -> bool:
    has_varnumber = bool(capabilities.get("has_varnumber"))
    has_varname = bool(capabilities.get("has_varname"))
    has_vartitle = bool(capabilities.get("has_vartitle"))
    has_longdesc = bool(capabilities.get("has_longdesc"))
    has_code = bool(capabilities.get("has_code"))
    has_label = bool(capabilities.get("has_label"))
    has_imputation = bool(capabilities.get("has_imputation"))
    if role == "metadata_varlist":
        return has_varnumber and has_varname and has_vartitle
    if role == "metadata_description":
        return has_longdesc and (has_varname or has_varnumber)
    if role == "metadata_codes":
        return has_code and has_label
    if role == "metadata_imputation":
        return has_code and has_label and has_imputation
    return False


def classify_table_role(table_name: str, columns: Iterable[str]) -> str:
    caps = table_role_capabilities(table_name, columns)
    name_key = str(caps["table_name_key"])
    has_unitid = bool(caps["has_unitid"])
    has_varnumber = bool(caps["has_varnumber"])
    has_varname = bool(caps["has_varname"])
    has_vartitle = bool(caps["has_vartitle"])
    has_longdesc = bool(caps["has_longdesc"])
    has_code = bool(caps["has_code"])
    has_label = bool(caps["has_label"])
    has_imputation = bool(caps["has_imputation"])

    if has_unitid and not (has_code or has_longdesc):
        return "data"
    if has_code and has_label and has_imputation:
        return "metadata_imputation"
    if has_code and has_label:
        return "metadata_codes"
    if has_varname and has_varnumber and has_vartitle:
        return "metadata_varlist"
    if has_longdesc and (has_varname or has_varnumber):
        return "metadata_description"
    if any(token in name_key for token in ("varlist", "layout", "variables")):
        return "metadata_varlist"
    if "descript" in name_key:
        return "metadata_description"
    if any(token in name_key for token in ("valueset", "frequenc", "code", "label")):
        return "metadata_codes"
    if has_varname or has_varnumber or has_longdesc or has_code:
        return "metadata_other"
    return "unknown"


def run_checked(cmd: list[str], *, cwd: str | Path | None = None, capture_output: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        cwd=str(cwd) if cwd is not None else None,
        check=True,
        capture_output=capture_output,
        text=False,
    )


def require_mdb_tools() -> None:
    missing = [name for name in ("mdb-tables", "mdb-schema", "mdb-export") if shutil.which(name) is None]
    if missing:
        raise SystemExit(
            "Missing required mdb-tools binaries: "
            + ", ".join(missing)
            + ". Install mdb-tools before running extraction."
        )


def maybe_mdb_count_binary() -> str | None:
    return shutil.which("mdb-count")


def decode_tool_output(raw: bytes) -> str:
    return raw.decode("utf-8", errors="replace").replace("\r\n", "\n").replace("\r", "\n")
