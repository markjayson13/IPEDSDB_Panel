#!/usr/bin/env python3
"""Render the published codebook JSON as a searchable, bookmarked PDF.

Install requirements-codebook.txt, then run this after build_codebook.py.
Only codebook documentation is written; panel releases remain unchanged.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
import hashlib
import json
from pathlib import Path
import re
from xml.sax.saxutils import escape


# Vera lacks this glyph; the standard PDF Symbol font renders and encodes it.
PARAGRAPH_GLYPH_FONTS = {"→": "Symbol"}


def native_stata_label(stata, export_code):
    """Use the recorded export label, not the source category's meaning."""
    return stata.get("value_labels", {}).get(str(export_code), "No native label assigned")


def year_ranges(years):
    values = sorted(set(int(y) for y in years))
    groups = []
    for y in values:
        if groups and groups[-1][-1] + 1 == y:
            groups[-1].append(y)
        else:
            groups.append([y])
    return ", ".join(str(g[0]) if len(g) == 1 else f"{g[0]}-{g[-1]}" for g in groups) or "Not supplied"


def load_codebook(directory):
    directory = Path(directory)
    index = json.loads((directory / "index.json").read_text())
    if index.get("schema_version") != 1:
        raise ValueError("Unsupported codebook schema")
    cache, variables, names = {}, [], set()
    for summary in index["variables"]:
        name, filename = summary["name"], summary["detail_file"]
        if name in names:
            raise ValueError(f"Duplicate variable: {name}")
        names.add(name)
        if not re.fullmatch(r"variables-\d+\.json", filename):
            raise ValueError("Unsafe detail file")
        if filename not in cache:
            cache[filename] = json.loads((directory / filename).read_text())
        detail = cache[filename][name]
        if detail["name"] != name:
            raise ValueError("Detail identity mismatch")
        variables.append(detail)
    if len(variables) != index["column_count"]:
        raise ValueError("Variable count differs from codebook index")
    return index, sorted(variables, key=lambda v: v["name"].upper())


def write_manifest(directory, index):
    """Bind the downloadable documentation to the exact release it describes."""
    directory = Path(directory)
    readme = (
        "IPEDSDB Panel user codebook\n\n"
        f"Release: {index['release']}\n"
        f"Dataset: {index['row_count']:,} institution-year rows, {index['column_count']:,} variables.\n\n"
        "Start with ipeds-panel-codebook.pdf, or use the searchable interface:\n"
        f"{index.get('codebook_url', 'https://markjayson13.github.io/IPEDSDB_Panel/')}\n\n"
        "codebook.csv gives one row per variable. Each description applies only\n"
        "to its description_years. definitions.csv.gz contains the full history;\n"
        "decompress it before opening the CSV. value-labels.csv gives source\n"
        "category meanings with their exact years. Stata mappings and physical\n"
        "source corrections are in the PDF and interactive reference.\n\n"
        "Missing definitions and undocumented category meanings remain flagged.\n"
        "Do not assume comparability across years, or interpret nulls as zero.\n"
        "These files contain documentation, not institution-level data.\n\n"
        "manifest.json records codebook checksums and the data/metadata hashes\n"
        "this codebook describes. index.json and variables-*.json power the site.\n"
    )
    (directory / "README.txt").write_text(readme)
    names = {"index.json", "README.txt", "ipeds-panel-codebook.pdf", *index.get("downloads", {}).values()}
    names.update(v["detail_file"] for v in index["variables"])
    artifacts = []
    for name in sorted(names):
        if Path(name).name != name:
            raise ValueError("Unsafe codebook artifact path")
        path = directory / name
        data = path.read_bytes()
        artifacts.append({"path": name, "size_bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()})
    manifest = {"schema_version": 1, "release": index["release"], "column_count": index["column_count"],
                "row_count": index["row_count"], "generated_from": index["generated_from"], "artifacts": artifacts}
    (directory / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


def render_pdf(directory, output=None):
    import reportlab
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_LEFT
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak, Table, TableStyle

    directory = Path(directory)
    output = Path(output) if output else directory / "ipeds-panel-codebook.pdf"
    index, variables = load_codebook(directory)
    fonts = Path(reportlab.__file__).parent / "fonts"
    pdfmetrics.registerFont(TTFont("Codebook", str(fonts / "Vera.ttf")))
    pdfmetrics.registerFont(TTFont("CodebookBold", str(fonts / "VeraBd.ttf")))
    pdfmetrics.registerFontFamily("Codebook", normal="Codebook", bold="CodebookBold", italic="Codebook", boldItalic="CodebookBold")

    # Fail rather than render a missing-glyph box in authoritative definitions.
    all_text = json.dumps([index, variables], ensure_ascii=False)
    cmap = pdfmetrics.getFont("Codebook").face.charToGlyph
    missing = sorted({c for c in all_text if ord(c) >= 32 and ord(c) not in cmap
                      and c not in PARAGRAPH_GLYPH_FONTS})
    if missing:
        raise ValueError(f"PDF font lacks source characters: {missing!r}")

    ink, muted, accent, line = [colors.HexColor(c) for c in ("#162B3D", "#536475", "#0B6B65", "#D7E1E8")]
    styles = {
        "body": ParagraphStyle("body", fontName="Codebook", fontSize=8.2, leading=11.5, textColor=ink, spaceAfter=5),
        "small": ParagraphStyle("small", fontName="Codebook", fontSize=7, leading=9.5, textColor=muted, spaceAfter=4),
        "title": ParagraphStyle("title", fontName="CodebookBold", fontSize=31, leading=36, textColor=ink, spaceAfter=16),
        "heading": ParagraphStyle("heading", fontName="CodebookBold", fontSize=15, leading=20, textColor=accent, spaceBefore=16, spaceAfter=6, keepWithNext=True),
        "sub": ParagraphStyle("sub", fontName="CodebookBold", fontSize=9, leading=12, textColor=ink, spaceBefore=7, spaceAfter=4, keepWithNext=True),
        "cell": ParagraphStyle("cell", fontName="Codebook", fontSize=7.1, leading=9.4, textColor=ink, alignment=TA_LEFT),
    }

    def p(text, kind="body"):
        text = "Not supplied" if text is None or text == "" else str(text)
        markup = escape(text).replace("\n", "<br/>")
        for glyph, font in PARAGRAPH_GLYPH_FONTS.items():
            markup = markup.replace(glyph, f'<font name="{font}">{glyph}</font>')
        return Paragraph(markup, styles[kind])

    story = []

    def table(headings, rows, widths):
        if not rows:
            return
        data = [[p(h, "cell") for h in headings]] + [[p(x, "cell") for x in row] for row in rows]
        t = Table(data, colWidths=widths, repeatRows=1, hAlign="LEFT", splitByRow=1, splitInRow=1)
        t.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#EAF1F3")),
            ("LINEBELOW", (0, 0), (-1, 0), 0.6, line),
            ("LINEBELOW", (0, 1), (-1, -1), 0.25, line),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 5),
            ("RIGHTPADDING", (0, 0), (-1, -1), 5),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ]))
        story.append(t)
        story.append(Spacer(1, 7))

    class Book(SimpleDocTemplate):
        def afterFlowable(self, flowable):
            if hasattr(flowable, "bookmark"):
                self.canv.bookmarkPage(flowable.bookmark)
                self.canv.addOutlineEntry(flowable.bookmark_title, flowable.bookmark, level=0, closed=False)
                self.section_title = flowable.bookmark_title

    def heading(text, bookmark=None):
        item = p(text, "heading")
        if bookmark:
            item.bookmark, item.bookmark_title = bookmark, text
        story.append(item)

    story += [Spacer(1, 35), p("IPEDSDB Panel", "small"), p("User codebook", "title"),
              p(f"{year_ranges(index['years'])} | {index['row_count']:,} institution-year rows | {index['column_count']:,} variables"),
              p(f"Labeled release: {index['release']}"),
              p(f"Source release status: {index.get('release_status', 'See source metadata')}"), Spacer(1, 18)]
    heading("How to use this codebook", "guide")
    guides = [
        f"Use the PDF bookmarks or alphabetical variable index to find a field. The online codebook provides search and year filters at {index.get('codebook_url', 'https://markjayson13.github.io/IPEDSDB_Panel/')}",
        "The panel key is UNITID plus year. It is an unbalanced panel: institution coverage differs by year. Year identifies an IPEDS release; survey, academic, fiscal and financial-aid reference periods can differ. Use the source-specific reference period where supplied.",
        "Observed years are years with at least one nonmissing panel value. A missing observation is not evidence of zero. Null counts cover the full panel. Negative source codes remain distinct from nulls; consult their variable- and year-specific meanings.",
        "Definitions and code labels apply only to the years listed. Changes in titles, source tables or definitions do not establish that a variable is comparable over time. Unspecified units, currency, price basis or reference periods are recorded as unknown.",
        "The companion Parquet retains source values and embedded metadata. The Stata file has native variable and category labels. Canonical integer strings preserve their numeric codes; other categorical strings use the reversible mappings printed below. Ordinary string nulls become empty strings in Stata. Use the original Parquet when that distinction matters.",
        "The full file has more than Stata/BE's 2,048-variable limit. Load a selected varlist from the full DTA, or use Stata/SE or MP. Source variable names and Stata export names are both recorded here.",
        f"This codebook describes the identified dataset; it does not include institution-level data. The data directory recorded by its manifest is {index.get('data_directory', 'Final')}/. Keep the data, matching metadata and checksums together.",
    ]
    story.extend(p(x) for x in guides)
    if index.get("release_notes"):
        heading("Release notes", "release-notes")
        story.extend(p(note) for note in index["release_notes"])
    heading("Known source gaps", "gaps")
    story.append(p("The published metadata is incomplete. No missing description or unknown category meaning has been guessed. The affected fields and observation counts follow; other measurement attributes can also be unspecified."))
    for issue in index.get("issues", []):
        story.append(p(f"{issue.get('variable', 'Dataset')}: {issue.get('message', issue.get('code', ''))}"))
    heading("Release identity", "identity")
    story.append(p("SHA-256 hashes bind this codebook to its published datasets and companion metadata. These identify the labeled derivative; original release files remain preserved."))
    for key, value in index.get("generated_from", {}).items():
        story.append(p(f"{key}: {value}", "small"))
    story.append(PageBreak())
    heading("Alphabetical variable index", "variables")
    story.append(p("Each variable name is a link to its definition. PDF bookmarks provide the same alphabetical index."))
    bookmarks = {v["name"]: f"v{i:04d}" for i, v in enumerate(variables)}
    rows = []
    for offset in range(0, len(variables), 3):
        row = []
        for v in variables[offset:offset+3]:
            row.append(Paragraph(f'<link href="#{bookmarks[v["name"]]}" color="#0B6B65">{escape(v["name"])}</link>', styles["small"]))
        row += [""] * (3-len(row))
        rows.append(row)
    toc = Table(rows, colWidths=[167]*3)
    toc.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP"), ("BOTTOMPADDING", (0, 0), (-1, -1), 3)]))
    story += [toc, PageBreak()]
    for v in variables:
        heading(v["name"], bookmarks[v["name"]])
        story.append(p(v["label"]))
        story.append(p(f"Storage: {v['storage_type']} | Stata name: {v.get('stata_name') or v['name']} | Observed years: {year_ranges(v['observed_years'])} | Nulls: {v['null_count']:,}", "small"))
        for issue in v.get("issues", []):
            story.append(p(f"Source gap: {issue.get('message', issue.get('code', ''))}"))
        story.append(p("Definitions by release year", "sub"))
        for d in v.get("definitions", []):
            story.append(p(f"{year_ranges(d['years'])}: {d.get('label') or 'Title not supplied'}", "small"))
            story.append(p(d.get("description") or "Description not supplied in source metadata."))
        if not v.get("definitions"):
            story.append(p(v.get("description") or "Description not supplied in source metadata."))
        semantics = v.get("semantic_metadata", {})
        if semantics:
            semantic_lines = []
            for key, val in semantics.items():
                resolved = val.get("value")
                if resolved is None:
                    resolved = "; ".join(str(x) for x in val.get("values", [])) or "Not supplied"
                semantic_lines.append(f"{key.replace('_', ' ')}: {resolved} ({val.get('status', 'unknown')})")
            story.append(p("Measurement metadata: " + " | ".join(semantic_lines), "small"))
        if v.get("codes"):
            story.append(p("Source category codes", "sub"))
            table(["Code", "Meaning", "Release years / source"],
                  [[c["code"], c.get("label") or "Meaning not supplied", year_ranges(c["years"])+" / "+", ".join(c.get("sources", []))] for c in v["codes"]], [60, 303, 138])
        records = v.get("source_records", [])
        consolidation = v.get("column_consolidation")
        if consolidation:
            story.append(p("Consolidated source columns", "sub"))
            story.append(p(consolidation.get("rationale", "Verified source-table moves; source values remain unchanged."), "small"))
            for member in consolidation["members"]:
                story.append(p(f"{member['column']}: {year_ranges(member['years'])}", "small"))
            caveats = consolidation.get("caveats", [])
            for note in caveats if isinstance(caveats, list) else [caveats]:
                if note:
                    story.append(p(note, "small"))
        if records:
            story.append(p("Source identity and reference period", "sub"))
            # Compact records without erasing physical table identity or year scope.
            for r in records:
                text = f"{year_ranges(r['years'])}: {r.get('table') or 'Table not supplied'}.{r.get('varname') or v['name']} | number {r.get('varnumber') or 'not supplied'} | source {r.get('source_file') or 'not supplied'}"
                if r.get("reference_period"):
                    text += f" | reference period: {r['reference_period']}"
                elif r.get("source_table_reference_period"):
                    text += f" | table coverage period: {r['source_table_reference_period']}"
                if r.get("release_type"):
                    text += f" | release: {r['release_type']}"
                if r.get("source_release_date"):
                    text += f" | release date: {r['source_release_date']}"
                if r.get("imputationvar"):
                    text += f" | dictionary imputation flag: {r['imputationvar']}"
                if r.get("imputation_flag_availability"):
                    text += f" | flag availability: {r['imputation_flag_availability']}"
                if r.get("reporting_population_note"):
                    text += f" | reporting population: {r['reporting_population_note']}"
                story.append(p(text, "small"))
            corrections = defaultdict(list)
            for r in records:
                if r.get("correction_id"):
                    corrections[(r["correction_id"], r.get("original_table"), r.get("table"), r.get("correction_reason"))].extend(r["years"])
            for (cid, original, resolved, reason), years in corrections.items():
                story.append(p(f"Documented correction {cid}, {year_ranges(years)}: original dictionary table {original}; resolved physical table {resolved}. {reason or ''}", "small"))
        stata = v.get("stata", {})
        conversion = stata.get("storage_conversion", "none")
        story.append(p(f"Stata storage conversion: {conversion}. Native variable label: {stata.get('label') or v['label']}", "small"))
        mapping = stata.get("source_code_map", [])
        if mapping:
            story.append(p("Reversible Stata category mapping", "sub"))
            table(["Source code", "Stata code", "Native value label"],
                  [[m["source_code"], m["export_code"], native_stata_label(stata, m["export_code"])]
                   for m in mapping], [78, 62, 361])
        story.append(Spacer(1, 7))

    def page(canvas, doc):
        canvas.saveState()
        width, height = A4
        canvas.setStrokeColor(line)
        canvas.line(47, 39, width-47, 39)
        canvas.setFont("Codebook", 7)
        canvas.setFillColor(muted)
        canvas.drawString(47, 27, f"IPEDSDB Panel | {index['release']}")
        canvas.drawRightString(width-47, 27, str(doc.page))
        if doc.page > 1:
            canvas.drawString(47, height-27, "USER CODEBOOK / " + year_ranges(index["years"]))
            canvas.drawRightString(width-47, height-27, getattr(doc, "section_title", ""))
        canvas.restoreState()

    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(".pdf.tmp")
    try:
        doc = Book(str(temporary), pagesize=A4, leftMargin=47, rightMargin=47,
                   topMargin=46, bottomMargin=49, title="IPEDSDB Panel user codebook", author="IPEDSDB Panel",
                   subject=f"Variable definitions and year-specific source codes for {index['release']}", pageCompression=1, invariant=1)
        doc.build(story, onFirstPage=page, onLaterPages=page)
        temporary.replace(output)
    finally:
        temporary.unlink(missing_ok=True)
    return {"path": str(output), "variables": len(variables), "size_bytes": output.stat().st_size,
            "sha256": hashlib.sha256(output.read_bytes()).hexdigest()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=Path("docs/codebook"))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = render_pdf(args.input_dir, args.output)
    if not args.output:
        index = json.loads((args.input_dir / "index.json").read_text())
        write_manifest(args.input_dir, index)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
