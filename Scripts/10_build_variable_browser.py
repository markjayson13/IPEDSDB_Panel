#!/usr/bin/env python3
"""
Build a static HTML browser for the current panel.

It reads a wide panel parquet plus `dictionary_lake.parquet`, then writes one
HTML file for finding real columns, reviewing metadata, and exporting
`selectedvars.txt`.
"""
from __future__ import annotations

import argparse
import hashlib
import html
import json
import os
import re
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq


GROUP_ORDER = [
    "Institution / Directory",
    "Admissions",
    "Costs / Price",
    "Student Financial Aid",
    "Enrollment / Student Profile",
    "Completions",
    "Graduation / Outcomes",
    "Finance",
    "Staff / Human Resources",
    "Custom / Derived / Other",
    "Panel-only / custom",
]


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>__TITLE__</title>
  <link rel="icon" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64'%3E%3Crect width='64' height='64' rx='14' fill='%230b6e69'/%3E%3Cpath d='M18 24.5L32 16l14 8.5v15L32 48l-14-8.5v-15Z' fill='%23fffaf1'/%3E%3Cpath d='M32 16v32' stroke='%230b6e69' stroke-width='4'/%3E%3C/svg%3E">
  <style>
    :root {
      --bg: #f3efe4;
      --bg-strong: #e8e0cf;
      --panel: rgba(255, 251, 243, 0.9);
      --panel-strong: rgba(255, 248, 236, 0.97);
      --line: rgba(48, 49, 45, 0.14);
      --line-strong: rgba(48, 49, 45, 0.24);
      --text: #1d2a2d;
      --muted: #5b665f;
      --accent: #0b6e69;
      --accent-strong: #104f53;
      --accent-soft: #d7ebe6;
      --warn: #b35c2e;
      --warn-soft: rgba(179, 92, 46, 0.12);
      --shadow: 0 22px 55px rgba(49, 40, 28, 0.14);
      --heading: "Iowan Old Style", "Palatino Linotype", "Book Antiqua", Georgia, serif;
      --body: "Avenir Next", "Segoe UI", "Helvetica Neue", Helvetica, sans-serif;
      --mono: "SFMono-Regular", Menlo, Monaco, Consolas, "Liberation Mono", monospace;
    }

    * { box-sizing: border-box; }
    html, body { margin: 0; padding: 0; }
    body {
      min-height: 100vh;
      font-family: var(--body);
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(179, 92, 46, 0.12), transparent 32%),
        radial-gradient(circle at top right, rgba(11, 110, 105, 0.11), transparent 38%),
        linear-gradient(180deg, #f6f1e7 0%, #efe7d8 100%);
    }

    body::before {
      content: "";
      position: fixed;
      inset: 0;
      pointer-events: none;
      background-image:
        linear-gradient(rgba(22, 37, 40, 0.03) 1px, transparent 1px),
        linear-gradient(90deg, rgba(22, 37, 40, 0.03) 1px, transparent 1px);
      background-size: 28px 28px;
      mask-image: linear-gradient(180deg, rgba(0,0,0,0.35), transparent 92%);
    }

    .shell {
      width: min(1460px, calc(100vw - 32px));
      margin: 24px auto 40px;
    }

    .hidden {
      display: none !important;
    }

    .hero,
    .panel {
      border: 1px solid var(--line);
      border-radius: 26px;
      box-shadow: var(--shadow);
      backdrop-filter: blur(10px);
    }

    .hero {
      position: relative;
      overflow: hidden;
      padding: 26px 28px;
      background: linear-gradient(135deg, rgba(255, 248, 238, 0.95), rgba(244, 239, 227, 0.92));
      transition: padding 160ms ease, gap 160ms ease;
      display: grid;
      gap: 18px;
    }

    .hero::after {
      content: "";
      position: absolute;
      width: 260px;
      height: 260px;
      right: -90px;
      top: -110px;
      border-radius: 999px;
      background: radial-gradient(circle, rgba(11, 110, 105, 0.18), transparent 72%);
    }

    body.hero-compact .hero {
      padding: 16px 20px;
      gap: 10px;
    }

    .hero-top {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: center;
      position: relative;
      z-index: 1;
    }

    .eyebrow {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 8px 12px;
      border-radius: 999px;
      background: rgba(11, 110, 105, 0.08);
      color: var(--accent-strong);
      font-size: 12px;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      font-weight: 700;
    }

    body.hero-compact .eyebrow {
      display: none;
    }

    .hero-main {
      display: grid;
      gap: 18px;
      grid-template-columns: minmax(280px, 0.95fr) minmax(0, 1.2fr);
      align-items: start;
      position: relative;
      z-index: 1;
    }

    body.hero-compact .hero-main {
      grid-template-columns: 1fr;
      gap: 10px;
    }

    .hero-copy-wrap {
      display: grid;
      gap: 12px;
      max-width: 70ch;
    }

    .hero h1 {
      margin: 0;
      font-family: var(--heading);
      font-size: clamp(2rem, 4.2vw, 4rem);
      line-height: 0.98;
      letter-spacing: -0.03em;
      max-width: 11ch;
    }

    body.hero-compact .hero h1 {
      font-size: clamp(1.3rem, 2.5vw, 1.9rem);
      max-width: none;
    }

    .hero-copy {
      color: var(--muted);
      font-size: 1rem;
      line-height: 1.55;
    }

    body.hero-compact .hero-copy {
      display: none;
    }

    .hero-grid {
      display: grid;
      gap: 12px;
      grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
    }

    .stat {
      padding: 15px 16px;
      border-radius: 18px;
      border: 1px solid rgba(16, 79, 83, 0.1);
      background: rgba(255, 255, 255, 0.55);
      min-width: 0;
    }

    body.hero-compact .stat {
      padding: 10px 12px;
      border-radius: 14px;
    }

    .stat-label {
      color: var(--muted);
      font-size: 0.72rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      margin-bottom: 8px;
      font-weight: 700;
    }

    .stat-value {
      font-size: 1.42rem;
      font-weight: 800;
      letter-spacing: -0.03em;
      line-height: 1.12;
      overflow-wrap: anywhere;
      word-break: break-word;
    }

    body.hero-compact .stat-value {
      font-size: 1.06rem;
    }

    .layout {
      display: grid;
      grid-template-columns: minmax(0, 1.65fr) minmax(340px, 0.9fr);
      gap: 18px;
      margin-top: 18px;
      align-items: start;
    }

    .panel {
      background: var(--panel);
    }

    .controls {
      padding: 22px;
      border-bottom: 1px solid rgba(48, 49, 45, 0.08);
    }

    .search-row,
    .filter-row,
    .save-row,
    .selection-actions {
      display: grid;
      gap: 10px;
    }

    .search-helpers,
    .drawer-actions {
      display: grid;
      gap: 10px;
    }

    .search-row {
      grid-template-columns: minmax(0, 1fr) auto auto auto;
      margin-bottom: 12px;
    }

    .filter-row {
      grid-template-columns: repeat(4, minmax(0, 1fr));
      margin-bottom: 14px;
    }

    .save-row {
      grid-template-columns: minmax(0, 1fr) auto;
    }

    .selection-actions {
      grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
    }

    .search-helpers {
      grid-template-columns: minmax(0, 1fr) auto;
      align-items: center;
      margin-bottom: 14px;
    }

    input,
    select,
    textarea {
      width: 100%;
      border-radius: 14px;
      border: 1px solid var(--line);
      padding: 12px 14px;
      background: rgba(255, 255, 255, 0.82);
      font: inherit;
      color: var(--text);
    }

    textarea {
      min-height: 170px;
      resize: vertical;
      font-family: var(--mono);
      font-size: 0.9rem;
      line-height: 1.45;
    }

    button,
    .chip {
      appearance: none;
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 10px 14px;
      background: rgba(255, 255, 255, 0.78);
      color: var(--text);
      font: inherit;
      cursor: pointer;
      transition: transform 120ms ease, border-color 120ms ease, background 120ms ease;
      white-space: normal;
      overflow-wrap: anywhere;
      word-break: break-word;
    }

    button:hover,
    .chip:hover {
      transform: translateY(-1px);
      border-color: var(--line-strong);
    }

    button:disabled {
      opacity: 0.48;
      cursor: not-allowed;
      transform: none;
    }

    .primary,
    .chip.active {
      background: var(--accent);
      color: white;
      border-color: var(--accent);
    }

    .secondary {
      background: var(--accent-soft);
      border-color: rgba(11, 110, 105, 0.16);
      color: var(--accent-strong);
    }

    .ghost {
      background: transparent;
    }

    .chips,
    .mini-actions,
    .info-strip,
    .selected-chip-list,
    .saved-sets,
    .summary-stack {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
    }

    .info-strip {
      align-items: center;
      color: var(--muted);
      font-size: 0.93rem;
    }

    .info-pill,
    .badge,
    .summary-pill {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 7px 11px;
      border-radius: 999px;
      border: 1px solid rgba(48, 49, 45, 0.08);
      background: rgba(243, 239, 228, 0.95);
      color: var(--muted);
      font-size: 0.8rem;
      max-width: 100%;
      white-space: normal;
      overflow-wrap: anywhere;
      word-break: break-word;
    }

    .summary-pill.warn,
    .badge.warn {
      background: var(--warn-soft);
      border-color: rgba(179, 92, 46, 0.18);
      color: #8a451d;
    }

    .browser {
      padding: 18px 22px 22px;
    }

    .browser-header,
    .section-head {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: baseline;
    }

    .browser-header {
      padding-bottom: 12px;
    }

    .browser-header h2,
    .selection h2,
    .section-head h3 {
      margin: 0;
      font-family: var(--heading);
      letter-spacing: -0.02em;
    }

    .browser-header h2,
    .selection h2 {
      font-size: 1.55rem;
    }

    .section-head h3 {
      font-size: 1.05rem;
    }

    .browser-note,
    .section-meta,
    .muted,
    .selection-copy,
    .footer-note {
      color: var(--muted);
      line-height: 1.5;
      overflow-wrap: anywhere;
      word-break: break-word;
    }

    .section {
      display: grid;
      gap: 12px;
      padding: 16px;
      border: 1px solid rgba(48, 49, 45, 0.08);
      border-radius: 18px;
      background: rgba(255, 255, 255, 0.46);
    }

    .selection {
      position: sticky;
      top: 18px;
      padding: 22px;
      display: grid;
      gap: 14px;
    }

    .selection-empty {
      background: linear-gradient(180deg, rgba(255,255,255,0.72), rgba(249, 244, 234, 0.85));
    }

    .summary-box {
      padding: 12px 14px;
      border-radius: 14px;
      background: rgba(16, 79, 83, 0.05);
      border: 1px solid rgba(16, 79, 83, 0.08);
      display: grid;
      gap: 8px;
    }

    .summary-box.warn {
      background: var(--warn-soft);
      border-color: rgba(179, 92, 46, 0.16);
    }

    .selected-chip-list {
      align-items: stretch;
    }

    .selected-chip {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      border: 1px solid rgba(11, 110, 105, 0.16);
      background: rgba(215, 235, 230, 0.6);
      padding: 7px 11px;
      border-radius: 999px;
      font-family: var(--mono);
      font-size: 0.82rem;
      max-width: 100%;
      white-space: normal;
      overflow-wrap: anywhere;
      word-break: break-word;
    }

    .selected-chip button {
      padding: 2px 8px;
      border-radius: 999px;
      font-size: 0.76rem;
    }

    .saved-sets {
      flex-direction: column;
    }

    .saved-set {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: center;
      padding: 12px 14px;
      border: 1px solid rgba(48, 49, 45, 0.08);
      border-radius: 14px;
      background: rgba(255, 255, 255, 0.65);
      min-width: 0;
    }

    .saved-set-name {
      font-weight: 800;
      letter-spacing: 0.01em;
      overflow-wrap: anywhere;
      word-break: break-word;
    }

    .saved-set-meta {
      color: var(--muted);
      font-size: 0.85rem;
      margin-top: 4px;
      overflow-wrap: anywhere;
      word-break: break-word;
    }

    .saved-set-actions {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
    }

    .view-toggle {
      display: inline-flex;
      gap: 8px;
      align-items: center;
      flex-wrap: wrap;
    }

    .group-head-actions,
    .family-actions {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      align-items: center;
    }

    .search-history {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      align-items: center;
      min-height: 38px;
    }

    .search-history .muted {
      font-size: 0.88rem;
    }

    .group-section + .group-section {
      margin-top: 16px;
    }

    .group-head {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: center;
      margin-bottom: 10px;
      padding: 12px 14px;
      border-radius: 16px;
      background: rgba(16, 79, 83, 0.05);
      border: 1px solid rgba(16, 79, 83, 0.08);
    }

    .group-head h3 {
      margin: 0;
      font-size: 1rem;
      letter-spacing: 0.01em;
      overflow-wrap: anywhere;
      word-break: break-word;
    }

    .group-count {
      color: var(--muted);
      font-size: 0.88rem;
      white-space: nowrap;
    }

    .family-actions {
      padding: 0 14px 14px;
      justify-content: flex-end;
    }

    details.source-cluster,
    details.export-panel {
      border: 1px solid rgba(48, 49, 45, 0.08);
      border-radius: 18px;
      overflow: hidden;
      background: rgba(255, 255, 255, 0.5);
    }

    details.source-cluster[open] summary,
    details.export-panel[open] summary {
      border-bottom: 1px solid rgba(48, 49, 45, 0.08);
    }

    summary {
      cursor: pointer;
      list-style: none;
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 14px;
      padding: 14px 16px;
    }

    summary::-webkit-details-marker { display: none; }

    .source-name {
      font-weight: 800;
      letter-spacing: 0.02em;
      overflow-wrap: anywhere;
      word-break: break-word;
    }

    .source-sub {
      color: var(--muted);
      font-size: 0.9rem;
      margin-top: 4px;
      overflow-wrap: anywhere;
      word-break: break-word;
    }

    .cluster-body {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(260px, 1fr));
      gap: 12px;
      padding: 14px;
    }

    .var-card {
      position: relative;
      display: grid;
      gap: 10px;
      padding: 14px;
      border: 1px solid rgba(48, 49, 45, 0.1);
      border-radius: 16px;
      background: linear-gradient(180deg, rgba(255,255,255,0.82), rgba(252,248,239,0.9));
    }

    .var-card.selected {
      border-color: rgba(11, 110, 105, 0.38);
      box-shadow: inset 0 0 0 1px rgba(11, 110, 105, 0.22);
    }

    .var-top {
      display: flex;
      gap: 10px;
      align-items: start;
    }

    .var-copy {
      display: grid;
      gap: 8px;
      min-width: 0;
    }

    .var-top input[type="checkbox"] {
      width: 19px;
      height: 19px;
      margin-top: 2px;
      accent-color: var(--accent);
      cursor: pointer;
      flex: 0 0 auto;
    }

    .var-name {
      margin: 0;
      font-size: 1rem;
      letter-spacing: 0.02em;
      font-family: var(--mono);
      overflow-wrap: anywhere;
      word-break: break-word;
    }

    .var-title {
      color: var(--text);
      font-weight: 700;
      line-height: 1.34;
      overflow-wrap: anywhere;
      word-break: break-word;
    }

    .var-desc {
      color: var(--muted);
      font-size: 0.92rem;
      line-height: 1.5;
      min-height: 3.2em;
      overflow-wrap: anywhere;
      word-break: break-word;
    }

    .meta-row {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }

    .card-actions {
      display: flex;
      justify-content: space-between;
      gap: 10px;
      align-items: center;
    }

    .card-actions .muted {
      font-size: 0.82rem;
    }

    .badge {
      padding: 6px 9px;
    }

    .coverage-strip {
      display: flex;
      gap: 3px;
      align-items: center;
      min-height: 10px;
    }

    .coverage-segment {
      flex: 1 1 0;
      min-width: 8px;
      height: 8px;
      border-radius: 999px;
      background: rgba(29, 42, 45, 0.11);
    }

    .coverage-segment.active {
      background: rgba(11, 110, 105, 0.78);
    }

    .coverage-segment.warn {
      background: rgba(179, 92, 46, 0.45);
    }

    .completeness-track {
      width: 100%;
      height: 8px;
      border-radius: 999px;
      overflow: hidden;
      background: rgba(29, 42, 45, 0.12);
      position: relative;
    }

    .completeness-fill {
      position: absolute;
      inset: 0 auto 0 0;
      background: linear-gradient(90deg, rgba(11, 110, 105, 0.55), rgba(11, 110, 105, 0.9));
    }

    .completeness-fill.warn {
      background: linear-gradient(90deg, rgba(179, 92, 46, 0.45), rgba(179, 92, 46, 0.85));
    }

    .table-wrap {
      overflow: auto;
      border: 1px solid rgba(48, 49, 45, 0.08);
      border-radius: 18px;
      background: rgba(255, 255, 255, 0.55);
    }

    .var-table {
      width: 100%;
      border-collapse: collapse;
      min-width: 980px;
    }

    .var-table th,
    .var-table td {
      padding: 12px 14px;
      border-bottom: 1px solid rgba(48, 49, 45, 0.08);
      text-align: left;
      vertical-align: top;
    }

    .var-table th {
      position: sticky;
      top: 0;
      background: rgba(246, 241, 231, 0.98);
      z-index: 1;
      font-size: 0.8rem;
      letter-spacing: 0.06em;
      text-transform: uppercase;
      color: var(--muted);
    }

    .var-table tr {
      transition: background 120ms ease;
    }

    .var-table tbody tr:hover {
      background: rgba(11, 110, 105, 0.05);
    }

    .var-table tr.selected {
      background: rgba(215, 235, 230, 0.45);
    }

    .table-name {
      display: grid;
      gap: 4px;
    }

    .table-var {
      font-family: var(--mono);
      font-size: 0.92rem;
      font-weight: 700;
      overflow-wrap: anywhere;
      word-break: break-word;
    }

    .table-title {
      color: var(--muted);
      font-size: 0.87rem;
      line-height: 1.45;
      max-width: 48ch;
      overflow-wrap: anywhere;
      word-break: break-word;
    }

    .table-meta {
      display: grid;
      gap: 6px;
      min-width: 140px;
    }

    .table-compact {
      font-size: 0.84rem;
      color: var(--muted);
    }

    .detail-overlay {
      position: fixed;
      inset: 0;
      z-index: 40;
      display: grid;
      grid-template-columns: minmax(0, 1fr);
    }

    .detail-backdrop {
      position: absolute;
      inset: 0;
      background: rgba(24, 28, 28, 0.34);
      backdrop-filter: blur(2px);
    }

    .detail-drawer {
      position: relative;
      margin-left: auto;
      width: min(560px, calc(100vw - 18px));
      height: 100vh;
      background: linear-gradient(180deg, rgba(255, 251, 243, 0.98), rgba(244, 239, 227, 0.98));
      border-left: 1px solid rgba(48, 49, 45, 0.12);
      box-shadow: -18px 0 40px rgba(29, 42, 45, 0.16);
      display: grid;
      grid-template-rows: auto 1fr;
    }

    .drawer-head {
      padding: 18px 20px;
      border-bottom: 1px solid rgba(48, 49, 45, 0.08);
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: start;
    }

    .drawer-title-wrap {
      display: grid;
      gap: 8px;
    }

    .drawer-title {
      margin: 0;
      font-family: var(--heading);
      font-size: 1.7rem;
      letter-spacing: -0.03em;
      overflow-wrap: anywhere;
      word-break: break-word;
    }

    .drawer-subtitle {
      color: var(--muted);
      line-height: 1.5;
      overflow-wrap: anywhere;
      word-break: break-word;
    }

    .detail-content {
      overflow: auto;
      padding: 18px 20px 28px;
      display: grid;
      gap: 14px;
    }

    .detail-section {
      display: grid;
      gap: 10px;
      padding: 14px;
      border-radius: 16px;
      border: 1px solid rgba(48, 49, 45, 0.08);
      background: rgba(255, 255, 255, 0.58);
    }

    .detail-section h3 {
      margin: 0;
      font-size: 1rem;
      letter-spacing: 0.01em;
    }

    .metric-grid {
      display: grid;
      gap: 12px;
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }

    .metric {
      display: grid;
      gap: 6px;
    }

    .metric-label {
      color: var(--muted);
      font-size: 0.8rem;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      font-weight: 700;
    }

    .metric-value {
      font-weight: 700;
      overflow-wrap: anywhere;
      word-break: break-word;
    }

    .related-list {
      display: grid;
      gap: 10px;
    }

    .related-item {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: start;
      padding: 12px 14px;
      border-radius: 14px;
      border: 1px solid rgba(48, 49, 45, 0.08);
      background: rgba(255, 255, 255, 0.66);
      min-width: 0;
    }

    .related-copy {
      display: grid;
      gap: 4px;
      min-width: 0;
    }

    .related-name {
      font-family: var(--mono);
      font-size: 0.86rem;
      font-weight: 700;
      overflow-wrap: anywhere;
      word-break: break-word;
    }

    .related-desc {
      color: var(--muted);
      font-size: 0.85rem;
      line-height: 1.45;
      overflow-wrap: anywhere;
      word-break: break-word;
    }

    code {
      overflow-wrap: anywhere;
      word-break: break-word;
    }

    .related-actions {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }

    .empty-state {
      padding: 28px 18px;
      border: 1px dashed var(--line);
      border-radius: 18px;
      text-align: center;
      color: var(--muted);
      background: rgba(255, 255, 255, 0.5);
    }

    .toast {
      position: fixed;
      right: 18px;
      bottom: 18px;
      display: flex;
      align-items: center;
      gap: 12px;
      padding: 12px 14px;
      border-radius: 16px;
      background: rgba(29, 42, 45, 0.95);
      color: white;
      box-shadow: 0 18px 40px rgba(29, 42, 45, 0.28);
      z-index: 30;
    }

    .toast button {
      background: rgba(255, 255, 255, 0.14);
      color: white;
      border-color: rgba(255, 255, 255, 0.18);
    }

    .footer-note {
      font-size: 0.85rem;
    }

    @media (max-width: 1140px) {
      .layout {
        grid-template-columns: 1fr;
      }

      .selection {
        position: static;
      }

      .hero-main {
        grid-template-columns: 1fr;
      }
    }

    @media (max-width: 760px) {
      .shell {
        width: min(100vw - 16px, 100%);
        margin: 8px auto 24px;
      }

      .hero,
      .controls,
      .browser,
      .selection {
        padding: 18px;
      }

      .search-row,
      .filter-row,
      .save-row,
      .search-helpers {
        grid-template-columns: 1fr;
      }

      .cluster-body {
        grid-template-columns: 1fr;
      }

      .metric-grid {
        grid-template-columns: 1fr;
      }

      .detail-drawer {
        width: 100vw;
      }

      .toast {
        right: 10px;
        left: 10px;
        bottom: 10px;
      }
    }
  </style>
</head>
<body>
  <div class="shell">
    <section class="hero" id="hero">
      <div class="hero-top">
        <div class="eyebrow">IPEDS variable picker</div>
        <button id="hero-toggle" class="ghost" type="button">Compact header</button>
      </div>
      <div class="hero-main">
        <div class="hero-copy-wrap">
          <h1>Variable Browser</h1>
          <div class="hero-copy">
            Browse only the variables that exist in this panel, then export a clean <code>selectedvars.txt</code>. <strong>UNITID</strong> and <strong>year</strong> are kept automatically.
          </div>
        </div>
        <div class="hero-grid">
          <div class="stat">
            <div class="stat-label">Panel file</div>
            <div class="stat-value" id="stat-panel-name">__PANEL_NAME__</div>
          </div>
          <div class="stat">
            <div class="stat-label">Selectable vars</div>
            <div class="stat-value" id="stat-total-vars">0</div>
          </div>
          <div class="stat">
            <div class="stat-label">Rows in panel</div>
            <div class="stat-value" id="stat-total-rows">0</div>
          </div>
          <div class="stat">
            <div class="stat-label">Selected now</div>
            <div class="stat-value" id="stat-selected-count">0</div>
          </div>
        </div>
      </div>
    </section>

    <div class="layout">
      <section class="panel">
        <div class="controls">
          <div class="search-row">
            <input id="search-box" type="search" placeholder="Search varname, title, description, source, or concept. Press / or Ctrl/Cmd+K to focus.">
            <button id="toggle-selected" class="ghost" type="button">Selected only</button>
            <button id="select-visible" class="secondary" type="button">Select visible</button>
            <button id="clear-visible" class="ghost" type="button">Clear visible</button>
          </div>
          <div class="search-helpers">
            <div id="recent-searches" class="search-history"></div>
            <div class="view-toggle">
              <button id="reset-filters" class="ghost" type="button">Reset filters</button>
              <button id="view-cards" class="primary" type="button">Cards</button>
              <button id="view-table" class="ghost" type="button">Table</button>
            </div>
          </div>
          <div class="filter-row">
            <select id="filter-form">
              <option value="all">All variable forms</option>
              <option value="amount">Amounts</option>
              <option value="rate">Rates / percentages</option>
              <option value="count">Counts</option>
              <option value="flag">Flags / statuses</option>
              <option value="identifier">Identifiers</option>
              <option value="category">Categories</option>
              <option value="text">Text / labels</option>
              <option value="measure">Other measures</option>
            </select>
            <select id="filter-panel-type">
              <option value="all">All panel types</option>
              <option value="numeric">Numeric columns</option>
              <option value="string">String columns</option>
              <option value="boolean">Boolean columns</option>
              <option value="other">Other panel types</option>
            </select>
            <select id="filter-coverage">
              <option value="all">All year coverage</option>
              <option value="full-window">Full analysis window</option>
              <option value="broad">Broad coverage</option>
              <option value="partial">Partial coverage</option>
              <option value="limited">Limited coverage</option>
              <option value="unknown">Unknown coverage</option>
            </select>
            <select id="filter-quality">
              <option value="all">All quality states</option>
              <option value="full">High completeness</option>
              <option value="strong">Strong completeness</option>
              <option value="partial">Partial completeness</option>
              <option value="sparse">Sparse completeness</option>
              <option value="mixed">Mixed source history</option>
              <option value="missing-meta">Missing metadata</option>
            </select>
          </div>
          <div class="chips" id="group-chips"></div>
          <div class="info-strip">
            <div class="info-pill">Keys always kept: <strong>UNITID</strong>, <strong>year</strong></div>
            <div class="info-pill">Semantic groups + <code>source_file</code> metadata</div>
            <div class="info-pill" id="schema-pill">Schema</div>
            <div class="info-pill" id="visible-pill">Visible variables: 0</div>
          </div>
        </div>
        <div class="browser">
          <div class="browser-header">
            <h2>Browse Variables</h2>
            <div class="browser-note" id="browse-note">Search favors exact varnames first, then families, titles, and descriptions.</div>
          </div>
          <div id="browser-root"></div>
        </div>
      </section>

      <aside class="panel selection">
        <div>
          <h2>Selection</h2>
          <div class="selection-copy">
            Track your current list, saved sets, imports, and exports.
          </div>
        </div>

        <div id="selection-empty" class="section selection-empty">
          <div class="section-head">
            <h3>Quick Start</h3>
            <div class="section-meta">0 selected</div>
          </div>
          <div class="selection-copy">
            Start with a preset, then narrow with search or filters.
          </div>
          <div class="chips" id="preset-buttons"></div>
        </div>

        <div id="selection-summary" class="section hidden"></div>

        <div class="section">
          <div class="section-head">
            <h3>Saved Sets</h3>
            <div class="section-meta" id="saved-sets-meta">0 saved</div>
          </div>
          <div class="save-row">
            <input id="save-set-name" type="text" placeholder="Name the current selection">
            <button id="save-current-set" class="secondary" type="button">Save current set</button>
          </div>
          <div id="saved-sets" class="saved-sets"></div>
        </div>

        <div class="section">
          <div class="section-head">
            <h3>Import & Validate</h3>
            <div class="section-meta">Check matches before loading</div>
          </div>
          <textarea id="import-box" spellcheck="false" placeholder="Paste one var per line or comma-separated values. Comments beginning with # are ignored."></textarea>
          <div class="selection-actions">
            <button id="apply-import" class="secondary" type="button">Replace with import</button>
            <button id="merge-import" class="ghost" type="button">Add import</button>
            <button id="clear-import" class="ghost" type="button">Clear pasted text</button>
          </div>
          <div id="import-summary" class="summary-box muted">
            Paste a variable list to see matches, missing names, and duplicates before loading it.
          </div>
        </div>

        <details class="export-panel" id="export-panel" open>
          <summary>
            <div>
              <div class="source-name">Export</div>
              <div class="source-sub">Use <code>selectedvars.txt</code> as the main output. Add a manifest only if you need one.</div>
            </div>
            <div class="section-meta">txt + manifest</div>
          </summary>
          <div class="section" style="border: 0; border-radius: 0; background: transparent; padding-top: 14px;">
            <div class="selection-actions">
              <button id="copy-selection" class="primary" type="button">Copy list</button>
              <button id="download-selection" class="secondary" type="button">Download .txt</button>
              <button id="download-manifest" class="ghost" type="button">Download manifest</button>
              <button id="clear-selection" class="ghost" type="button">Clear all</button>
            </div>
            <textarea id="selection-box" spellcheck="false" placeholder="Selected vars will appear here, one per line."></textarea>
            <div class="muted" id="selection-meta">0 variables selected</div>
          </div>
        </details>

        <div class="footer-note">
          Static page. It does not edit the parquet. Regenerate it when the panel schema changes.
        </div>
      </aside>
    </div>
  </div>

  <div id="toast" class="toast hidden">
    <span id="toast-text"></span>
    <button id="undo-action" type="button">Undo</button>
  </div>

  <div id="detail-overlay" class="detail-overlay hidden">
    <div id="detail-backdrop" class="detail-backdrop"></div>
    <aside class="detail-drawer" aria-modal="true" role="dialog" aria-labelledby="detail-title">
      <div class="drawer-head">
        <div class="drawer-title-wrap">
          <h2 id="detail-title" class="drawer-title">Variable detail</h2>
          <div id="detail-subtitle" class="drawer-subtitle">Inspect metadata, coverage, quality, and nearby variables without losing your place.</div>
        </div>
        <button id="detail-close" class="ghost" type="button">Close</button>
      </div>
      <div id="detail-content" class="detail-content"></div>
    </aside>
  </div>

  <script id="variable-browser-data" type="application/json">__DATA__</script>
  <script>
    (() => {
      const payload = JSON.parse(document.getElementById("variable-browser-data").textContent);
      const rows = payload.variables || [];
      const presets = payload.presets || [];
      const schemaHash = payload.schemaHash || "default";
      const storageBase = `ipeds-variable-browser:${schemaHash}`;
      const storageKey = `${storageBase}:selection`;
      const savedSetsKey = `${storageBase}:savedSets`;
      const recentSearchesKey = `${storageBase}:recentSearches`;
      const viewModeKey = `${storageBase}:viewMode`;
      const introSeenKey = `${storageBase}:introSeen`;
      const heroCompactKey = `${storageBase}:heroCompact`;
      const byNameUpper = new Map(rows.map((row) => [String(row.exportName).toUpperCase(), row]));
      const byExportName = new Map(rows.map((row) => [String(row.exportName), row]));
      const selection = new Set();
      let lastUndo = null;
      let toastTimer = null;

      rows.forEach((row) => {
        row.varnameLower = String(row.varname || "").toLowerCase();
        row.exportNameLower = String(row.exportName || "").toLowerCase();
        row.varTitleLower = String(row.varTitle || "").toLowerCase();
        row.longDescriptionLower = String(row.longDescription || "").toLowerCase();
        row.primarySourceLower = String(row.primarySource || "").toLowerCase();
        row.componentGroupLower = String(row.componentGroup || "").toLowerCase();
        row.semanticFamilyLower = String(row.semanticFamily || "").toLowerCase();
        row.variableFormLower = String(row.variableForm || "").toLowerCase();
      });

      const searchBox = document.getElementById("search-box");
      const recentSearchesHost = document.getElementById("recent-searches");
      const groupChips = document.getElementById("group-chips");
      const browserRoot = document.getElementById("browser-root");
      const selectionBox = document.getElementById("selection-box");
      const selectionMeta = document.getElementById("selection-meta");
      const visiblePill = document.getElementById("visible-pill");
      const browseNote = document.getElementById("browse-note");
      const selectedCountStat = document.getElementById("stat-selected-count");
      const totalVarsStat = document.getElementById("stat-total-vars");
      const totalRowsStat = document.getElementById("stat-total-rows");
      const panelNameStat = document.getElementById("stat-panel-name");
      const schemaPill = document.getElementById("schema-pill");
      const toggleSelected = document.getElementById("toggle-selected");
      const selectVisibleButton = document.getElementById("select-visible");
      const clearVisibleButton = document.getElementById("clear-visible");
      const selectionEmpty = document.getElementById("selection-empty");
      const selectionSummary = document.getElementById("selection-summary");
      const presetButtons = document.getElementById("preset-buttons");
      const saveSetName = document.getElementById("save-set-name");
      const saveCurrentSet = document.getElementById("save-current-set");
      const savedSetsHost = document.getElementById("saved-sets");
      const savedSetsMeta = document.getElementById("saved-sets-meta");
      const importBox = document.getElementById("import-box");
      const importSummary = document.getElementById("import-summary");
      const heroToggle = document.getElementById("hero-toggle");
      const resetFiltersButton = document.getElementById("reset-filters");
      const viewCardsButton = document.getElementById("view-cards");
      const viewTableButton = document.getElementById("view-table");
      const toast = document.getElementById("toast");
      const toastText = document.getElementById("toast-text");
      const undoAction = document.getElementById("undo-action");
      const detailOverlay = document.getElementById("detail-overlay");
      const detailBackdrop = document.getElementById("detail-backdrop");
      const detailContent = document.getElementById("detail-content");
      const detailTitle = document.getElementById("detail-title");
      const detailSubtitle = document.getElementById("detail-subtitle");
      const detailClose = document.getElementById("detail-close");

      const state = {
        query: "",
        group: "All",
        selectedOnly: false,
        formFilter: "all",
        panelTypeFilter: "all",
        coverageFilter: "all",
        qualityFilter: "all",
        heroCompact: false,
        viewMode: "cards",
        detailVar: "",
      };

      const groupNames = ["All", ...(payload.groupNames || [])];
      const formatNumber = (value) => new Intl.NumberFormat("en-US").format(value);
      const shortTime = (value) => {
        if (!value) {
          return "";
        }
        const parsed = new Date(value);
        if (Number.isNaN(parsed.getTime())) {
          return value;
        }
        return parsed.toLocaleString([], { year: "numeric", month: "short", day: "numeric" });
      };
      const formatPct = (value) => (value === null || value === undefined ? "n/a" : `${Number(value).toFixed(1)}%`);

      panelNameStat.textContent = payload.panelName || "";
      totalVarsStat.textContent = formatNumber(rows.length);
      totalRowsStat.textContent = formatNumber(payload.panelRows || 0);
      schemaPill.textContent = `Schema ${String(payload.schemaHash || "").slice(0, 8)} · built ${shortTime(payload.buildDate)}`;

      function safeLocalStorage(action) {
        try {
          return action();
        } catch (error) {
          return null;
        }
      }

      function buildSummaryPill(text, tone = "") {
        const pill = document.createElement("span");
        pill.className = `summary-pill${tone ? ` ${tone}` : ""}`;
        pill.textContent = text;
        return pill;
      }

      function uniqueOrdered(values) {
        const out = [];
        const seen = new Set();
        values.forEach((value) => {
          const token = String(value || "").trim();
          const key = token.toUpperCase();
          if (!token || seen.has(key)) {
            return;
          }
          seen.add(key);
          out.push(token);
        });
        return out;
      }

      function loadRecentSearches() {
        const raw = safeLocalStorage(() => localStorage.getItem(recentSearchesKey));
        if (!raw) {
          return [];
        }
        try {
          const parsed = JSON.parse(raw);
          return Array.isArray(parsed) ? parsed.filter((item) => typeof item === "string").slice(0, 8) : [];
        } catch (error) {
          return [];
        }
      }

      function storeRecentSearches(items) {
        safeLocalStorage(() => localStorage.setItem(recentSearchesKey, JSON.stringify(items.slice(0, 8))));
      }

      function rememberRecentSearch(query) {
        const token = String(query || "").trim();
        if (token.length < 2) {
          return;
        }
        const next = [token, ...loadRecentSearches().filter((item) => item.toLowerCase() !== token.toLowerCase())];
        storeRecentSearches(next);
      }

      function parseVarListDetailed(text) {
        const tokens = [];
        String(text || "")
          .split(/\\r?\\n/)
          .forEach((line) => {
            const trimmed = line.trim();
            if (!trimmed || trimmed.startsWith("#")) {
              return;
            }
            trimmed.split(",").forEach((part) => {
              const token = part.trim();
              if (token && token.toUpperCase() !== "UNITID" && token.toLowerCase() !== "year") {
                tokens.push(token);
              }
            });
          });

        const matched = [];
        const missing = [];
        const duplicates = [];
        const seen = new Set();

        tokens.forEach((token) => {
          const key = token.toUpperCase();
          if (seen.has(key)) {
            duplicates.push(token);
            return;
          }
          seen.add(key);
          const row = byNameUpper.get(key);
          if (row) {
            matched.push(row.exportName);
          } else {
            missing.push(token);
          }
        });

        return {
          tokens,
          matched,
          missing,
          duplicates,
        };
      }

      function currentSelectionRows() {
        return rows
          .filter((row) => selection.has(row.exportName))
          .sort((a, b) => a.panelOrder - b.panelOrder);
      }

      function currentSelectionText() {
        return currentSelectionRows().map((row) => row.exportName).join("\\n");
      }

      function currentManifest() {
        const selected = currentSelectionRows();
        return {
          manifestType: "ipeds-variable-browser-selection",
          panelName: payload.panelName,
          dictionaryName: payload.dictionaryName,
          schemaHash: payload.schemaHash,
          buildDate: payload.buildDate,
          panelModifiedAt: payload.panelModifiedAt,
          panelRows: payload.panelRows,
          panelYears: payload.panelYears,
          selectedCount: selected.length,
          selectedGroups: [...new Set(selected.map((row) => row.componentGroup))],
          warnings: buildSelectionWarnings(selected).map((item) => item.text),
          selectedVariables: selected.map((row) => ({
            exportName: row.exportName,
            varname: row.varname,
            componentGroup: row.componentGroup,
            semanticFamily: row.semanticFamily,
            variableForm: row.variableForm,
            panelType: row.panelType,
            completenessPct: row.completenessPct,
            coverageBucket: row.coverageBucket,
            primarySource: row.primarySource,
          })),
        };
      }

      function setSelectionFromNames(names) {
        selection.clear();
        uniqueOrdered(names).forEach((name) => {
          const row = byNameUpper.get(String(name).toUpperCase());
          if (row) {
            selection.add(row.exportName);
          }
        });
      }

      function persistSelection() {
        safeLocalStorage(() => localStorage.setItem(storageKey, currentSelectionText()));
      }

      function loadSelection() {
        const saved = safeLocalStorage(() => localStorage.getItem(storageKey));
        if (!saved) {
          return;
        }
        setSelectionFromNames(saved.split(/\\r?\\n/));
      }

      function showToast(message, options = {}) {
        toastText.textContent = message;
        toast.classList.remove("hidden");
        undoAction.classList.toggle("hidden", !options.undoable);
        if (toastTimer) {
          window.clearTimeout(toastTimer);
        }
        toastTimer = window.setTimeout(() => {
          toast.classList.add("hidden");
        }, options.undoable ? 5200 : 2600);
      }

      function rememberUndo(label, previousSelection) {
        lastUndo = { previousSelection: [...previousSelection] };
        showToast(label, { undoable: true });
      }

      function refreshSelectionBox() {
        const count = selection.size;
        selectionBox.value = currentSelectionText();
        selectionMeta.textContent = `${formatNumber(count)} variable${count === 1 ? "" : "s"} selected`;
        selectedCountStat.textContent = formatNumber(count);
        persistSelection();
      }

      function searchScore(row, query) {
        if (!query) {
          return 0;
        }
        if (row.varnameLower === query || row.exportNameLower === query) {
          return 140;
        }
        if (row.varnameLower.startsWith(query) || row.exportNameLower.startsWith(query)) {
          return 115;
        }
        if (row.varnameLower.includes(query) || row.exportNameLower.includes(query)) {
          return 92;
        }
        if (row.varTitleLower.includes(query)) {
          return 74;
        }
        if (row.variableFormLower.includes(query)) {
          return 68;
        }
        if (row.primarySourceLower === query) {
          return 64;
        }
        if (row.semanticFamilyLower.includes(query)) {
          return 62;
        }
        if (row.componentGroupLower.includes(query)) {
          return 58;
        }
        if (row.longDescriptionLower.includes(query)) {
          return 34;
        }
        return 0;
      }

      function passesFilters(row) {
        if (state.group !== "All" && row.componentGroup !== state.group) {
          return false;
        }
        if (state.selectedOnly && !selection.has(row.exportName)) {
          return false;
        }
        if (state.formFilter !== "all" && row.variableForm !== state.formFilter) {
          return false;
        }
        if (state.panelTypeFilter !== "all" && row.panelType !== state.panelTypeFilter) {
          return false;
        }
        if (state.coverageFilter !== "all" && row.coverageBucket !== state.coverageFilter) {
          return false;
        }
        if (state.qualityFilter !== "all") {
          if (["full", "strong", "partial", "sparse", "unknown"].includes(state.qualityFilter)) {
            if (row.completenessBucket !== state.qualityFilter) {
              return false;
            }
          } else if (state.qualityFilter === "mixed" && row.sourceCount <= 1) {
            return false;
          } else if (state.qualityFilter === "missing-meta" && !row.metadataMissing) {
            return false;
          }
        }
        return true;
      }

      function filteredRows() {
        const query = state.query.trim().toLowerCase();
        const visible = rows
          .filter((row) => passesFilters(row))
          .map((row) => ({ row, score: searchScore(row, query) }))
          .filter((item) => !query || item.score > 0)
          .sort((a, b) => {
            if (query && b.score !== a.score) {
              return b.score - a.score;
            }
            return a.row.panelOrder - b.row.panelOrder;
          })
          .map((item) => item.row);
        return visible;
      }

      function updateBulkActionLabels(visible) {
        const count = formatNumber(visible.length);
        selectVisibleButton.textContent = `Select visible (${count})`;
        clearVisibleButton.textContent = `Clear visible (${count})`;
        selectVisibleButton.disabled = visible.length === 0;
        clearVisibleButton.disabled = visible.length === 0;
      }

      function renderGroupChips() {
        const counts = new Map(groupNames.map((group) => [group, 0]));
        rows.forEach((row) => {
          counts.set("All", (counts.get("All") || 0) + 1);
          counts.set(row.componentGroup, (counts.get(row.componentGroup) || 0) + 1);
        });

        groupChips.innerHTML = "";
        groupNames.forEach((group) => {
          const button = document.createElement("button");
          button.type = "button";
          button.className = `chip${state.group === group ? " active" : ""}`;
          button.textContent = `${group} (${formatNumber(counts.get(group) || 0)})`;
          button.addEventListener("click", () => {
            state.group = group;
            render();
          });
          groupChips.appendChild(button);
        });
      }

      function toggleSelection(row, checked, labelPrefix = "") {
        const before = new Set(selection);
        if (checked) {
          selection.add(row.exportName);
          rememberUndo(`${labelPrefix}Added ${row.exportName}`, before);
        } else {
          selection.delete(row.exportName);
          rememberUndo(`${labelPrefix}Removed ${row.exportName}`, before);
        }
        render();
      }

      function buildCoverageStrip(row) {
        const strip = document.createElement("div");
        strip.className = "coverage-strip";
        const years = payload.panelYears || {};
        const minYear = Number(years.min);
        const maxYear = Number(years.max);
        if (!Number.isFinite(minYear) || !Number.isFinite(maxYear) || minYear > maxYear) {
          const segment = document.createElement("span");
          segment.className = "coverage-segment";
          strip.appendChild(segment);
          return strip;
        }
        for (let year = minYear; year <= maxYear; year += 1) {
          const segment = document.createElement("span");
          const active = row.yearMin !== null && row.yearMax !== null && year >= row.yearMin && year <= row.yearMax;
          segment.className = `coverage-segment${active ? " active" : ""}${row.coverageBucket === "limited" && active ? " warn" : ""}`;
          segment.title = String(year);
          strip.appendChild(segment);
        }
        return strip;
      }

      function buildCompletenessTrack(row) {
        const track = document.createElement("div");
        track.className = "completeness-track";
        const fill = document.createElement("div");
        fill.className = `completeness-fill${row.completenessBucket === "sparse" ? " warn" : ""}`;
        fill.style.width = `${Math.max(0, Math.min(100, Number(row.completenessPct || 0)))}%`;
        track.appendChild(fill);
        return track;
      }

      function setQuery(query) {
        state.query = String(query || "");
        searchBox.value = state.query;
        render();
      }

      function renderRecentSearches() {
        const recent = loadRecentSearches();
        recentSearchesHost.innerHTML = "";
        if (!recent.length) {
          const note = document.createElement("div");
          note.className = "muted";
          note.textContent = "Recent searches will appear here after you browse.";
          recentSearchesHost.appendChild(note);
          return;
        }
        recent.forEach((query) => {
          const button = document.createElement("button");
          button.type = "button";
          button.className = "chip";
          button.textContent = query;
          button.addEventListener("click", () => {
            setQuery(query);
            searchBox.focus();
          });
          recentSearchesHost.appendChild(button);
        });
        const clear = document.createElement("button");
        clear.type = "button";
        clear.className = "ghost";
        clear.textContent = "Clear recent";
        clear.addEventListener("click", () => {
          storeRecentSearches([]);
          renderRecentSearches();
        });
        recentSearchesHost.appendChild(clear);
      }

      function openDetail(row) {
        if (!row) {
          return;
        }
        state.detailVar = row.exportName;
        renderDetailDrawer();
      }

      function closeDetail() {
        state.detailVar = "";
        renderDetailDrawer();
      }

      function renderViewToggle() {
        viewCardsButton.className = state.viewMode === "cards" ? "primary" : "ghost";
        viewTableButton.className = state.viewMode === "table" ? "primary" : "ghost";
      }

      function resetFilters() {
        state.query = "";
        state.group = "All";
        state.selectedOnly = false;
        state.formFilter = "all";
        state.panelTypeFilter = "all";
        state.coverageFilter = "all";
        state.qualityFilter = "all";
        searchBox.value = "";
        document.getElementById("filter-form").value = "all";
        document.getElementById("filter-panel-type").value = "all";
        document.getElementById("filter-coverage").value = "all";
        document.getElementById("filter-quality").value = "all";
      }

      function applySelectionDelta(rowsSubset, shouldSelect, label) {
        if (!rowsSubset.length) {
          showToast("No variables available for that action");
          return;
        }
        const before = new Set(selection);
        let changed = 0;
        rowsSubset.forEach((row) => {
          const hasRow = selection.has(row.exportName);
          if (shouldSelect && !hasRow) {
            selection.add(row.exportName);
            changed += 1;
          } else if (!shouldSelect && hasRow) {
            selection.delete(row.exportName);
            changed += 1;
          }
        });
        if (!changed) {
          showToast(`No changes: ${label}`);
          return;
        }
        rememberUndo(`${label} (${formatNumber(changed)})`, before);
        render();
      }

      function buildCard(row) {
        const card = document.createElement("div");
        card.className = `var-card${selection.has(row.exportName) ? " selected" : ""}`;

        const top = document.createElement("div");
        top.className = "var-top";

        const checkbox = document.createElement("input");
        checkbox.type = "checkbox";
        checkbox.checked = selection.has(row.exportName);
        checkbox.addEventListener("change", () => {
          toggleSelection(row, checkbox.checked);
        });
        top.appendChild(checkbox);

        const textWrap = document.createElement("div");
        textWrap.className = "var-copy";
        const name = document.createElement("h4");
        name.className = "var-name";
        name.textContent = row.varname;
        textWrap.appendChild(name);

        const title = document.createElement("div");
        title.className = "var-title";
        title.textContent = row.varTitle || "No dictionary title available";
        textWrap.appendChild(title);
        top.appendChild(textWrap);
        card.appendChild(top);

        const desc = document.createElement("div");
        desc.className = "var-desc";
        desc.textContent = row.longDescription || "No long description available for this column in dictionary_lake.";
        card.appendChild(desc);

        const meta = document.createElement("div");
        meta.className = "meta-row";
        const badges = [];
        if (row.primarySource) {
          badges.push({ text: `source ${row.primarySource}` });
        }
        if (row.sourceCount > 1) {
          badges.push({ text: `+${row.sourceCount - 1} more source${row.sourceCount === 2 ? "" : "s"}`, warn: true });
        }
        if (row.variableForm) {
          badges.push({ text: row.variableForm });
        }
        if (row.panelType) {
          badges.push({ text: `panel ${row.panelType}` });
        }
        if (row.dictionaryDataType) {
          badges.push({ text: `dict ${row.dictionaryDataType}` });
        }
        if (row.completenessLabel) {
          badges.push({ text: row.completenessLabel, warn: row.completenessBucket === "sparse" });
        }
        if (row.coverageLabel) {
          badges.push({ text: row.coverageLabel, warn: row.coverageBucket === "limited" });
        }
        if (row.metadataMissing) {
          badges.push({ text: "missing metadata", warn: true });
        }
        badges.forEach((item) => {
          const badge = document.createElement("span");
          badge.className = `badge${item.warn ? " warn" : ""}`;
          badge.textContent = item.text;
          meta.appendChild(badge);
        });
        card.appendChild(meta);

        const coverage = buildCoverageStrip(row);
        card.appendChild(coverage);

        const actions = document.createElement("div");
        actions.className = "card-actions";
        const family = document.createElement("div");
        family.className = "muted";
        family.textContent = row.semanticFamily || row.componentGroup;
        const inspect = document.createElement("button");
        inspect.type = "button";
        inspect.className = "ghost";
        inspect.textContent = "Inspect";
        inspect.addEventListener("click", () => openDetail(row));
        actions.append(family, inspect);
        card.appendChild(actions);

        return card;
      }

      function renderTableBrowser(visible) {
        const wrap = document.createElement("div");
        wrap.className = "table-wrap";
        const table = document.createElement("table");
        table.className = "var-table";
        table.innerHTML = `
          <thead>
            <tr>
              <th>Select</th>
              <th>Variable</th>
              <th>Family</th>
              <th>Type</th>
              <th>Years</th>
              <th>Completeness</th>
              <th>Source</th>
              <th></th>
            </tr>
          </thead>
        `;

        const tbody = document.createElement("tbody");
        visible.forEach((row) => {
          const tr = document.createElement("tr");
          if (selection.has(row.exportName)) {
            tr.classList.add("selected");
          }

          const selectCell = document.createElement("td");
          const checkbox = document.createElement("input");
          checkbox.type = "checkbox";
          checkbox.checked = selection.has(row.exportName);
          checkbox.addEventListener("click", (event) => event.stopPropagation());
          checkbox.addEventListener("change", () => toggleSelection(row, checkbox.checked));
          selectCell.appendChild(checkbox);
          tr.appendChild(selectCell);

          const nameCell = document.createElement("td");
          const nameWrap = document.createElement("div");
          nameWrap.className = "table-name";
          const name = document.createElement("div");
          name.className = "table-var";
          name.textContent = row.varname;
          const title = document.createElement("div");
          title.className = "table-title";
          title.textContent = row.varTitle || "No dictionary title available";
          nameWrap.append(name, title);
          nameCell.appendChild(nameWrap);
          tr.appendChild(nameCell);

          const familyCell = document.createElement("td");
          const familyStrong = document.createElement("strong");
          familyStrong.textContent = row.semanticFamily || "Other variables";
          const familyMeta = document.createElement("div");
          familyMeta.className = "table-compact";
          familyMeta.textContent = row.componentGroup;
          familyCell.append(familyStrong, familyMeta);
          tr.appendChild(familyCell);

          const typeCell = document.createElement("td");
          const typeMeta = document.createElement("div");
          typeMeta.className = "table-meta";
          const typeValue = document.createElement("div");
          typeValue.textContent = row.variableForm || "measure";
          const typeLabel = document.createElement("div");
          typeLabel.className = "table-compact";
          typeLabel.textContent = `panel ${row.panelType}`;
          typeMeta.append(typeValue, typeLabel);
          typeCell.appendChild(typeMeta);
          tr.appendChild(typeCell);

          const yearsCell = document.createElement("td");
          yearsCell.append(buildCoverageStrip(row));
          const yearsLabel = document.createElement("div");
          yearsLabel.className = "table-compact";
          yearsLabel.textContent = row.coverageLabel;
          yearsCell.appendChild(yearsLabel);
          tr.appendChild(yearsCell);

          const completenessCell = document.createElement("td");
          completenessCell.append(buildCompletenessTrack(row));
          const completenessLabel = document.createElement("div");
          completenessLabel.className = "table-compact";
          completenessLabel.textContent = row.completenessLabel;
          completenessCell.appendChild(completenessLabel);
          tr.appendChild(completenessCell);

          const sourceCell = document.createElement("td");
          const sourceMeta = document.createElement("div");
          sourceMeta.className = "table-meta";
          const sourceValue = document.createElement("div");
          sourceValue.textContent = row.primarySource || "n/a";
          const sourceLabel = document.createElement("div");
          sourceLabel.className = "table-compact";
          sourceLabel.textContent = row.sourceCount > 1 ? `${row.sourceCount} source histories` : "single source history";
          sourceMeta.append(sourceValue, sourceLabel);
          sourceCell.appendChild(sourceMeta);
          tr.appendChild(sourceCell);

          const actionCell = document.createElement("td");
          const inspect = document.createElement("button");
          inspect.type = "button";
          inspect.className = "ghost";
          inspect.textContent = "Inspect";
          inspect.addEventListener("click", (event) => {
            event.stopPropagation();
            openDetail(row);
          });
          actionCell.appendChild(inspect);
          tr.appendChild(actionCell);

          tr.addEventListener("click", () => openDetail(row));
          tbody.appendChild(tr);
        });

        table.appendChild(tbody);
        wrap.appendChild(table);
        browserRoot.innerHTML = "";
        browserRoot.appendChild(wrap);
      }

      function renderDetailDrawer() {
        const row = byExportName.get(state.detailVar);
        if (!row) {
          detailOverlay.classList.add("hidden");
          detailContent.innerHTML = "";
          detailTitle.textContent = "Variable detail";
          detailSubtitle.textContent = "Inspect metadata, coverage, quality, and nearby variables without losing your place.";
          return;
        }

        detailOverlay.classList.remove("hidden");
        detailTitle.textContent = row.varname;
        detailSubtitle.textContent = row.varTitle || "No dictionary title available";
        detailContent.innerHTML = "";

        const actions = document.createElement("div");
        actions.className = "detail-section";
        const actionGrid = document.createElement("div");
        actionGrid.className = "drawer-actions";
        const selectButton = document.createElement("button");
        selectButton.type = "button";
        selectButton.className = selection.has(row.exportName) ? "ghost" : "primary";
        selectButton.textContent = selection.has(row.exportName) ? "Remove from selection" : "Add to selection";
        selectButton.addEventListener("click", () => toggleSelection(row, !selection.has(row.exportName), "Drawer: "));
        actionGrid.appendChild(selectButton);
        actions.appendChild(actionGrid);
        detailContent.appendChild(actions);

        const description = document.createElement("section");
        description.className = "detail-section";
        const descriptionHeading = document.createElement("h3");
        descriptionHeading.textContent = "Description";
        const descriptionBody = document.createElement("div");
        descriptionBody.textContent = row.longDescription || "No long description available for this column in dictionary_lake.";
        description.append(descriptionHeading, descriptionBody);
        detailContent.appendChild(description);

        const metrics = document.createElement("section");
        metrics.className = "detail-section";
        const metricsHeading = document.createElement("h3");
        metricsHeading.textContent = "Metadata";
        metrics.appendChild(metricsHeading);
        const metricGrid = document.createElement("div");
        metricGrid.className = "metric-grid";
        [
          ["Component", row.componentGroup],
          ["Semantic family", row.semanticFamily || "Other variables"],
          ["Variable form", row.variableForm || "measure"],
          ["Panel type", row.panelType || "other"],
          ["Dictionary type", row.dictionaryDataType || "n/a"],
          ["Primary source", row.primarySource || "n/a"],
          ["Coverage", row.coverageLabel],
          ["Completeness", row.completenessLabel],
        ].forEach(([label, value]) => {
          const metric = document.createElement("div");
          metric.className = "metric";
          const metricLabel = document.createElement("div");
          metricLabel.className = "metric-label";
          metricLabel.textContent = label;
          const metricValue = document.createElement("div");
          metricValue.className = "metric-value";
          metricValue.textContent = value;
          metric.append(metricLabel, metricValue);
          metricGrid.appendChild(metric);
        });
        metrics.appendChild(metricGrid);
        detailContent.appendChild(metrics);

        const coverage = document.createElement("section");
        coverage.className = "detail-section";
        const coverageHeading = document.createElement("h3");
        coverageHeading.textContent = "Coverage and quality";
        coverage.appendChild(coverageHeading);
        const coverageLabel = document.createElement("div");
        coverageLabel.className = "muted";
        coverageLabel.textContent = `${row.coverageLabel} · ${row.completenessLabel}`;
        coverage.appendChild(coverageLabel);
        coverage.appendChild(buildCoverageStrip(row));
        coverage.appendChild(buildCompletenessTrack(row));
        detailContent.appendChild(coverage);

        const warnings = buildSelectionWarnings([row]);
        if (warnings.length) {
          const warningSection = document.createElement("section");
          warningSection.className = "detail-section";
          const warningHeading = document.createElement("h3");
          warningHeading.textContent = "Warnings";
          warningSection.appendChild(warningHeading);
          const stack = document.createElement("div");
          stack.className = "summary-stack";
          warnings.forEach((item) => stack.appendChild(buildSummaryPill(item.text, item.tone)));
          warningSection.appendChild(stack);
          detailContent.appendChild(warningSection);
        }

        const relatedSection = document.createElement("section");
        relatedSection.className = "detail-section";
        const relatedHeading = document.createElement("h3");
        relatedHeading.textContent = "Related variables";
        relatedSection.appendChild(relatedHeading);
        const relatedList = document.createElement("div");
        relatedList.className = "related-list";
        const relatedRows = (row.relatedVariables || [])
          .map((name) => byExportName.get(name))
          .filter(Boolean);
        if (!relatedRows.length) {
          const note = document.createElement("div");
          note.className = "muted";
          note.textContent = "No related-variable hints are available for this column yet.";
          relatedList.appendChild(note);
        } else {
          relatedRows.forEach((related) => {
            const item = document.createElement("div");
            item.className = "related-item";
            const copy = document.createElement("div");
            copy.className = "related-copy";
            const relatedName = document.createElement("div");
            relatedName.className = "related-name";
            relatedName.textContent = related.varname;
            const relatedDesc = document.createElement("div");
            relatedDesc.className = "related-desc";
            relatedDesc.textContent = related.varTitle || related.semanticFamily || related.componentGroup;
            const relatedMeta = document.createElement("div");
            relatedMeta.className = "table-compact";
            relatedMeta.textContent = related.semanticFamily || related.componentGroup;
            copy.append(relatedName, relatedDesc, relatedMeta);
            const buttons = document.createElement("div");
            buttons.className = "related-actions";
            const openButton = document.createElement("button");
            openButton.type = "button";
            openButton.className = "ghost";
            openButton.textContent = "Open";
            openButton.addEventListener("click", () => openDetail(related));
            const selectRelated = document.createElement("button");
            selectRelated.type = "button";
            selectRelated.className = selection.has(related.exportName) ? "ghost" : "secondary";
            selectRelated.textContent = selection.has(related.exportName) ? "Selected" : "Select";
            selectRelated.addEventListener("click", () => {
              if (!selection.has(related.exportName)) {
                toggleSelection(related, true, "Related: ");
              }
            });
            buttons.append(openButton, selectRelated);
            item.append(copy, buttons);
            relatedList.appendChild(item);
          });
        }
        relatedSection.appendChild(relatedList);
        detailContent.appendChild(relatedSection);
      }

      function renderBrowser() {
        const visible = filteredRows();
        visiblePill.textContent = `Visible variables: ${formatNumber(visible.length)}`;
        browseNote.textContent = state.selectedOnly
          ? "Showing only the currently selected variables."
          : state.query
            ? `Weighted search matches for "${state.query.trim()}".`
            : state.viewMode === "table"
              ? "Table mode is optimized for dense scanning across multiple metadata columns."
              : "Search favors exact varnames first, then families, titles, and descriptions.";
        updateBulkActionLabels(visible);

        if (!visible.length) {
          browserRoot.innerHTML = '<div class="empty-state">No variables match the current filters. Broaden the search or clear one of the facets.</div>';
          return visible;
        }

        if (state.viewMode === "table") {
          renderTableBrowser(visible);
          return visible;
        }

        const groups = new Map();
        visible.forEach((row) => {
          if (!groups.has(row.componentGroup)) {
            groups.set(row.componentGroup, new Map());
          }
          const family = row.semanticFamily || "Other variables";
          if (!groups.get(row.componentGroup).has(family)) {
            groups.get(row.componentGroup).set(family, []);
          }
          groups.get(row.componentGroup).get(family).push(row);
        });

        browserRoot.innerHTML = "";
        (payload.groupNames || [])
          .filter((group) => groups.has(group))
          .forEach((group) => {
            const section = document.createElement("section");
            section.className = "group-section";

            const groupRows = [...groups.get(group).values()].flat();
            const rowsInGroup = groupRows.length;
            const head = document.createElement("div");
            head.className = "group-head";
            const title = document.createElement("h3");
            title.textContent = group;
            const headActions = document.createElement("div");
            headActions.className = "group-head-actions";
            const count = document.createElement("div");
            count.className = "group-count";
            count.textContent = `${formatNumber(rowsInGroup)} visible`;
            const selectGroup = document.createElement("button");
            selectGroup.type = "button";
            selectGroup.className = "secondary";
            selectGroup.textContent = `Select group (${formatNumber(rowsInGroup)})`;
            selectGroup.addEventListener("click", () => {
              applySelectionDelta(groupRows, true, `Selected group "${group}"`);
            });
            const clearGroup = document.createElement("button");
            clearGroup.type = "button";
            clearGroup.className = "ghost";
            clearGroup.textContent = `Clear group (${formatNumber(groupRows.filter((row) => selection.has(row.exportName)).length)})`;
            clearGroup.addEventListener("click", () => {
              applySelectionDelta(groupRows, false, `Cleared group "${group}"`);
            });
            headActions.append(count, selectGroup, clearGroup);
            head.append(title, headActions);
            section.appendChild(head);

            [...groups.get(group).entries()]
              .sort((a, b) => a[0].localeCompare(b[0]))
              .forEach(([family, items]) => {
                const details = document.createElement("details");
                details.className = "source-cluster";
                details.open = state.query.trim() !== "" || state.selectedOnly || items.some((row) => selection.has(row.exportName));

                const sources = [...new Set(items.map((row) => row.primarySource).filter(Boolean))];
                const multiSourceCount = items.filter((row) => row.sourceCount > 1).length;
                const sourcePreview = sources.length
                  ? `sources ${sources.slice(0, 3).join(", ")}${sources.length > 3 ? ` +${sources.length - 3}` : ""}`
                  : "no source metadata";
                const subline = [
                  `${formatNumber(items.length)} vars`,
                  sourcePreview,
                  multiSourceCount ? `${formatNumber(multiSourceCount)} mixed history` : "",
                ].filter(Boolean).join(" · ");
                details.innerHTML = `<summary><div><div class="source-name">${family}</div><div class="source-sub">${subline}</div></div><div class="group-count">${formatNumber(items.length)}</div></summary>`;

                const familyActions = document.createElement("div");
                familyActions.className = "family-actions";
                const selectFamily = document.createElement("button");
                selectFamily.type = "button";
                selectFamily.className = "secondary";
                selectFamily.textContent = `Select family (${formatNumber(items.length)})`;
                selectFamily.addEventListener("click", () => {
                  applySelectionDelta(items, true, `Selected family "${family}"`);
                });
                const selectedInFamily = items.filter((row) => selection.has(row.exportName)).length;
                const clearFamily = document.createElement("button");
                clearFamily.type = "button";
                clearFamily.className = "ghost";
                clearFamily.textContent = `Clear family (${formatNumber(selectedInFamily)})`;
                clearFamily.addEventListener("click", () => {
                  applySelectionDelta(items, false, `Cleared family "${family}"`);
                });
                familyActions.append(selectFamily, clearFamily);
                details.appendChild(familyActions);

                const body = document.createElement("div");
                body.className = "cluster-body";
                items.forEach((row) => body.appendChild(buildCard(row)));
                details.appendChild(body);
                section.appendChild(details);
              });

            browserRoot.appendChild(section);
          });

        return visible;
      }

      function buildSelectionWarnings(selected) {
        const warnings = [];
        const sparse = selected.filter((row) => row.completenessBucket === "sparse").length;
        const limited = selected.filter((row) => row.coverageBucket === "limited").length;
        const mixed = selected.filter((row) => row.sourceCount > 1).length;
        const missingMeta = selected.filter((row) => row.metadataMissing).length;
        if (sparse) {
          warnings.push({ text: `${formatNumber(sparse)} sparse completeness`, tone: "warn" });
        }
        if (limited) {
          warnings.push({ text: `${formatNumber(limited)} limited year coverage`, tone: "warn" });
        }
        if (mixed) {
          warnings.push({ text: `${formatNumber(mixed)} mixed source history`, tone: "" });
        }
        if (missingMeta) {
          warnings.push({ text: `${formatNumber(missingMeta)} missing metadata`, tone: "" });
        }
        return warnings;
      }

      function renderSelectionWorkbench() {
        const selected = currentSelectionRows();
        selectionEmpty.classList.toggle("hidden", selected.length > 0);
        selectionSummary.classList.toggle("hidden", selected.length === 0);
        saveCurrentSet.disabled = selected.length === 0;

        if (!selected.length) {
          selectionSummary.innerHTML = "";
          return;
        }

        selectionSummary.innerHTML = "";
        const groups = new Map();
        selected.forEach((row) => {
          groups.set(row.componentGroup, (groups.get(row.componentGroup) || 0) + 1);
        });

        const head = document.createElement("div");
        head.className = "section-head";
        const heading = document.createElement("h3");
        heading.textContent = "Current Selection";
        const meta = document.createElement("div");
        meta.className = "section-meta";
        meta.textContent = `${formatNumber(selected.length)} vars across ${formatNumber(groups.size)} groups`;
        head.append(heading, meta);
        selectionSummary.appendChild(head);

        const counts = document.createElement("div");
        counts.className = "summary-stack";
        [...groups.entries()]
          .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
          .forEach(([group, count]) => {
            counts.appendChild(buildSummaryPill(`${group}: ${formatNumber(count)}`));
          });
        selectionSummary.appendChild(counts);

        const warnings = buildSelectionWarnings(selected);
        if (warnings.length) {
          const warningBox = document.createElement("div");
          warningBox.className = "summary-box warn";
          const title = document.createElement("strong");
          title.textContent = "Selection warnings";
          warningBox.appendChild(title);
          const stack = document.createElement("div");
          stack.className = "summary-stack";
          warnings.forEach((item) => stack.appendChild(buildSummaryPill(item.text, item.tone)));
          warningBox.appendChild(stack);
          selectionSummary.appendChild(warningBox);
        }

        const chips = document.createElement("div");
        chips.className = "selected-chip-list";
        selected.slice(0, 28).forEach((row) => {
          const chip = document.createElement("div");
          chip.className = "selected-chip";
          const label = document.createElement("span");
          label.textContent = row.exportName;
          const remove = document.createElement("button");
          remove.type = "button";
          remove.className = "ghost";
          remove.textContent = "remove";
          remove.addEventListener("click", () => {
            const before = new Set(selection);
            selection.delete(row.exportName);
            rememberUndo(`Removed ${row.exportName}`, before);
            render();
          });
          chip.append(label, remove);
          chips.appendChild(chip);
        });
        if (selected.length > 28) {
          chips.appendChild(buildSummaryPill(`+${formatNumber(selected.length - 28)} more`));
        }
        selectionSummary.appendChild(chips);
      }

      function loadSavedSets() {
        const raw = safeLocalStorage(() => localStorage.getItem(savedSetsKey));
        if (!raw) {
          return [];
        }
        try {
          const parsed = JSON.parse(raw);
          if (!Array.isArray(parsed)) {
            return [];
          }
          return parsed
            .filter((item) => item && typeof item.name === "string" && Array.isArray(item.variables))
            .slice(0, 25);
        } catch (error) {
          return [];
        }
      }

      function storeSavedSets(savedSets) {
        safeLocalStorage(() => localStorage.setItem(savedSetsKey, JSON.stringify(savedSets.slice(0, 25))));
      }

      function nextSavedSetName(baseName, existingIds = new Set()) {
        const existingNames = new Set(loadSavedSets()
          .filter((item) => !existingIds.has(item.id))
          .map((item) => item.name.toLowerCase()));
        let candidate = `${baseName} copy`;
        let counter = 2;
        while (existingNames.has(candidate.toLowerCase())) {
          candidate = `${baseName} copy ${counter}`;
          counter += 1;
        }
        return candidate;
      }

      function renderSavedSets() {
        const savedSets = loadSavedSets();
        savedSetsMeta.textContent = `${formatNumber(savedSets.length)} saved`;
        savedSetsHost.innerHTML = "";

        if (!savedSets.length) {
          const note = document.createElement("div");
          note.className = "muted";
          note.textContent = "No saved sets for this schema yet.";
          savedSetsHost.appendChild(note);
          return;
        }

        savedSets.forEach((savedSet) => {
          const row = document.createElement("div");
          row.className = "saved-set";

          const metaWrap = document.createElement("div");
          const name = document.createElement("div");
          name.className = "saved-set-name";
          name.textContent = savedSet.name;
          const detail = document.createElement("div");
          detail.className = "saved-set-meta";
          detail.textContent = `${formatNumber(savedSet.variables.length)} vars · updated ${shortTime(savedSet.updatedAt)}`;
          metaWrap.append(name, detail);

          const actions = document.createElement("div");
          actions.className = "saved-set-actions";

          const load = document.createElement("button");
          load.type = "button";
          load.className = "secondary";
          load.textContent = "Replace";
          load.addEventListener("click", () => {
            const before = new Set(selection);
            setSelectionFromNames(savedSet.variables);
            rememberUndo(`Loaded saved set "${savedSet.name}"`, before);
            render();
          });

          const add = document.createElement("button");
          add.type = "button";
          add.className = "ghost";
          add.textContent = "Add";
          add.addEventListener("click", () => {
            const rowsToAdd = savedSet.variables
              .map((name) => byNameUpper.get(String(name).toUpperCase()))
              .filter(Boolean);
            applySelectionDelta(rowsToAdd, true, `Added saved set "${savedSet.name}"`);
          });

          const duplicate = document.createElement("button");
          duplicate.type = "button";
          duplicate.className = "ghost";
          duplicate.textContent = "Duplicate";
          duplicate.addEventListener("click", () => {
            const savedSetsNow = loadSavedSets();
            const duplicateName = nextSavedSetName(savedSet.name);
            const record = {
              id: `${Date.now()}-${Math.random().toString(16).slice(2, 8)}`,
              name: duplicateName,
              variables: [...savedSet.variables],
              updatedAt: new Date().toISOString(),
            };
            storeSavedSets([record, ...savedSetsNow]);
            renderSavedSets();
            showToast(`Duplicated saved set "${duplicateName}"`);
          });

          const rename = document.createElement("button");
          rename.type = "button";
          rename.className = "ghost";
          rename.textContent = "Rename";
          rename.addEventListener("click", () => {
            const nextName = window.prompt("Rename saved set", savedSet.name);
            if (nextName === null) {
              return;
            }
            const trimmed = nextName.trim();
            if (!trimmed) {
              showToast("Saved set name cannot be empty");
              return;
            }
            const savedSetsNow = loadSavedSets();
            const nameConflict = savedSetsNow.some((item) => item.id !== savedSet.id && item.name.toLowerCase() === trimmed.toLowerCase());
            if (nameConflict) {
              showToast(`Saved set "${trimmed}" already exists`);
              return;
            }
            const updated = savedSetsNow.map((item) => (
              item.id === savedSet.id
                ? { ...item, name: trimmed, updatedAt: new Date().toISOString() }
                : item
            ));
            storeSavedSets(updated);
            renderSavedSets();
            showToast(`Renamed saved set to "${trimmed}"`);
          });

          const remove = document.createElement("button");
          remove.type = "button";
          remove.className = "ghost";
          remove.textContent = "Delete";
          remove.addEventListener("click", () => {
            const updated = loadSavedSets().filter((item) => item.id !== savedSet.id);
            storeSavedSets(updated);
            renderSavedSets();
            showToast(`Deleted saved set "${savedSet.name}"`);
          });

          actions.append(load, add, duplicate, rename, remove);
          row.append(metaWrap, actions);
          savedSetsHost.appendChild(row);
        });
      }

      function renderPresetButtons() {
        presetButtons.innerHTML = "";
        presets.forEach((preset) => {
          const button = document.createElement("button");
          button.type = "button";
          button.className = "chip secondary";
          button.textContent = `${preset.label} (${formatNumber((preset.variables || []).length)})`;
          button.title = preset.description || preset.label;
          button.addEventListener("click", () => {
            const before = new Set(selection);
            setSelectionFromNames(preset.variables || []);
            rememberUndo(`Loaded preset "${preset.label}"`, before);
            render();
          });
          presetButtons.appendChild(button);
        });
      }

      function renderImportPreview() {
        const parsed = parseVarListDetailed(importBox.value);
        importSummary.innerHTML = "";

        if (!parsed.tokens.length) {
          importSummary.className = "summary-box muted";
          importSummary.innerHTML = 'Paste a variable list to see matches, missing names, and duplicates before loading it.';
          return parsed;
        }

        importSummary.className = "summary-box";
        const top = document.createElement("div");
        top.className = "summary-stack";
        top.appendChild(buildSummaryPill(`${formatNumber(parsed.matched.length)} matched`));
        top.appendChild(buildSummaryPill(`${formatNumber(parsed.missing.length)} missing`, parsed.missing.length ? "warn" : ""));
        top.appendChild(buildSummaryPill(`${formatNumber(parsed.duplicates.length)} duplicates`, parsed.duplicates.length ? "warn" : ""));
        top.appendChild(buildSummaryPill("UNITID and year auto-kept"));
        importSummary.appendChild(top);

        if (parsed.missing.length) {
          const miss = document.createElement("div");
          miss.className = "muted";
          miss.textContent = `Missing sample: ${parsed.missing.slice(0, 8).join(", ")}${parsed.missing.length > 8 ? " ..." : ""}`;
          importSummary.appendChild(miss);
        }
        if (parsed.duplicates.length) {
          const dup = document.createElement("div");
          dup.className = "muted";
          dup.textContent = `Duplicate sample: ${parsed.duplicates.slice(0, 8).join(", ")}${parsed.duplicates.length > 8 ? " ..." : ""}`;
          importSummary.appendChild(dup);
        }
        return parsed;
      }

      function applyHeroMode() {
        document.body.classList.toggle("hero-compact", state.heroCompact);
        heroToggle.textContent = state.heroCompact ? "Expand header" : "Compact header";
        safeLocalStorage(() => localStorage.setItem(heroCompactKey, state.heroCompact ? "1" : "0"));
      }

      function render() {
        renderRecentSearches();
        renderViewToggle();
        renderGroupChips();
        renderBrowser();
        renderDetailDrawer();
        refreshSelectionBox();
        renderSelectionWorkbench();
        renderSavedSets();
        toggleSelected.className = state.selectedOnly ? "primary" : "ghost";
      }

      document.getElementById("filter-form").addEventListener("change", (event) => {
        state.formFilter = event.target.value;
        render();
      });

      document.getElementById("filter-panel-type").addEventListener("change", (event) => {
        state.panelTypeFilter = event.target.value;
        render();
      });

      document.getElementById("filter-coverage").addEventListener("change", (event) => {
        state.coverageFilter = event.target.value;
        render();
      });

      document.getElementById("filter-quality").addEventListener("change", (event) => {
        state.qualityFilter = event.target.value;
        render();
      });

      document.getElementById("select-visible").addEventListener("click", () => {
        const visible = filteredRows();
        if (!visible.length) {
          return;
        }
        const before = new Set(selection);
        visible.forEach((row) => selection.add(row.exportName));
        rememberUndo(`Selected ${formatNumber(visible.length)} visible vars`, before);
        render();
      });

      document.getElementById("clear-visible").addEventListener("click", () => {
        const visible = filteredRows();
        if (!visible.length) {
          return;
        }
        const before = new Set(selection);
        visible.forEach((row) => selection.delete(row.exportName));
        rememberUndo(`Cleared ${formatNumber(visible.length)} visible vars`, before);
        render();
      });

      document.getElementById("clear-selection").addEventListener("click", () => {
        if (!selection.size) {
          return;
        }
        const before = new Set(selection);
        selection.clear();
        rememberUndo("Cleared current selection", before);
        render();
      });

      document.getElementById("toggle-selected").addEventListener("click", () => {
        state.selectedOnly = !state.selectedOnly;
        render();
      });

      document.getElementById("copy-selection").addEventListener("click", async () => {
        const text = currentSelectionText();
        if (!text) {
          return;
        }
        try {
          await navigator.clipboard.writeText(text);
          showToast("Copied selectedvars.txt list");
        } catch (error) {
          selectionBox.focus();
          selectionBox.select();
          document.execCommand("copy");
          showToast("Copied selectedvars.txt list");
        }
      });

      document.getElementById("download-selection").addEventListener("click", () => {
        const text = currentSelectionText();
        const blob = new Blob([text + (text ? "\\n" : "")], { type: "text/plain;charset=utf-8" });
        const href = URL.createObjectURL(blob);
        const anchor = document.createElement("a");
        anchor.href = href;
        anchor.download = "selectedvars.txt";
        document.body.appendChild(anchor);
        anchor.click();
        anchor.remove();
        URL.revokeObjectURL(href);
        showToast("Downloaded selectedvars.txt");
      });

      document.getElementById("download-manifest").addEventListener("click", () => {
        const blob = new Blob([JSON.stringify(currentManifest(), null, 2)], { type: "application/json;charset=utf-8" });
        const href = URL.createObjectURL(blob);
        const anchor = document.createElement("a");
        anchor.href = href;
        anchor.download = "selectedvars_manifest.json";
        document.body.appendChild(anchor);
        anchor.click();
        anchor.remove();
        URL.revokeObjectURL(href);
        showToast("Downloaded selection manifest");
      });

      document.getElementById("apply-import").addEventListener("click", () => {
        const parsed = renderImportPreview();
        if (!parsed.matched.length) {
          showToast("Import has no matched variables");
          return;
        }
        const before = new Set(selection);
        setSelectionFromNames(parsed.matched);
        rememberUndo(`Loaded import: ${formatNumber(parsed.matched.length)} matched`, before);
        render();
      });

      document.getElementById("merge-import").addEventListener("click", () => {
        const parsed = renderImportPreview();
        if (!parsed.matched.length) {
          showToast("Import has no matched variables");
          return;
        }
        const rowsToAdd = parsed.matched
          .map((name) => byNameUpper.get(String(name).toUpperCase()))
          .filter(Boolean);
        applySelectionDelta(rowsToAdd, true, `Added import matches`);
      });

      document.getElementById("clear-import").addEventListener("click", () => {
        importBox.value = "";
        renderImportPreview();
      });

      document.getElementById("save-current-set").addEventListener("click", () => {
        const selected = currentSelectionRows();
        if (!selected.length) {
          return;
        }
        const name = saveSetName.value.trim() || `Selection ${new Date().toLocaleString()}`;
        const savedSets = loadSavedSets();
        const now = new Date().toISOString();
        const existing = savedSets.find((item) => item.name.toLowerCase() === name.toLowerCase());
        const record = {
          id: existing ? existing.id : `${Date.now()}-${Math.random().toString(16).slice(2, 8)}`,
          name,
          variables: selected.map((row) => row.exportName),
          updatedAt: now,
        };
        const next = existing
          ? savedSets.map((item) => (item.id === existing.id ? record : item))
          : [record, ...savedSets];
        storeSavedSets(next);
        saveSetName.value = "";
        renderSavedSets();
        showToast(`Saved set "${name}"`);
      });

      undoAction.addEventListener("click", () => {
        if (!lastUndo) {
          return;
        }
        setSelectionFromNames(lastUndo.previousSelection);
        lastUndo = null;
        toast.classList.add("hidden");
        render();
      });

      selectionBox.addEventListener("change", () => {
        const before = new Set(selection);
        const parsed = parseVarListDetailed(selectionBox.value);
        setSelectionFromNames(parsed.matched);
        rememberUndo("Applied manual export edits", before);
        render();
      });

      importBox.addEventListener("input", () => {
        renderImportPreview();
      });

      searchBox.addEventListener("input", (event) => {
        state.query = event.target.value;
        render();
      });

      searchBox.addEventListener("change", () => {
        rememberRecentSearch(searchBox.value);
        renderRecentSearches();
      });

      searchBox.addEventListener("keydown", (event) => {
        if (event.key === "Enter") {
          rememberRecentSearch(searchBox.value);
          renderRecentSearches();
        }
      });

      heroToggle.addEventListener("click", () => {
        state.heroCompact = !state.heroCompact;
        applyHeroMode();
      });

      resetFiltersButton.addEventListener("click", () => {
        resetFilters();
        render();
      });

      viewCardsButton.addEventListener("click", () => {
        state.viewMode = "cards";
        safeLocalStorage(() => localStorage.setItem(viewModeKey, state.viewMode));
        render();
      });

      viewTableButton.addEventListener("click", () => {
        state.viewMode = "table";
        safeLocalStorage(() => localStorage.setItem(viewModeKey, state.viewMode));
        render();
      });

      detailClose.addEventListener("click", () => {
        closeDetail();
      });

      detailBackdrop.addEventListener("click", () => {
        closeDetail();
      });

      window.addEventListener("keydown", (event) => {
        const target = event.target;
        const isEditable = target && (target.tagName === "INPUT" || target.tagName === "TEXTAREA" || target.isContentEditable);
        if (event.key === "Escape" && state.detailVar) {
          closeDetail();
        }
        if (!isEditable && event.key === "/") {
          event.preventDefault();
          searchBox.focus();
          searchBox.select();
        }
        if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
          event.preventDefault();
          searchBox.focus();
          searchBox.select();
        }
      });

      const hasSeenIntro = safeLocalStorage(() => localStorage.getItem(introSeenKey)) === "1";
      const storedHeroCompact = safeLocalStorage(() => localStorage.getItem(heroCompactKey));
      const storedViewMode = safeLocalStorage(() => localStorage.getItem(viewModeKey));
      state.heroCompact = storedHeroCompact === null ? hasSeenIntro : storedHeroCompact === "1";
      state.viewMode = storedViewMode === "table" ? "table" : "cards";
      safeLocalStorage(() => localStorage.setItem(introSeenKey, "1"));
      applyHeroMode();

      loadSelection();
      renderPresetButtons();
      renderImportPreview();
      render();
    })();
  </script>
</body>
</html>
"""


def parse_args() -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parents[1]
    data_root = Path(os.environ.get("IPEDSDB_ROOT", "/Users/markjaysonfarol13/Projects/IPEDSDB_Paneling"))
    parser = argparse.ArgumentParser(
        description="Build a static HTML variable browser for a panel parquet."
    )
    parser.add_argument(
        "--input",
        default=str(data_root / "Panels" / "panel_clean_analysis_2004_2023.parquet"),
        help="Wide or cleaned panel parquet",
    )
    parser.add_argument(
        "--dictionary",
        default=str(data_root / "Dictionary" / "dictionary_lake.parquet"),
        help="Path to dictionary_lake.parquet",
    )
    parser.add_argument(
        "--output",
        default=str(repo_root / "Customize_Panel" / "variable_browser.html"),
        help="Output HTML file",
    )
    parser.add_argument(
        "--title",
        default="IPEDS Variable Browser",
        help="HTML document title",
    )
    return parser.parse_args()


def best_text(series: pd.Series) -> str:
    vals = [str(v).strip() for v in series.dropna().tolist()]
    vals = [v for v in vals if v and v.lower() not in {"nan", "none", "<na>", "nat"}]
    if not vals:
        return ""
    vals = sorted(set(vals), key=lambda x: (-len(x), x))
    return vals[0]


def choose_primary_source(source_counts: Counter[str]) -> str:
    if not source_counts:
        return ""
    max_count = max(source_counts.values())
    winners = sorted([name for name, count in source_counts.items() if count == max_count])
    return winners[0] if winners else ""


def compute_schema_hash(panel_path: Path, names_and_types: list[tuple[str, str]], total_rows: int) -> str:
    digest = hashlib.sha256()
    digest.update(str(panel_path.name).encode("utf-8"))
    digest.update(str(total_rows).encode("utf-8"))
    for name, dtype in names_and_types:
        digest.update(name.encode("utf-8"))
        digest.update(dtype.encode("utf-8"))
    return digest.hexdigest()


def classify_completeness(pct: float | None) -> str:
    if pct is None:
        return "unknown"
    if pct >= 95:
        return "full"
    if pct >= 75:
        return "strong"
    if pct >= 40:
        return "partial"
    return "sparse"


def classify_coverage(year_min: int | None, year_max: int | None, year_count: int, window_count: int) -> str:
    if not year_count:
        return "unknown"
    if year_min is None or year_max is None or window_count <= 0:
        return "unknown"
    if year_count >= window_count:
        return "full-window"
    if year_count >= max(3, int(round(window_count * 0.75))):
        return "broad"
    if year_count >= max(2, int(round(window_count * 0.4))):
        return "partial"
    return "limited"


def infer_panel_type(field_type: str) -> str:
    text = str(field_type).strip().lower()
    if any(token in text for token in ("int", "double", "float", "decimal")):
        return "numeric"
    if "bool" in text:
        return "boolean"
    if "string" in text:
        return "string"
    return "other"


def infer_variable_form(varname: str, title: str, panel_type: str, dictionary_dtype: str) -> str:
    name = str(varname or "").strip().upper()
    title_text = str(title or "").strip().upper()
    combined = f"{name} {title_text}"

    if (
        name in {"UNITID", "OPEID", "OPEID6", "PEO1OPEID", "DFRCGID", "DFRCUSCG"}
        or name.endswith("URL")
        or " IDENTIFIER" in combined
        or combined.endswith(" IDENTIFIER")
    ):
        return "identifier"

    if any(token in combined for token in (" WEBSITE", " WEB ADDRESS", " ADDRESS", " CITY", " ZIP", " URL", " PHONE")):
        return "text"

    if any(token in combined for token in (" FLAG", " STATUS", " INDICATOR")) or name.endswith("FLG") or name.startswith("PRCH_"):
        return "flag"

    if (
        "PERCENT" in combined
        or " RATE" in combined
        or " RATIO" in combined
        or " SHARE" in combined
        or name.endswith("_P")
        or name.startswith("PCT")
        or name.endswith("PCT")
    ):
        return "rate"

    if (
        title_text.startswith("NUMBER OF")
        or " NUMBER OF " in combined
        or (" STUDENT" in combined and any(token in combined for token in (" RECEIVING", " RECIPIENT")))
        or " HEADCOUNT" in combined
        or " ENROLLMENT" in combined
        or name.endswith("_N")
        or (name.endswith("N") and any(token in name for token in ("AID", "GRNT", "LOAN", "ENR", "RET", "ADM")))
        or bool(re.search(r"(GRNT|LOAN|AID|ENR|RET|ADM|UNDUP).*[N]\\d*$", name))
    ):
        return "count"

    if any(token in combined for token in (" AMOUNT", " TUITION", " FEE", " COST", " PRICE", " EXPENSE", " EXPENDITURE", " REVENUE", " ASSET", " LIABILITY", " SALARY", " ENDOWMENT", " NET PRICE")):
        return "amount"

    if (
        dictionary_dtype.upper() in {"A", "DISC"}
        or panel_type == "string"
    ):
        if any(token in combined for token in (" NAME", " TITLE", " DESCRIPTION")):
            return "text"
        return "category"

    return "measure"


def combined_metadata_text(varname: str, title: str, description: str) -> str:
    return f"{varname or ''} {title or ''} {description or ''}".upper()


def infer_component_group(source_file: str, varname: str, title: str, description: str) -> str:
    sf = str(source_file or "").strip().upper()
    combined = combined_metadata_text(varname, title, description)

    if sf in {"ADM", "AL", "DRVADM"}:
        return "Admissions"
    if sf in {"SFA", "SFA_P", "SFAV"} or sf.startswith("F_FA"):
        return "Student Financial Aid"
    if sf.startswith("EF") or sf in {"DRVEF", "EAP"}:
        return "Enrollment / Student Profile"
    if sf.startswith("C_") or sf in {"CDEP", "DRVC", "CSTEM", "CUSTOMCG", "CUSTOMCGIDS"}:
        return "Completions"
    if sf.startswith("GR") or sf == "DRVGR":
        return "Graduation / Outcomes"
    if (sf.startswith("F_") and not sf.startswith("F_FA")) or sf == "DFR":
        return "Finance"
    if sf.startswith("S_") or sf == "DRVHR":
        return "Staff / Human Resources"

    if any(token in combined for token in ("PELL", "SCHOLARSHIP", "NET PRICE", "GRANT", "LOAN", "AID")):
        return "Student Financial Aid"
    if any(token in combined for token in ("TUITION", "ROOM", "BOARD", "PRICE", "COST OF ATTENDANCE", "APPLICATION FEE", "REQUIRED FEES", "RMBRD", "CHARGE")):
        return "Costs / Price"
    if any(token in combined for token in ("APPLICATION", "APPLICANT", "ADMISSION", "ADMIT", "ACCEPTANCE", "OPEN ADMISSIONS", "SELECTIVITY", "ACT", "SAT", "YIELD")):
        return "Admissions"
    if any(token in combined for token in ("GRADUATION RATE", "TRANSFER-OUT", "TRANSFER OUT", "OUTCOME", "COHORT DEFAULT", "RETENTION RATE")):
        return "Graduation / Outcomes"
    if any(token in combined for token in ("COMPLETION", "AWARD", "DEGREE", "CERTIFICATE", "MAJOR", "FIELD OF STUDY", "PROGRAM OF STUDY")):
        return "Completions"
    if any(token in combined for token in ("ENROLLMENT", "HEADCOUNT", "FTE", "FULL-TIME", "PART-TIME", "RETENTION", "FIRST-TIME", "FIRST TIME", "RACE", "ETHNICITY", "RESIDENT", "FOREIGN")):
        return "Enrollment / Student Profile"
    if any(token in combined for token in ("REVENUE", "EXPENSE", "EXPENDITURE", "ASSET", "LIABILITY", "DEBT", "ENDOWMENT", "INSTRUCTION", "STUDENT SERVICES", "ACADEMIC SUPPORT", "AUXILIARY")):
        return "Finance"
    if any(token in combined for token in ("FACULTY", "STAFF", "EMPLOYEE", "SALARY", "PAYROLL", "TENURE")):
        return "Staff / Human Resources"

    if sf in {"IC", "IC_AY", "IC_PY", "HD", "FLAGS", "KEYS", "ICMISSION"}:
        return "Institution / Directory"
    if sf in {"DRVIC"}:
        return "Costs / Price"
    if not sf:
        return "Panel-only / custom"
    return "Custom / Derived / Other"


def infer_semantic_family(component_group: str, varname: str, title: str, description: str) -> str:
    combined = combined_metadata_text(varname, title, description)

    if component_group == "Institution / Directory":
        if any(token in combined for token in ("STATE", "COUNTY", "REGION", "FIPS", "CBSA", "LATITUDE", "LONGITUDE", "DISTRICT", "LOCALE")):
            return "Location and geography"
        if any(token in combined for token in ("SYSTEM", "BRANCH", "PARENT", "SYSTEM NAME")):
            return "System affiliation"
        if any(token in combined for token in ("HBCU", "TRIBAL", "LAND GRANT", "MEDICAL", "HOSPITAL", "POSTSECONDARY")):
            return "Mission and institutional flags"
        if any(token in combined for token in ("CONTROL", "SECTOR", "LEVEL", "OFFER", "DEGREE", "OPEN ADMISSIONS")):
            return "Control and offerings"
        return "Institution identity"

    if component_group == "Admissions":
        if any(token in combined for token in ("ACT", "SAT", "TEST")):
            return "Test scores"
        if any(token in combined for token in ("APPLICATION", "APPLICANT", "ADMIT", "ACCEPT", "ENROLLED")):
            return "Applications and admits"
        if any(token in combined for token in ("OPEN ADMISSIONS", "SELECTIVITY", "YIELD", "ADMISSION RATE")):
            return "Access and selectivity"
        return "Other admissions"

    if component_group == "Costs / Price":
        if any(token in combined for token in ("ROOM", "BOARD", "RMBRD")):
            return "Room and board"
        if any(token in combined for token in ("APPLICATION FEE", "APPLFEE")):
            return "Application and ancillary fees"
        if any(token in combined for token in ("TUITION", "FEE", "REQUIRED FEES")):
            return "Tuition and fees"
        if any(token in combined for token in ("PRICE", "COST OF ATTENDANCE", "TOTAL COST", "CHARGE")):
            return "Cost of attendance"
        return "Other price variables"

    if component_group == "Student Financial Aid":
        if "NET PRICE" in combined:
            return "Net price and aid composition"
        if any(token in combined for token in ("RECIPIENT", "NUMBER OF STUDENTS", "ANY AID", "RECEIVING")):
            return "Aid recipients and take-up"
        if any(token in combined for token in ("PELL", "FEDERAL GRANT")):
            return "Pell and federal grants"
        if "INSTITUTIONAL GRANT" in combined:
            return "Institutional grants"
        if "LOAN" in combined:
            return "Loans"
        return "Grant aid and packaging"

    if component_group == "Enrollment / Student Profile":
        if any(token in combined for token in ("ENROLLMENT", "HEADCOUNT", "FTE", "FULL-TIME", "PART-TIME", "FIRST-TIME", "TRANSFER")):
            return "Headcount and attendance"
        if any(token in combined for token in ("RACE", "ETHNICITY", "WHITE", "BLACK", "HISPANIC", "ASIAN", "NONRESIDENT", "FOREIGN", "RESIDENT")):
            return "Student composition"
        if any(token in combined for token in ("RETENTION", "PERSISTENCE")):
            return "Retention and persistence"
        return "Other enrollment profile"

    if component_group == "Completions":
        if any(token in combined for token in ("FIELD OF STUDY", "CIP", "STEM", "MAJOR")):
            return "Fields of study"
        return "Awards and completions"

    if component_group == "Graduation / Outcomes":
        if any(token in combined for token in ("TRANSFER", "OUTCOME", "RETENTION")):
            return "Transfer and cohort outcomes"
        return "Graduation rates"

    if component_group == "Finance":
        if "REVENUE" in combined:
            return "Revenue"
        if any(token in combined for token in ("EXPENSE", "EXPENDITURE", "INSTRUCTION", "ACADEMIC SUPPORT", "STUDENT SERVICES", "AUXILIARY")):
            return "Expenditures and functional spending"
        if any(token in combined for token in ("ASSET", "LIABILITY", "DEBT")):
            return "Assets, liabilities, and debt"
        if "ENDOWMENT" in combined:
            return "Endowment and investments"
        return "Other finance"

    if component_group == "Staff / Human Resources":
        if any(token in combined for token in ("SALARY", "PAYROLL", "WAGE")):
            return "Salaries and payroll"
        if "FACULTY" in combined:
            return "Faculty profile"
        return "Staff counts and composition"

    if component_group == "Panel-only / custom":
        return "Panel-only / custom"

    return "Custom / derived"


def desired_dictionary_columns(dictionary_path: Path) -> list[str]:
    wanted = ["year", "varname", "varTitle", "longDescription", "DataType", "format", "source_file"]
    available = set(pq.read_schema(dictionary_path).names)
    return [col for col in wanted if col in available]


def build_dictionary_summary(dictionary_path: Path) -> dict[str, dict]:
    cols = desired_dictionary_columns(dictionary_path)
    df = pd.read_parquet(dictionary_path, columns=cols)
    for col in ["year", "varname", "varTitle", "longDescription", "DataType", "format", "source_file"]:
        if col not in df.columns:
            df[col] = pd.NA

    df["varname"] = df["varname"].fillna("").astype(str).str.upper().str.strip()
    df["source_file"] = df["source_file"].fillna("").astype(str).str.upper().str.strip()
    df["year_numeric"] = pd.to_numeric(df["year"], errors="coerce")
    df = df[df["varname"] != ""].copy()

    summaries: dict[str, dict] = {}
    for varname, group in df.groupby("varname", sort=False):
        counts = Counter([value for value in group["source_file"].tolist() if value])
        sources_ranked = sorted(counts, key=lambda name: (-counts[name], name))
        primary_source = choose_primary_source(counts)
        year_values = group["year_numeric"].dropna()
        summaries[varname] = {
            "varTitle": best_text(group["varTitle"]),
            "longDescription": best_text(group["longDescription"]),
            "dictionaryDataType": best_text(group["DataType"]),
            "primarySource": primary_source,
            "sourceCount": len(sources_ranked),
            "yearMin": int(year_values.min()) if not year_values.empty else None,
            "yearMax": int(year_values.max()) if not year_values.empty else None,
            "yearCount": int(year_values.nunique()) if not year_values.empty else 0,
        }
    return summaries


def build_panel_profile(input_path: Path) -> tuple[dict, dict[str, dict]]:
    parquet_file = pq.ParquetFile(input_path)
    metadata = parquet_file.metadata
    names = parquet_file.schema_arrow.names
    fields = [(field.name, str(field.type)) for field in parquet_file.schema_arrow]
    total_rows = int(metadata.num_rows)

    year_min = None
    year_max = None
    if "year" in names:
        year_idx = names.index("year")
        mins: list[int] = []
        maxs: list[int] = []
        for row_group in range(metadata.num_row_groups):
            stats = metadata.row_group(row_group).column(year_idx).statistics
            if stats is None or stats.min is None or stats.max is None:
                continue
            mins.append(int(stats.min))
            maxs.append(int(stats.max))
        if mins and maxs:
            year_min = min(mins)
            year_max = max(maxs)

    completeness: dict[str, dict] = {}
    for col_idx, name in enumerate(names):
        null_total = 0
        known = True
        for row_group in range(metadata.num_row_groups):
            stats = metadata.row_group(row_group).column(col_idx).statistics
            if stats is None or stats.null_count is None:
                known = False
                break
            null_total += int(stats.null_count)
        if total_rows == 0:
            non_null = 0
            pct = None
            bucket = "unknown"
        elif known:
            non_null = total_rows - null_total
            pct = round((non_null / total_rows) * 100, 1)
            bucket = classify_completeness(pct)
        else:
            non_null = None
            pct = None
            bucket = "unknown"
        completeness[str(name).upper()] = {
            "nonNullCount": non_null,
            "completenessPct": pct,
            "completenessBucket": bucket,
        }

    modified_at = datetime.fromtimestamp(input_path.stat().st_mtime, tz=UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    profile = {
        "panelRows": total_rows,
        "panelCols": len(names),
        "panelYears": {
            "min": year_min,
            "max": year_max,
            "count": (year_max - year_min + 1) if year_min is not None and year_max is not None else 0,
        },
        "panelModifiedAt": modified_at,
        "schemaHash": compute_schema_hash(input_path, fields, total_rows),
    }
    return profile, completeness


def build_presets(rows: list[dict]) -> list[dict]:
    by_varname = {row["varname"]: row["exportName"] for row in rows}

    def unique_exports(exports: list[str]) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        for export_name in exports:
            if export_name in seen:
                continue
            seen.add(export_name)
            out.append(export_name)
        return out

    def present(varnames: list[str]) -> list[str]:
        return unique_exports([by_varname[name] for name in varnames if name in by_varname])

    def by_groups(groups: set[str]) -> list[str]:
        return unique_exports([row["exportName"] for row in rows if row["componentGroup"] in groups])

    presets = [
        {
            "id": "core-baseline-controls",
            "label": "Core Baseline Controls",
            "description": "Common institution descriptors and baseline controls.",
            "variables": present([
                "INSTNM",
                "STABBR",
                "FIPS",
                "OBEREG",
                "SECTOR",
                "CONTROL",
                "ICLEVEL",
                "HLOFFER",
                "UGOFFER",
                "LOCALE",
                "OPENADMP",
                "FT_UG",
                "INSTSIZE",
                "CBSA",
                "COUNTYCD",
            ]),
        },
        {
            "id": "costs-price",
            "label": "Price / COA",
            "description": "Published tuition, fees, and living-cost variables.",
            "variables": by_groups({"Costs / Price"}),
        },
        {
            "id": "aid-packaging",
            "label": "Aid Packaging",
            "description": "Student financial aid variables for grants, loans, and aid recipients.",
            "variables": by_groups({"Student Financial Aid"}),
        },
        {
            "id": "admissions-funnel",
            "label": "Admissions Funnel",
            "description": "Admissions selectivity, applications, and test-score variables.",
            "variables": by_groups({"Admissions"}),
        },
        {
            "id": "enrollment-outcomes",
            "label": "Enrollment + Outcomes",
            "description": "Enrollment, student profile, and graduation/outcome measures.",
            "variables": by_groups({"Enrollment / Student Profile", "Graduation / Outcomes"}),
        },
        {
            "id": "finance",
            "label": "Finance",
            "description": "Finance and expenditure variables from the current panel schema.",
            "variables": by_groups({"Finance"}),
        },
    ]
    return [preset for preset in presets if preset["variables"]]


SIMILARITY_STOPWORDS = {
    "THE",
    "AND",
    "FOR",
    "WITH",
    "FROM",
    "THAT",
    "THIS",
    "WITHIN",
    "RATE",
    "TOTAL",
    "NUMBER",
    "STUDENT",
    "STUDENTS",
    "VARIABLE",
    "IPEDS",
    "DATA",
    "INSTITUTION",
    "INSTITUTIONS",
}


def similarity_tokens(*parts: str) -> set[str]:
    text = " ".join(str(part or "").upper() for part in parts)
    tokens = set(re.findall(r"[A-Z0-9]+", text))
    return {token for token in tokens if len(token) >= 3 and token not in SIMILARITY_STOPWORDS}


def varname_roots(name: str) -> set[str]:
    text = str(name or "").upper()
    roots = {text}
    trimmed = re.sub(r"\d+$", "", text)
    if trimmed:
        roots.add(trimmed)
    leading_letters = re.match(r"[A-Z]+", text)
    if leading_letters:
        roots.add(leading_letters.group(0))
    return {root for root in roots if len(root) >= 3}


def attach_related_variables(rows: list[dict]) -> None:
    enriched: list[dict] = []
    for row in rows:
        row["_sim_tokens"] = similarity_tokens(row["varname"], row["varTitle"], row["longDescription"], row["semanticFamily"])
        row["_roots"] = varname_roots(row["varname"])
        enriched.append(row)

    for row in enriched:
        scored: list[tuple[int, int, str]] = []
        for other in enriched:
            if other["exportName"] == row["exportName"]:
                continue
            score = 0
            if other["componentGroup"] == row["componentGroup"]:
                score += 20
            if other["semanticFamily"] == row["semanticFamily"]:
                score += 35
            if other["variableForm"] == row["variableForm"]:
                score += 8
            if row["primarySource"] and other["primarySource"] == row["primarySource"]:
                score += 6
            if row["_roots"] & other["_roots"]:
                score += 18
            overlap = len(row["_sim_tokens"] & other["_sim_tokens"])
            score += overlap * 3
            if score >= 28:
                scored.append((score, other["panelOrder"], other["exportName"]))
        scored.sort(key=lambda item: (-item[0], item[1], item[2]))
        row["relatedVariables"] = [export_name for _, _, export_name in scored[:6]]

    for row in enriched:
        row.pop("_sim_tokens", None)
        row.pop("_roots", None)


def build_payload(input_path: Path, dictionary_path: Path, title: str) -> dict:
    schema = pq.read_schema(input_path)
    generated_at = datetime.now(tz=UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    profile, completeness = build_panel_profile(input_path)
    dictionary_summary = build_dictionary_summary(dictionary_path)
    panel_year_count = int(profile["panelYears"]["count"])

    rows: list[dict] = []
    for idx, name in enumerate(schema.names):
        field = schema.field(name)
        name_upper = str(name).upper().strip()
        if name_upper in {"YEAR", "UNITID"}:
            continue
        meta = dictionary_summary.get(name_upper, {})
        title_text = meta.get("varTitle", "")
        description_text = meta.get("longDescription", "")
        component_group = infer_component_group(
            meta.get("primarySource", ""),
            name_upper,
            title_text,
            description_text,
        )
        panel_type = infer_panel_type(field.type)
        dictionary_dtype = meta.get("dictionaryDataType", "")
        variable_form = infer_variable_form(name_upper, title_text, panel_type, dictionary_dtype)
        semantic_family = infer_semantic_family(component_group, name_upper, title_text, description_text)
        completeness_meta = completeness.get(name_upper, {})
        year_min = meta.get("yearMin")
        year_max = meta.get("yearMax")
        year_count = int(meta.get("yearCount", 0))
        coverage_bucket = classify_coverage(year_min, year_max, year_count, panel_year_count)
        row = {
            "panelOrder": idx,
            "exportName": str(name),
            "varname": name_upper,
            "varTitle": title_text,
            "longDescription": description_text,
            "dictionaryDataType": dictionary_dtype,
            "primarySource": meta.get("primarySource", ""),
            "sourceCount": int(meta.get("sourceCount", 0)),
            "yearMin": year_min,
            "yearMax": year_max,
            "yearCount": year_count,
            "panelDataType": str(field.type),
            "panelType": panel_type,
            "componentGroup": component_group,
            "semanticFamily": semantic_family,
            "variableForm": variable_form,
            "coverageBucket": coverage_bucket,
            "coverageLabel": f"{year_min}-{year_max}" if year_min is not None and year_max is not None else "unknown years",
            "nonNullCount": completeness_meta.get("nonNullCount"),
            "completenessPct": completeness_meta.get("completenessPct"),
            "completenessBucket": completeness_meta.get("completenessBucket", "unknown"),
            "completenessLabel": (
                f"{completeness_meta['completenessPct']:.1f}% non-null"
                if completeness_meta.get("completenessPct") is not None
                else "null stats unavailable"
            ),
            "metadataMissing": not any(
                [
                    meta.get("varTitle", ""),
                    meta.get("longDescription", ""),
                    dictionary_dtype,
                    meta.get("primarySource", ""),
                ]
            ),
        }
        rows.append(row)

    attach_related_variables(rows)
    group_names = [group for group in GROUP_ORDER if any(row["componentGroup"] == group for row in rows)]
    return {
        "title": title,
        "panelName": input_path.name,
        "dictionaryName": dictionary_path.name,
        "schemaHash": profile["schemaHash"],
        "buildDate": generated_at,
        "panelModifiedAt": profile["panelModifiedAt"],
        "panelRows": profile["panelRows"],
        "panelCols": profile["panelCols"],
        "panelYears": profile["panelYears"],
        "totalVariables": len(rows),
        "groupNames": group_names,
        "presets": build_presets(rows),
        "variables": rows,
    }


def render_html(payload: dict) -> str:
    data_json = json.dumps(payload, ensure_ascii=True, separators=(",", ":")).replace("</", "<\\/")
    out = HTML_TEMPLATE.replace("__TITLE__", html.escape(str(payload["title"])))
    out = out.replace("__PANEL_NAME__", html.escape(str(payload["panelName"])))
    out = out.replace("__DATA__", data_json)
    return out


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    dictionary_path = Path(args.dictionary)
    output_path = Path(args.output)

    if not input_path.exists():
        raise SystemExit(f"Missing input panel: {input_path}")
    if not dictionary_path.exists():
        raise SystemExit(f"Missing dictionary lake: {dictionary_path}")

    payload = build_payload(input_path, dictionary_path, title=args.title)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_html(payload), encoding="utf-8")
    print(
        f"Wrote {output_path} vars={payload['totalVariables']:,} groups={len(payload['groupNames'])}",
        flush=True,
    )


if __name__ == "__main__":
    main()
