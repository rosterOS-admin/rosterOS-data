#!/usr/bin/env node
/**
 * build-adp-overlays.cjs — per-tournament ADP overlays for the extension.
 *
 * The scraper writes one CSV per slate (data/adp.csv = BBM, plus
 * data/adp-eliminator.csv and data/adp-weekly.csv). BBM is built into the
 * canonical adp.json (build-adp-json.cjs). The two format slates only differ
 * from BBM in the ADP column, so we ship them as compact overlays rather than
 * full boards:
 *
 *   adp-eliminator.json  { "<player_id>": <adp number>, ... }
 *   adp-weekly.json      { "<player_id>": <adp number>, ... }
 *
 * The extension keys these by the same player_id (id) as adp.json, and when a
 * non-default tournament archetype is detected it overrides each candidate's
 * ADP anchor with the format value. Players without a numeric ADP in a format
 * are omitted (the extension falls back to the BBM ADP for them).
 *
 * Usage: node scripts/build-adp-overlays.cjs
 */
"use strict";

const fs = require("fs");
const path = require("path");

const DATA_DIR = path.join(__dirname, "..", "data");
const ROOT = path.join(__dirname, "..");

const OVERLAYS = [
  { csv: "adp-eliminator.csv", json: "adp-eliminator.json", label: "Eliminator" },
  { csv: "adp-weekly.csv", json: "adp-weekly.json", label: "Weekly Winners" },
];

// ── Minimal RFC-4180 CSV row parser (matches build-adp-json.cjs) ──────
function parseCSVRow(line) {
  const out = [];
  let cur = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"') {
        if (line[i + 1] === '"') { cur += '"'; i++; }
        else inQuotes = false;
      } else cur += ch;
    } else if (ch === '"') {
      inQuotes = true;
    } else if (ch === ",") {
      out.push(cur); cur = "";
    } else {
      cur += ch;
    }
  }
  out.push(cur);
  return out;
}

function buildOverlay(csvPath) {
  const lines = fs.readFileSync(csvPath, "utf8").split("\n");
  // Header: "id","firstName","lastName","adp","projectedPoints",...
  const overlay = {};
  let priced = 0;
  for (let i = 1; i < lines.length; i++) {
    const row = parseCSVRow(lines[i]);
    if (row.length < 4) continue;
    const id = (row[0] || "").trim();
    const adpRaw = (row[3] || "").trim();
    if (!id || !adpRaw || adpRaw === "-") continue;
    const adp = parseFloat(adpRaw);
    if (Number.isNaN(adp)) continue;
    overlay[id] = adp;
    priced++;
  }
  return { overlay, priced };
}

let anyMissing = false;
for (const { csv, json, label } of OVERLAYS) {
  const csvPath = path.join(DATA_DIR, csv);
  if (!fs.existsSync(csvPath)) {
    console.warn(`[adp-overlays] SKIP ${label}: ${csv} not found (scraper hasn't produced it yet)`);
    anyMissing = true;
    continue;
  }
  const { overlay, priced } = buildOverlay(csvPath);
  if (priced === 0) {
    throw new Error(`[adp-overlays] ${label}: 0 priced players in ${csv} — refusing to write an empty overlay`);
  }
  const outPath = path.join(ROOT, json);
  fs.writeFileSync(outPath, JSON.stringify(overlay, null, 0) + "\n", "utf8");
  console.log(`[adp-overlays] Wrote ${json} (${priced} priced players) for ${label}`);
}

if (anyMissing) {
  console.warn("[adp-overlays] One or more format CSVs were missing — run the scraper first.");
}
