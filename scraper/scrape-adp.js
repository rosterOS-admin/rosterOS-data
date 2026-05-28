#!/usr/bin/env node
/**
 * scrape-adp.js — Underdog Fantasy ADP fetcher
 *
 * Fetches player and ADP data from two public Underdog Stats API endpoints
 * (no login or cookies required) and merges them into the pipeline CSV.
 *
 *   GET /v1/slates/{slate}/players          — player roster (id, name, position, team)
 *   GET /v1/slates/{slate}/scoring_types/{scoring_type}/appearances
 *                                           — ADP, projected points, position rank
 *
 * The output CSV matches the pipeline schema:
 *   id, firstName, lastName, adp, projectedPoints, positionRank,
 *   slotName, teamName, lineupStatus, byeWeek
 *
 * Outputs:
 *   • data/adp.csv                   (latest, consumed by the build pipeline)
 *   • snapshots/adp_YYYY-MM-DD.csv   (daily historical snapshot)
 *
 * Usage:
 *   node scrape-adp.js                       # normal run
 *   node scrape-adp.js --dry-run             # fetch only, don't write files
 *   node scrape-adp.js --output ./out.csv    # custom output path
 *   node scrape-adp.js --rankings-id <uuid>  # override slate ID
 *
 * Optional env overrides:
 *   UNDERDOG_SLATE_ID
 *   UNDERDOG_SCORING_TYPE_ID
 *   UNDERDOG_PRODUCT
 *   UNDERDOG_PRODUCT_EXPERIENCE_ID
 *   UNDERDOG_STATE_CONFIG_ID
 *   SNAPSHOT_TIME_ZONE               # defaults to America/New_York
 */

import { writeFileSync, mkdirSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

// ── Paths ──────────────────────────────────────────────────────────
const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(__dirname, "..");
const ADP_CSV_PATH = resolve(ROOT, "data", "adp.csv");
const SNAPSHOTS_DIR = resolve(ROOT, "snapshots");
const SNAPSHOT_TIME_ZONE = process.env.SNAPSHOT_TIME_ZONE || "America/New_York";

// ── CLI flags ──────────────────────────────────────────────────────
const args = process.argv.slice(2);
const DRY_RUN = args.includes("--dry-run");
const customOutput = (() => {
  const i = args.indexOf("--output");
  return i !== -1 ? args[i + 1] : null;
})();
const cliSlateId = (() => {
  const i = args.indexOf("--rankings-id");
  return i !== -1 ? args[i + 1] : null;
})();

const cliProductExperienceId = (() => {
  const i = args.indexOf("--product-experience-id");
  return i !== -1 ? args[i + 1] : null;
})();

const cliStateConfigId = (() => {
  const i = args.indexOf("--state-config-id");
  return i !== -1 ? args[i + 1] : null;
})();

// ── API constants ──────────────────────────────────────────────────
const SLATE_ID =
  cliSlateId || process.env.UNDERDOG_SLATE_ID || "a9c04e81-1ace-4b16-a31d-4c725a47f16f";
const SCORING_TYPE =
  process.env.UNDERDOG_SCORING_TYPE_ID || "ccf300b0-9197-5951-bd96-cba84ad71e86";
const PRODUCT = process.env.UNDERDOG_PRODUCT || "fantasy";
const PRODUCT_EXPERIENCE_ID =
  cliProductExperienceId ||
  process.env.UNDERDOG_PRODUCT_EXPERIENCE_ID ||
  "018e1234-5678-9abc-def0-123456789002";
const STATE_CONFIG_ID =
  cliStateConfigId || process.env.UNDERDOG_STATE_CONFIG_ID || "48c035e5-a055-4413-81c3-51b7f5928efa";

const query = new URLSearchParams({
  product: PRODUCT,
  product_experience_id: PRODUCT_EXPERIENCE_ID,
  state_config_id: STATE_CONFIG_ID,
});

const PLAYERS_URL = `https://stats.underdogfantasy.com/v1/slates/${SLATE_ID}/players?${query.toString()}`;
const APPEARANCES_URL =
  `https://stats.underdogfantasy.com/v1/slates/${SLATE_ID}/scoring_types/${SCORING_TYPE}/appearances?${query.toString()}`;

const USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36";

// ── Schema ─────────────────────────────────────────────────────────
const CSV_HEADER = '"id","firstName","lastName","adp","projectedPoints","positionRank","slotName","teamName","lineupStatus","byeWeek"';
const POSITIONS  = new Set(["QB", "RB", "WR", "TE"]);
const MIN_EXPECTED_ROWS = 50;

// ── Underdog team UUID → abbreviation fallback ────────────────────
// The players endpoint returns team_id as a UUID.  The teams array
// returned alongside may carry the abbreviation, but when it doesn't
// (or uses a different key) this hardcoded map provides a reliable
// fallback.  UUIDs are stable Underdog identifiers for NFL teams.
const TEAM_UUID_TO_ABBREV = {
  "8459772c-695a-5890-afa1-7ec38da17201": "ARI",
  "8699a914-7d44-5a36-b7a6-2063d3ea761f": "ATL",
  "01bfe9d5-f671-57ed-aa60-249fcca9267c": "BAL",
  "0719b253-43db-532e-8b5c-6c12c6c3f951": "BUF",
  "17f3bc4a-e2d6-5dc5-a554-00549ff0139f": "CAR",
  "e8a4678a-ccdb-5f35-be13-aef6e59a5680": "CHI",
  "ab235cf0-a041-5d36-8241-90a90f0dcb5e": "CIN",
  "338ae7df-02ad-5a94-9767-60f511bd55e1": "CLE",
  "26e67e06-664e-50a6-ad7b-c102705fde8b": "DAL",
  "7b3b21be-a209-5dee-a3f7-3fae61f52a1e": "DEN",
  "a8980eb6-327f-5bc5-abf9-de1e944b566d": "DET",
  "a2fadaac-562f-5a7e-bd63-6d88d90c1ac4": "GB",
  "4ab08caa-b79d-598e-82ef-0906a60d2a89": "HOU",
  "f11f1cf1-9933-5203-8181-95020ee64399": "IND",
  "b49b653c-dc7c-516e-b1c2-da1b30d1b1a6": "JAX",
  "a6f458f4-5078-56e4-a839-af96f1191314": "KC",
  "c7b497d4-18b6-522b-abaa-e5c3d24bc021": "LAC",
  "d150534e-6a05-587b-b9e3-50ef86602e20": "LAR",
  "5ce78c37-b02c-52da-9e4f-fcd65b6ccb76": "LV",
  "9153e0a9-da83-5246-ba43-417537f1bcce": "MIA",
  "5af1e185-627e-5345-851d-3c65c5b66614": "MIN",
  "ef876de9-81ac-5e60-af6e-0ca1700f3a51": "NE",
  "530518ed-91db-57c4-9077-27cc0dd9a293": "NO",
  "d40a4380-49a6-5b5f-ab7d-f5393787ed12": "NYG",
  "516ded41-e882-5175-9a00-d0b4c028eb74": "NYJ",
  "de7219e5-92f1-5989-804a-68479055ba42": "PHI",
  "ecc8eb1b-f714-57a6-bcf3-b183dd6c12a8": "PIT",
  "31631011-5902-52f6-ba01-c4c8d8eb3fd9": "SEA",
  "7161e62b-de20-56e2-a300-0dc23637faaa": "SF",
  "1a20f1da-c502-5224-9c4a-bc363174cd21": "TB",
  "f96aa8db-21c2-5b86-b49d-7e64b4eda61d": "TEN",
  "a6d8dc19-daaf-5798-a8f2-df7f9fc9eecd": "WAS",
};

// Map full position names (from position_name / position_display_name) → abbreviation
const POSITION_FULL_TO_ABBR = {
  "QUARTERBACK":   "QB",
  "RUNNING BACK":  "RB",
  "WIDE RECEIVER": "WR",
  "TIGHT END":     "TE",
};

// Players listed at a non-fantasy position (e.g. CB) who should be treated
// as a specific fantasy position for scoring purposes.
const PLAYER_POSITION_OVERRIDES = {
  "Travis Hunter": "WR",
};

/** Resolve a human-readable position abbreviation from player + appearance objects. */
function resolvePosition(app, player) {
  // Appearances rarely carry position; try anyway
  for (const key of ["position_display_name", "position_name", "position", "slot_name", "slotName"]) {
    const raw = app[key] || player[key];
    if (!raw) continue;
    const upper = String(raw).toUpperCase();
    if (POSITIONS.has(upper)) return upper;
    if (POSITION_FULL_TO_ABBR[upper]) return POSITION_FULL_TO_ABBR[upper];
  }
  // position_id in player data is a UUID — skip it; it won't match POSITIONS
  return "";
}

// Ordered list of flat field names to try for projected fantasy points.
// The appearances endpoint uses "points" for total projected season points.
const PROJECTION_FLAT_KEYS = ["projected_points", "projectedPoints", "fpts", "fantasy_points", "points", "avg_weekly_points"];

// Field names to check inside a nested projection object.
const PROJECTION_NESTED_KEYS = ["fantasy_points", "value", "points", "projected_points", "fpts"];

/**
 * Extract the ADP from an appearance record.
 * The Underdog appearances endpoint nests ADP inside the `projection` object
 * as `projection.adp` (e.g. { adp: "1.4", points: 294.9, ... }).
 * Falls back to top-level fields for other API shapes.
 */
function resolveAdp(app) {
  const proj = app.projection;
  if (proj && typeof proj === "object") {
    const nested = proj.adp ?? proj.average_draft_position ?? proj.avg_pick;
    if (nested !== undefined && nested !== null && nested !== "") return nested;
  }
  // Top-level fallback for other API shapes
  return pick(app, "average_draft_position", "adp", "avg_pick", "pick_number", "sort_by") || "";
}

/**
 * Extract projected fantasy points from an appearance record.
 * The `projection` field may be a plain number, string, or nested object.
 */
function resolveProjection(app) {
  const raw = app.projection;
  if (raw === null || raw === undefined) {
    // No nested projection — try flat fields directly on the appearance.
    return pick(app, ...PROJECTION_FLAT_KEYS) || "";
  }
  if (typeof raw === "number") return raw;
  if (typeof raw === "string" && raw !== "") return raw;
  if (typeof raw === "object") {
    // Common nested shapes: { fantasy_points, value, points, projected_points }
    for (const k of PROJECTION_NESTED_KEYS) {
      if (raw[k] !== undefined && raw[k] !== null) return raw[k];
    }
  }
  // Fallback: flat fields on the appearance
  return pick(app, ...PROJECTION_FLAT_KEYS) || "";
}

// ── Helpers ────────────────────────────────────────────────────────

/** Quote a value for CSV output (RFC 4180). */
function csvQuote(val) {
  if (val === null || val === undefined || val === "") return "";
  const s = String(val);
  if (s.includes('"')) return `"${s.replace(/"/g, '""')}"`;
  return `"${s}"`;
}

/**
 * Return the first non-empty value found under any of the given keys.
 * Checks both the object itself and one level of nesting (player / appearance).
 */
function pick(obj, ...keys) {
  for (const k of keys) {
    if (obj[k] !== undefined && obj[k] !== null && obj[k] !== "") return obj[k];
  }
  for (const nested of ["player", "appearance"]) {
    if (obj[nested] && typeof obj[nested] === "object") {
      for (const k of keys) {
        if (obj[nested][k] !== undefined && obj[nested][k] !== null && obj[nested][k] !== "") {
          return obj[nested][k];
        }
      }
    }
  }
  return "";
}

/**
 * Walk a JSON value and return the largest top-level array found,
 * searching one level deep into object values.
 */
function findArray(obj) {
  if (Array.isArray(obj)) return obj;
  if (!obj || typeof obj !== "object") return [];
  let best = [];
  for (const val of Object.values(obj)) {
    if (Array.isArray(val) && val.length > best.length) best = val;
  }
  return best;
}

function snapshotDateInTimeZone(now = new Date(), timeZone = SNAPSHOT_TIME_ZONE) {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(now);

  const byType = Object.fromEntries(
    parts
      .filter((p) => p.type !== "literal")
      .map((p) => [p.type, p.value])
  );

  return `${byType.year}-${byType.month}-${byType.day}`;
}

/** Fetch a URL and return the parsed JSON body with retry and timeout. */
async function fetchJson(url, { retries = 3, timeoutMs = 30_000 } = {}) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const res = await fetch(url, {
        headers: {
          Accept: "application/json",
          "User-Agent": USER_AGENT,
        },
        signal: controller.signal,
      });
      clearTimeout(timer);
      if (!res.ok) {
        throw new Error(`HTTP ${res.status} ${res.statusText} — ${url}`);
      }
      return res.json();
    } catch (err) {
      clearTimeout(timer);
      if (attempt >= retries) throw err;
      const delay = 1000 * 2 ** (attempt - 1); // 1s, 2s, 4s
      console.warn(`[scraper] Attempt ${attempt}/${retries} failed for ${url}: ${err.message}. Retrying in ${delay}ms…`);
      await new Promise((r) => setTimeout(r, delay));
    }
  }
}

// ── Main ───────────────────────────────────────────────────────────

async function main() {
  console.log("[scraper] Fetching from Underdog Stats API (no auth required)…");
  console.log(`[scraper]   players     → ${PLAYERS_URL}`);
  console.log(`[scraper]   appearances → ${APPEARANCES_URL}`);

  const [playersBody, appsBody] = await Promise.all([
    fetchJson(PLAYERS_URL),
    fetchJson(APPEARANCES_URL),
  ]);

  // Prefer explicitly-named arrays over the largest-array heuristic.
  // The appearances endpoint returns { appearances: [...], players: [...] };
  // findArray() may pick the wrong (larger) array if we don't name it explicitly.
  const players     = playersBody.players || playersBody.athletes || findArray(playersBody);
  const appearances = appsBody.appearances || findArray(appsBody);

  if (!Array.isArray(players)) {
    throw new Error(`Players response is not an array (got ${typeof players}) — API shape may have changed`);
  }
  if (players.length === 0) {
    throw new Error("Players array is empty — API returned no player data");
  }
  if (!Array.isArray(appearances)) {
    throw new Error(`Appearances response is not an array (got ${typeof appearances}) — API shape may have changed`);
  }
  if (appearances.length === 0) {
    throw new Error("Appearances array is empty — API returned no appearance data");
  }

  // Build teamId → abbreviation map from the teams array returned alongside players.
  // Common shapes: { id, abbreviation } or { id, abbr } or { id, short_name }.
  const teamAbbrMap = new Map();
  const teamsArray = playersBody.teams || playersBody.nfl_teams || [];
  for (const t of teamsArray) {
    const id = t.id;
    const abbr = t.abbreviation || t.abbr || t.short_name || t.name || "";
    if (id && abbr) teamAbbrMap.set(String(id), String(abbr));
  }
  if (teamAbbrMap.size > 0) {
    console.log(`[scraper] Built team map with ${teamAbbrMap.size} teams`);
  }

  console.log(`[scraper] Received ${players.length} players, ${appearances.length} appearances`);

  if (players.length > 0) {
    console.log("[scraper] Players sample keys:", Object.keys(players[0]).join(", "));
  }
  if (appearances.length > 0) {
    console.log("[scraper] Appearances sample keys:", Object.keys(appearances[0]).join(", "));
  }

  // Build player map: id → player object
  const playerMap = new Map();
  for (const p of players) {
    const id = pick(p, "id", "player_id", "playerId");
    if (id) playerMap.set(String(id), p);
  }

  // Merge appearances with player data
  const rows = [];
  for (const app of appearances) {
    // appearances.player_id joins to players.id
    // appearances.id may be an integer rank (1, 2, …) — use player_id as the canonical row UUID
    const playerId = String(app.player_id || app.playerId || "");
    if (!playerId) continue;

    // Use the player UUID as the row id so downstream consumers can match by player
    const rowId = playerId;

    const player = playerMap.get(playerId) || {};

    const firstName      = pick(player, "first_name", "firstName");
    const lastName       = pick(player, "last_name", "lastName");
    const fullName       = `${firstName} ${lastName}`.trim();
    const position       = PLAYER_POSITION_OVERRIDES[fullName] || resolvePosition(app, player);
    if (!POSITIONS.has(position)) continue;
    // Resolve team abbreviation: prefer explicit name fields, then look up team_id in the
    // teams array returned by the players endpoint, then try the hardcoded UUID map,
    // then fall back to team_id as-is.
    const rawTeamId      = pick(player, "team_id", "teamId");
    const teamName       = pick(player, "team_name", "teamName", "team") ||
                           (rawTeamId ? teamAbbrMap.get(String(rawTeamId)) || TEAM_UUID_TO_ABBREV[String(rawTeamId)] || rawTeamId : "");
    const adp            = resolveAdp(app);
    const projectedPts   = resolveProjection(app);
    const positionRank   = pick(app, "position_rank", "positionRank", "rank");

    rows.push([rowId, firstName, lastName, adp, projectedPts, positionRank, position, teamName, "", ""]);
  }

  // Sort ascending by ADP; players with no ADP go last
  rows.sort((a, b) => {
    const aAdp = parseFloat(a[3]) || Infinity;
    const bAdp = parseFloat(b[3]) || Infinity;
    return aAdp - bAdp;
  });

  if (rows.length < MIN_EXPECTED_ROWS) {
    throw new Error(
      `Only ${rows.length} player rows after merge — expected ${MIN_EXPECTED_ROWS}+. ` +
      "Check the sample keys logged above and adjust field resolution if needed."
    );
  }

  const csv =
    CSV_HEADER + "\n" +
    rows.map((r) => r.map(csvQuote).join(",")).join("\n") + "\n";

  console.log(`[scraper] Merged ${rows.length} players`);

  if (DRY_RUN) {
    console.log("[scraper] Dry run — skipping file writes");
    console.log(csv.split("\n").slice(0, 6).join("\n"));
    return;
  }

  const outPath = customOutput || ADP_CSV_PATH;
  writeFileSync(outPath, csv, "utf8");
  console.log(`[scraper] Wrote ${outPath}`);

  mkdirSync(SNAPSHOTS_DIR, { recursive: true });
  const date = snapshotDateInTimeZone();
  const snapshotPath = resolve(SNAPSHOTS_DIR, `adp_${date}.csv`);
  writeFileSync(snapshotPath, csv, "utf8");
  console.log(`[scraper] Wrote snapshot ${snapshotPath} (${SNAPSHOT_TIME_ZONE})`);
}

main().catch((err) => {
  console.error("[scraper] Fatal:", err.message);
  process.exit(1);
});
