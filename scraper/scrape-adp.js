#!/usr/bin/env node
/**
 * scrape-adp.js — Underdog Fantasy ADP fetcher
 *
 * Fetches player and ADP data from public Underdog Stats API endpoints
 * (no login or cookies required) and merges them into the pipeline CSV.
 *
 *   GET /v1/slates/{slate}/players          — player roster (id, name, position, team)
 *   GET /v1/slates/{slate}/scoring_types/{scoring_type}/appearances
 *                                           — ADP, projected points, position rank
 *
 * ADP is PER-SLATE, and each Underdog tournament draws from its own slate, so
 * ADP differs by tournament (confirmed 2026-08-04: top of board identical,
 * mid/late diverges up to ~20 spots in the format-predicted direction — the
 * Eliminator drafts floor RBs earlier, Weekly Winners drafts ceilings earlier).
 * We fetch three slates:
 *   • Best Ball Mania (default) — also the slate the entire dog family shares
 *   • The Eliminator            — survivor / floor format
 *   • Weekly Winners            — weekly-lottery / ceiling format (Weekly Woofs too)
 * The BBM output is unchanged (data/adp.csv); the two alt formats write their
 * own CSVs so downstream can build per-tournament ADP anchors.
 *
 * Discovering slate ids: GET api.underdogfantasy.com/v2/lobby maps every
 * tournament title → slate_id (tournaments[].tournament_rounds[].slate_id,
 * weekly_winners[].slate_id). Re-check these each season if ADP looks stale.
 *
 * The output CSV matches the pipeline schema:
 *   id, firstName, lastName, adp, projectedPoints, positionRank,
 *   slotName, teamName, lineupStatus, byeWeek
 *
 * Outputs (per slate):
 *   • data/adp.csv                            (BBM — consumed by the build pipeline)
 *   • data/adp-eliminator.csv                 (The Eliminator)
 *   • data/adp-weekly.csv                     (Weekly Winners)
 *   • snapshots/{prefix}_YYYY-MM-DD.csv       (daily historical snapshot each)
 *
 * Usage:
 *   node scrape-adp.js                       # normal run (all slates)
 *   node scrape-adp.js --dry-run             # fetch only, don't write files
 *   node scrape-adp.js --output ./out.csv    # custom output path (primary slate only)
 *   node scrape-adp.js --rankings-id <uuid>  # override the PRIMARY (BBM) slate ID
 *   node scrape-adp.js --primary-only        # fetch only the BBM slate (legacy behavior)
 *
 * Optional env overrides:
 *   UNDERDOG_SLATE_ID                (primary / BBM slate)
 *   UNDERDOG_ELIMINATOR_SLATE_ID
 *   UNDERDOG_WEEKLY_SLATE_ID
 *   UNDERDOG_SCORING_TYPE_ID
 *   UNDERDOG_PRODUCT
 *   UNDERDOG_PRODUCT_EXPERIENCE_ID
 *   UNDERDOG_STATE_CONFIG_ID
 *   SNAPSHOT_TIME_ZONE               # defaults to America/New_York
 */

import { writeFileSync, mkdirSync, readFileSync, existsSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

// ── Paths ──────────────────────────────────────────────────────────
const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(__dirname, "..");
const DATA_DIR = resolve(ROOT, "data");
const ADP_CSV_PATH = resolve(DATA_DIR, "adp.csv");
const ADP_OVERRIDES_PATH = resolve(DATA_DIR, "adp-overrides.json");
const SNAPSHOTS_DIR = resolve(ROOT, "snapshots");
const SNAPSHOT_TIME_ZONE = process.env.SNAPSHOT_TIME_ZONE || "America/New_York";

// ── CLI flags ──────────────────────────────────────────────────────
const args = process.argv.slice(2);
const DRY_RUN = args.includes("--dry-run");
const PRIMARY_ONLY = args.includes("--primary-only");
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
const ELIMINATOR_SLATE_ID =
  process.env.UNDERDOG_ELIMINATOR_SLATE_ID || "0ee05b31-f904-4e6e-985b-42f290e3aa3a";
const WEEKLY_SLATE_ID =
  process.env.UNDERDOG_WEEKLY_SLATE_ID || "d9fd5f58-393f-400c-a010-3bf79b822b48";
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

// Slates to scrape. The first entry is the primary (BBM) — its output path is
// the pipeline-consumed data/adp.csv and honors --output. The player pool is
// fetched once from the primary slate and reused for all slates: player_id is
// stable across slates (verified), so appearances join cleanly by player_id.
const SLATES = [
  { key: "bbm",        label: "Best Ball Mania / dogs", slateId: SLATE_ID,            csvPath: ADP_CSV_PATH,                              snapshotPrefix: "adp" },
  { key: "eliminator", label: "The Eliminator",         slateId: ELIMINATOR_SLATE_ID, csvPath: resolve(DATA_DIR, "adp-eliminator.csv"),  snapshotPrefix: "adp-eliminator" },
  { key: "weekly",     label: "Weekly Winners",         slateId: WEEKLY_SLATE_ID,     csvPath: resolve(DATA_DIR, "adp-weekly.csv"),      snapshotPrefix: "adp-weekly" },
];
// --output overrides only the primary slate's path (single-file manual runs).
if (customOutput) SLATES[0].csvPath = resolve(customOutput);
const SLATES_TO_RUN = PRIMARY_ONLY ? SLATES.slice(0, 1) : SLATES;

const playersUrl = (slateId) =>
  `https://stats.underdogfantasy.com/v1/slates/${slateId}/players?${query.toString()}`;
const appearancesUrl = (slateId) =>
  `https://stats.underdogfantasy.com/v1/slates/${slateId}/scoring_types/${SCORING_TYPE}/appearances?${query.toString()}`;

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
  // position_id in player data is a UUID — skip it; it won't match POSITIONS.
  // NB: we deliberately do NOT fall back to projection.position_rank. Underdog
  // classifies fullbacks (Juszczyk = "RB102") and other non-fantasy roles in
  // position_rank, and the established board excludes them by keying off the
  // player's real position. Every slate's players are present in the shared
  // BBM player map (verified: zero orphans across BBM/Eliminator/Weekly), so a
  // rank fallback would rescue nothing but those intended exclusions.
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

/**
 * Load the manual ADP override map (data/adp-overrides.json).
 *
 * Applied on EVERY scrape run so the daily job can't revert it: Underdog reports
 * "-" (no market) for players it has pulled from the board, which lands as a null
 * ADP downstream — that breaks the extension's CLV math and freezes the data
 * visualizer's ADP-history line at the player's last real value. An override
 * forces a sentinel ADP (216 = one past the pool bottom, "effectively undrafted").
 *
 * Returns a normalized map { playerId: { adp:number, firstName?, lastName?,
 * position?, team? } }. Keys starting with "_" (e.g. "_note") are ignored, and a
 * bare-number value is accepted as shorthand for { adp: <number> }.
 */
function loadAdpOverrides() {
  if (!existsSync(ADP_OVERRIDES_PATH)) return {};
  let raw;
  try {
    raw = JSON.parse(readFileSync(ADP_OVERRIDES_PATH, "utf8"));
  } catch (err) {
    // A malformed override file must not silently drop overrides — fail loud.
    throw new Error(`Failed to parse ${ADP_OVERRIDES_PATH}: ${err.message}`);
  }
  const src = raw && typeof raw.overrides === "object" ? raw.overrides : raw;
  const out = {};
  for (const [id, val] of Object.entries(src || {})) {
    if (id.startsWith("_")) continue;
    const entry = typeof val === "number" ? { adp: val } : val;
    if (entry && typeof entry.adp === "number" && Number.isFinite(entry.adp)) {
      out[id] = entry;
    } else {
      console.warn(`[scraper] Ignoring override for ${id}: no finite numeric "adp"`);
    }
  }
  return out;
}

/** Merge a slate's appearances against the shared player/team maps → CSV rows. */
function mergeRows(appearances, playerMap, teamAbbrMap, overrides = {}) {
  const rows = [];
  const overriddenSeen = new Set();
  for (const app of appearances) {
    // appearances.player_id joins to players.id (stable across slates).
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
    // Resolve team abbreviation: prefer explicit name fields, then look up team_id
    // (from the player, or the appearance itself for players missing from the map)
    // in the teams array, then the hardcoded UUID map, then fall back to the id.
    const rawTeamId      = pick(player, "team_id", "teamId") || app.team_id || "";
    const teamName       = pick(player, "team_name", "teamName", "team") ||
                           (rawTeamId ? teamAbbrMap.get(String(rawTeamId)) || TEAM_UUID_TO_ABBREV[String(rawTeamId)] || rawTeamId : "");
    // A manual override forces the ADP (e.g. a player Underdog has pulled from the
    // board), replacing whatever "-"/value the API returned. Everything else on the
    // row stays live.
    const override       = overrides[playerId];
    const adp            = override ? override.adp : resolveAdp(app);
    if (override) overriddenSeen.add(playerId);
    const projectedPts   = resolveProjection(app);
    const positionRank   = pick(app, "position_rank", "positionRank", "rank") ||
                           (app.projection && app.projection.position_rank) || "";

    rows.push([rowId, firstName, lastName, adp, projectedPts, positionRank, position, teamName, "", ""]);
  }

  // Overridden players that dropped out of appearances entirely (Underdog fully
  // de-listed them) still need a sentinel row, or CLV math and the ADP-history
  // line would silently lose them again. Synthesize from the player pool, falling
  // back to the override file's own name/team/position fields.
  for (const [playerId, ov] of Object.entries(overrides)) {
    if (overriddenSeen.has(playerId)) continue;
    const player    = playerMap.get(playerId) || {};
    const firstName = pick(player, "first_name", "firstName") || ov.firstName || "";
    const lastName  = pick(player, "last_name", "lastName") || ov.lastName || "";
    const fullName  = `${firstName} ${lastName}`.trim();
    const position  = PLAYER_POSITION_OVERRIDES[fullName] || resolvePosition({}, player) || ov.position || "";
    if (!POSITIONS.has(position)) {
      console.warn(`[scraper] Override ${playerId} absent from appearances and has no resolvable position — skipping synthesized row`);
      continue;
    }
    const rawTeamId = pick(player, "team_id", "teamId") || "";
    const teamName  = pick(player, "team_name", "teamName", "team") ||
                      (rawTeamId ? teamAbbrMap.get(String(rawTeamId)) || TEAM_UUID_TO_ABBREV[String(rawTeamId)] || rawTeamId : "") ||
                      ov.team || "";
    rows.push([playerId, firstName, lastName, ov.adp, 0, "", position, teamName, "", ""]);
  }

  // Sort ascending by ADP; players with no ADP go last
  rows.sort((a, b) => {
    const aAdp = parseFloat(a[3]) || Infinity;
    const bAdp = parseFloat(b[3]) || Infinity;
    return aAdp - bAdp;
  });

  return rows;
}

/** Serialize rows to the pipeline CSV string. */
function rowsToCsv(rows) {
  return CSV_HEADER + "\n" + rows.map((r) => r.map(csvQuote).join(",")).join("\n") + "\n";
}

// ── Main ───────────────────────────────────────────────────────────

async function main() {
  console.log("[scraper] Fetching from Underdog Stats API (no auth required)…");
  console.log(`[scraper]   ${SLATES_TO_RUN.length} slate(s): ${SLATES_TO_RUN.map((s) => s.key).join(", ")}`);

  // Player pool + team map are fetched ONCE from the primary slate and reused
  // for every slate (player_id is stable across slates). The appearances call
  // is what differs per slate — it carries the slate-specific ADP.
  const playersBody = await fetchJson(playersUrl(SLATE_ID));
  const players = playersBody.players || playersBody.athletes || findArray(playersBody);

  if (!Array.isArray(players)) {
    throw new Error(`Players response is not an array (got ${typeof players}) — API shape may have changed`);
  }
  if (players.length === 0) {
    throw new Error("Players array is empty — API returned no player data");
  }

  // Build teamId → abbreviation map from the teams array returned alongside players.
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

  // Build player map: id → player object
  const playerMap = new Map();
  for (const p of players) {
    const id = pick(p, "id", "player_id", "playerId");
    if (id) playerMap.set(String(id), p);
  }
  console.log(`[scraper] Received ${players.length} players`);
  console.log("[scraper] Players sample keys:", Object.keys(players[0]).join(", "));

  // Manual ADP overrides (e.g. players Underdog has pulled from the board) —
  // re-applied every run to all slates so the daily scrape can't revert them.
  const adpOverrides = loadAdpOverrides();
  const overrideCount = Object.keys(adpOverrides).length;
  if (overrideCount > 0) {
    console.log(`[scraper] Loaded ${overrideCount} manual ADP override(s) from ${ADP_OVERRIDES_PATH}`);
  }

  const date = snapshotDateInTimeZone();
  let anyWritten = false;

  for (const slate of SLATES_TO_RUN) {
    console.log(`\n[scraper] === ${slate.label} (${slate.key}) ===`);
    console.log(`[scraper]   appearances → ${appearancesUrl(slate.slateId)}`);

    const appsBody = await fetchJson(appearancesUrl(slate.slateId));
    const appearances = appsBody.appearances || findArray(appsBody);
    if (!Array.isArray(appearances)) {
      throw new Error(`[${slate.key}] Appearances response is not an array (got ${typeof appearances}) — API shape may have changed`);
    }
    if (appearances.length === 0) {
      throw new Error(`[${slate.key}] Appearances array is empty — API returned no appearance data`);
    }
    console.log(`[scraper]   ${appearances.length} appearances; sample keys: ${Object.keys(appearances[0]).join(", ")}`);

    const rows = mergeRows(appearances, playerMap, teamAbbrMap, adpOverrides);
    if (rows.length < MIN_EXPECTED_ROWS) {
      throw new Error(
        `[${slate.key}] Only ${rows.length} player rows after merge — expected ${MIN_EXPECTED_ROWS}+. ` +
        "Check the sample keys logged above and adjust field resolution if needed."
      );
    }
    const priced = rows.filter((r) => r[3] !== "").length;
    console.log(`[scraper]   Merged ${rows.length} players (${priced} with ADP)`);

    const csv = rowsToCsv(rows);

    if (DRY_RUN) {
      console.log(`[scraper]   Dry run — skipping writes for ${slate.key}`);
      console.log(csv.split("\n").slice(0, 4).join("\n"));
      continue;
    }

    mkdirSync(DATA_DIR, { recursive: true });
    writeFileSync(slate.csvPath, csv, "utf8");
    console.log(`[scraper]   Wrote ${slate.csvPath}`);

    mkdirSync(SNAPSHOTS_DIR, { recursive: true });
    const snapshotPath = resolve(SNAPSHOTS_DIR, `${slate.snapshotPrefix}_${date}.csv`);
    writeFileSync(snapshotPath, csv, "utf8");
    console.log(`[scraper]   Wrote snapshot ${snapshotPath} (${SNAPSHOT_TIME_ZONE})`);
    anyWritten = true;
  }

  if (!DRY_RUN && anyWritten) {
    console.log(`\n[scraper] Done — ${SLATES_TO_RUN.length} slate(s) written for ${date}.`);
  }
}

main().catch((err) => {
  console.error("[scraper] Fatal:", err.message);
  process.exit(1);
});
