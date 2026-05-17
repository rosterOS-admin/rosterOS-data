#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");

const csvPath = path.join(__dirname, "..", "data", "2026Schedule.csv");
const jsonPath = path.join(__dirname, "..", "schedule.json");

function parseCSVRow(line) {
  if (!line) return [];
  const fields = [];
  let i = 0;
  while (i < line.length) {
    if (line[i] === '"') {
      let value = "";
      i += 1;
      while (i < line.length) {
        if (line[i] === '"' && line[i + 1] === '"') {
          value += '"';
          i += 2;
        } else if (line[i] === '"') {
          i += 1;
          break;
        } else {
          value += line[i];
          i += 1;
        }
      }
      fields.push(value);
      if (line[i] === ',') i += 1;
    } else {
      const next = line.indexOf(',', i);
      if (next === -1) {
        fields.push(line.slice(i).trim());
        break;
      }
      fields.push(line.slice(i, next).trim());
      i = next + 1;
    }
  }
  return fields;
}

if (!fs.existsSync(csvPath)) {
  console.error(`Missing schedule CSV: ${csvPath}`);
  process.exit(1);
}

const csv = fs.readFileSync(csvPath, "utf8");
const lines = csv.split(/\r?\n/).filter(Boolean);
if (lines.length < 2) {
  console.error(`Schedule CSV must contain at least a header and one team row: ${csvPath}`);
  process.exit(1);
}

const header = parseCSVRow(lines[0]);
const weekNums = header.slice(1).map((value) => {
  const n = Number(value);
  if (!Number.isInteger(n)) {
    console.error(`Invalid week number in header: ${value}`);
    process.exit(1);
  }
  return n;
});

const weekSchedules = {};
for (let lineIndex = 1; lineIndex < lines.length; lineIndex += 1) {
  const row = parseCSVRow(lines[lineIndex]);
  if (row.length === 0) continue;
  const team = String(row[0] || "").trim();
  if (!team) continue;
  const scheduleEntries = weekNums.map((_, idx) => {
    const raw = String(row[idx + 1] || "").trim();
    return raw || "BYE";
  });
  weekSchedules[team] = scheduleEntries;
}

const output = {
  weekNums,
  weekSchedules,
};

fs.writeFileSync(jsonPath, JSON.stringify(output, null, 2) + "\n", "utf8");
console.log(`Wrote ${jsonPath} (${Object.keys(weekSchedules).length} teams, ${weekNums.length} weeks)`);
