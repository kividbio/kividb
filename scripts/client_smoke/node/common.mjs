// What the Node runners share: run.py's contract (host port protocol cases.json),
// the cases, and one JSON line per reply.
import { readFileSync } from "node:fs";

export const [host, portArg, protocolArg, casesPath] = process.argv.slice(2);
export const port = Number(portArg);
export const protocol = Number(protocolArg);
export const cases = JSON.parse(readFileSync(casesPath, "utf8"));

// A reply as JSON: buffers as strings, maps as [key, value] pairs, sets sorted.
export function norm(v) {
  if (Buffer.isBuffer(v)) return v.toString("utf8");
  if (v instanceof Map) return [...v].map(([k, x]) => [norm(k), norm(x)]);
  if (v instanceof Set) return [...v].map(norm).sort((a, b) => (JSON.stringify(a) < JSON.stringify(b) ? -1 : 1));
  if (Array.isArray(v)) return v.map(norm);
  if (v !== null && typeof v === "object") return Object.entries(v).map(([k, x]) => [k, norm(x)]);
  if (typeof v === "bigint") return Number(v);
  if (typeof v === "number" && !Number.isFinite(v)) return String(v);
  return v === undefined ? null : v;
}

export async function runAll(client, version, send) {
  console.log(JSON.stringify({ client, version }));
  for (const c of cases) {
    let out;
    try {
      out = { reply: norm(await send(c.cmd)) };
    } catch (e) {
      if (!/^[A-Z]+/.test(e.message ?? "")) throw e;
      out = { error: e.message.split(" ")[0] };
    }
    console.log(JSON.stringify({ case: c.name, ...out }));
  }
}
