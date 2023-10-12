const rawToEntityEntries = [
  ["&", "&amp;"],
  ["<", "&lt;"],
  [">", "&gt;"],
  ['"', "&quot;"],
  ["'", "&#39;"],
] as const;

const rawToEntity = new Map<string, string>(rawToEntityEntries);

const rawRe = new RegExp(`[${[...rawToEntity.keys()].join("")}]`, "g");

/**
 * Escapes text for safe interpolation into HTML text content and quoted attributes
 */
export function escapeHTML(str: string) {
  return str.replaceAll(rawRe, (m) => rawToEntity.get(m) ?? m);
}
