import io, os, re, subprocess, json, datetime, collections

ROOT = "/Users/sjwiesman/materialize"
CONTENT = "doc/user/content"
TODAY = datetime.date(2026, 8, 31)

# --- last-touched date per file, one git pass ---
log = subprocess.run(
    ["git", "log", "--pretty=format:@@%ad", "--date=short", "--name-only", "--", CONTENT],
    cwd=ROOT, capture_output=True, text=True).stdout
last = {}
cur = None
for line in log.splitlines():
    if line.startswith("@@"):
        cur = line[2:]
    elif line.strip() and cur and line not in last:
        last[line] = cur

rows = []
for dirpath, _, files in os.walk(os.path.join(ROOT, CONTENT)):
    for f in files:
        if not f.endswith(".md"):
            continue
        full = os.path.join(dirpath, f)
        rel = os.path.relpath(full, ROOT)
        s = io.open(full, encoding="utf-8", errors="replace").read()
        m = re.search(r'^title:\s*"?(.*?)"?\s*$', s, re.M)
        title = m.group(1) if m else ""
        body = re.sub(r'^---.*?^---', '', s, count=1, flags=re.S | re.M)
        words = len(body.split())
        sect = rel.split("/")[3] if rel.count("/") > 3 else "(root)"
        rows.append({
            "path": rel,
            "section": sect,
            "title": title,
            "words": words,
            "last": last.get(rel, ""),
            "sql_blocks": len(re.findall(r'```(?:mzsql|sql)', s)),
            "headless": "headless: true" in s,
        })

for r in rows:
    if r["last"]:
        y, mo, d = (int(x) for x in r["last"].split("-"))
        r["age_days"] = (TODAY - datetime.date(y, mo, d)).days
    else:
        r["age_days"] = None

json.dump(rows, io.open("/private/tmp/claude-501/-Users-sjwiesman-materialize-doc-user/e4f723b6-c2bf-4ad6-8cfa-01058a754028/scratchpad/inventory.json", "w"), indent=1)

# --- section rollup ---
agg = collections.defaultdict(lambda: {"n": 0, "w": 0, "sql": 0, "ages": []})
for r in rows:
    a = agg[r["section"]]
    a["n"] += 1
    a["w"] += r["words"]
    a["sql"] += r["sql_blocks"]
    if r["age_days"] is not None:
        a["ages"].append(r["age_days"])

print("%-28s %5s %8s %6s %8s %8s" % ("section", "files", "words", "sqlblk", "med_age", "max_age"))
for k in sorted(agg, key=lambda k: -agg[k]["w"]):
    a = agg[k]
    ages = sorted(a["ages"])
    med = ages[len(ages)//2] if ages else -1
    mx = ages[-1] if ages else -1
    print("%-28s %5d %8d %6d %8d %8d" % (k, a["n"], a["w"], a["sql"], med, mx))

stale = [r for r in rows if r["age_days"] is not None and r["age_days"] > 365
         and r["sql_blocks"] > 0 and not r["section"] in ("releases",)]
print("\nPages with SQL examples untouched >1yr: %d" % len(stale))
