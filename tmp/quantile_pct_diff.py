from pathlib import Path
import math


def parse_rows(path):
    rows = {}
    for line in Path(path).read_text(errors="ignore").splitlines():
        if not line.startswith("|"):
            continue
        parts = [p.strip() for p in line.strip("|").split("|")]
        if len(parts) < 3:
            continue
        name = parts[0]
        if name in ("name", ""):
            continue
        rows[name] = parts[1:]
    return rows


brute = "/tmp/rs_curr_cmp_wide_brute_recheck.txt"
tdig = "/tmp/rs_curr_cmp_wide_tdigest.txt"
b = parse_rows(brute)
t = parse_rows(tdig)
common = sorted(set(b) & set(t))

exact_rows = 0
changed_rows = 0

cell_total = 0
cell_equal = 0
cell_changed = 0
pct_diffs = []

for key in common:
    bv = b[key]
    tv = t[key]
    if bv == tv:
        exact_rows += 1
    else:
        changed_rows += 1

    for x, y in zip(bv, tv):
        try:
            xf = float(x)
            yf = float(y)
        except Exception:
            continue

        cell_total += 1
        if xf == yf:
            cell_equal += 1
            continue

        cell_changed += 1
        denom = abs(xf)
        if denom == 0:
            if yf == 0:
                pct = 0.0
            else:
                continue
        else:
            pct = abs(yf - xf) / denom * 100.0

        if math.isfinite(pct):
            pct_diffs.append(pct)

row_total = len(common)
row_changed_pct = (changed_rows / row_total * 100.0) if row_total else float("nan")
row_exact_pct = (exact_rows / row_total * 100.0) if row_total else float("nan")
cell_changed_pct = (cell_changed / cell_total * 100.0) if cell_total else float("nan")
cell_exact_pct = (cell_equal / cell_total * 100.0) if cell_total else float("nan")

pct_sorted = sorted(pct_diffs)


def q(v):
    if not pct_sorted:
        return float("nan")
    idx = min(len(pct_sorted) - 1, max(0, int((len(pct_sorted) - 1) * v)))
    return pct_sorted[idx]


mean_pct = (sum(pct_sorted) / len(pct_sorted)) if pct_sorted else float("nan")
max_pct = pct_sorted[-1] if pct_sorted else float("nan")

print(f"row_total={row_total}")
print(f"row_exact={exact_rows}")
print(f"row_changed={changed_rows}")
print(f"row_exact_pct={row_exact_pct:.4f}")
print(f"row_changed_pct={row_changed_pct:.4f}")
print(f"cell_total={cell_total}")
print(f"cell_exact={cell_equal}")
print(f"cell_changed={cell_changed}")
print(f"cell_exact_pct={cell_exact_pct:.4f}")
print(f"cell_changed_pct={cell_changed_pct:.4f}")
print(f"pctdiff_count_nonzero_denom={len(pct_sorted)}")
print(f"pctdiff_mean={mean_pct:.4f}")
print(f"pctdiff_p50={q(0.5):.4f}")
print(f"pctdiff_p90={q(0.9):.4f}")
print(f"pctdiff_p95={q(0.95):.4f}")
print(f"pctdiff_p99={q(0.99):.4f}")
print(f"pctdiff_max={max_pct:.4f}")
