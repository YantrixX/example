Below is a **working pattern** I’ve used for exactly this kind of problem: read *(kml_filename, clue)* from a spreadsheet, **walk the full KML hierarchy**, score “best match” against the clue (including **multi-word hierarchical matching**), optionally filter by **colour (e.g., “blue area”)**, then **export the matched layer/features to a new KML** and **write the new KML name back to the spreadsheet**.

It uses:

* **fastkml** to reliably traverse nested folders/placemarks + styles/colours
* **geopandas/fiona** for output KML writing (simple + robust)

---

## Install deps

```bash
pip install pandas openpyxl geopandas fiona shapely fastkml lxml
```

> Notes:
>
> * `geopandas` will pull `fiona`/`shapely`. On macOS you may prefer conda/mamba if wheels fight.
> * Writing KML via `GeoDataFrame.to_file(..., driver="KML")` relies on GDAL’s KML driver.

---

## Spreadsheet format

Example Excel (or CSV) columns:

* `kml_file`  (path or filename)
* `clue`      (your matching text)
* script will create/update: `matched_kml`

---

## Python script

Save as `match_kml_layers.py`

```python
import os
import re
import math
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import geopandas as gpd
from shapely.geometry import shape

from fastkml import kml
from lxml import etree


# ----------------------------
# Config you might tweak
# ----------------------------
STOPWORDS = {
    "area", "region", "polygon", "poly", "boundary", "layer", "zone", "the", "a", "an", "of", "and", "to", "in"
}
COLOUR_WORDS = {"blue", "red", "green", "yellow", "orange", "purple", "pink", "black", "white", "grey", "gray"}

# If clue has "blue area", we treat "blue" as a colour filter, and ignore "area" as a stopword.
# You can expand this mapping if your KMLs have specific style names you can detect.
COLOUR_TARGETS = {
    "blue": (0, 0, 255),
    "red": (255, 0, 0),
    "green": (0, 255, 0),
    "yellow": (255, 255, 0),
    "orange": (255, 165, 0),
    "purple": (128, 0, 128),
    "pink": (255, 105, 180),
    "black": (0, 0, 0),
    "white": (255, 255, 255),
    "grey": (128, 128, 128),
    "gray": (128, 128, 128),
}


@dataclass
class PlacemarkRecord:
    path: str                 # Folder hierarchy path (e.g., "Top/Child/Subchild")
    name: str
    description: str
    geom: Any                 # shapely geometry
    style_colour_rgb: Optional[Tuple[int, int, int]]  # (r,g,b) if detected


# ----------------------------
# Helpers: text + scoring
# ----------------------------
def normalise_text(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[_\-]+", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s

def tokenise_clue(clue: str) -> List[str]:
    clue = normalise_text(clue)
    tokens = re.findall(r"[a-z0-9]+", clue)
    tokens = [t for t in tokens if t not in STOPWORDS]
    return tokens

def extract_colour_word(tokens: List[str]) -> Optional[str]:
    for t in tokens:
        if t in COLOUR_WORDS:
            return t
    return None

def remove_colour_tokens(tokens: List[str]) -> List[str]:
    return [t for t in tokens if t not in COLOUR_WORDS]

def ngrams(tokens: List[str], n: int) -> List[str]:
    return [" ".join(tokens[i:i+n]) for i in range(0, max(0, len(tokens)-n+1))]

def text_contains_phrase(haystack: str, phrase: str) -> bool:
    return phrase in haystack

def cosineish_bonus(x: int) -> float:
    # mild non-linear bonus for longer phrase matches
    return 1.0 + math.log1p(max(0, x))

def score_path_match(path_segments: List[str], tokens: List[str]) -> float:
    """
    Multi-word hierarchical matching:
      - try to match longest consecutive phrases to earlier (higher) segments first,
        then remaining words to later (lower) segments.
    We do a greedy "longest phrase" walk.
    """
    if not tokens:
        return 0.0

    segs = [normalise_text(s) for s in path_segments if s]
    if not segs:
        return 0.0

    score = 0.0
    i = 0
    seg_index = 0

    while i < len(tokens) and seg_index < len(segs):
        best_n = 0
        best_seg = None

        # try longer phrases first (up to 5 words)
        for n in range(min(5, len(tokens)-i), 0, -1):
            phrase = " ".join(tokens[i:i+n])
            # search from current segment onward to respect hierarchy
            for j in range(seg_index, len(segs)):
                if phrase in segs[j]:
                    best_n = n
                    best_seg = j
                    break
            if best_n:
                break

        if best_n and best_seg is not None:
            # favour higher segments: earlier = better
            hierarchy_weight = 1.2 if best_seg == seg_index else 1.0
            score += hierarchy_weight * (best_n * 3.0) * cosineish_bonus(best_n)
            i += best_n
            seg_index = best_seg + 1
        else:
            # no match for this token in remaining segments; small penalty and advance
            score -= 0.2
            i += 1

    return score

def score_record(rec: PlacemarkRecord, clue_tokens: List[str]) -> float:
    """
    Overall scoring against:
      - folder path segments (hierarchical)
      - placemark name/description
    """
    path_segments = rec.path.split("/") if rec.path else []
    path_text = normalise_text(rec.path)
    name_text = normalise_text(rec.name)
    desc_text = normalise_text(rec.description)

    base = 0.0

    # Hierarchical scoring
    base += score_path_match(path_segments, clue_tokens)

    # Extra boosts if phrases appear directly in name/desc
    # try 3-grams then 2-grams then 1-grams
    for n, w in [(3, 2.0), (2, 1.4), (1, 1.0)]:
        for phrase in ngrams(clue_tokens, n):
            if phrase and (phrase in name_text):
                base += w * (n * 2.5)
            if phrase and (phrase in desc_text):
                base += (w * 0.8) * (n * 2.0)

    return base

def colour_distance(a: Tuple[int,int,int], b: Tuple[int,int,int]) -> float:
    return math.sqrt(sum((a[i]-b[i])**2 for i in range(3)))

def colour_matches(rec_rgb: Optional[Tuple[int,int,int]], target_rgb: Tuple[int,int,int]) -> bool:
    if rec_rgb is None:
        return False
    # allow a bit of tolerance because styles vary (stroke vs fill etc.)
    return colour_distance(rec_rgb, target_rgb) <= 90.0


# ----------------------------
# KML parsing (fastkml)
# ----------------------------
def parse_kml_file(kml_path: str) -> Tuple[List[PlacemarkRecord], Dict[str, Tuple[int,int,int]]]:
    """
    Returns:
      - placemark records (with full folder path)
      - style colour lookup (styleId -> rgb)
    """
    with open(kml_path, "rb") as f:
        data = f.read()

    k = kml.KML()
    k.from_string(data)

    # Extract style colours if possible (fastkml keeps Style objects, but can be inconsistent
    # across KML exports; we also do a fallback XML parse).
    style_map = extract_style_colours_fallback_xml(data)

    records: List[PlacemarkRecord] = []
    for doc in k.features():
        walk_features(doc, parent_path="", records=records, style_map=style_map)

    return records, style_map

def walk_features(feature: Any, parent_path: str, records: List[PlacemarkRecord], style_map: Dict[str, Tuple[int,int,int]]):
    """
    Recursively walk Document/Folder/Placemark features.
    """
    name = getattr(feature, "name", None) or ""
    this_path = parent_path
    if name:
        this_path = f"{parent_path}/{name}".strip("/") if parent_path else name

    # If feature is a Placemark with geometry
    geom = getattr(feature, "geometry", None)
    if geom is not None:
        desc = getattr(feature, "description", None) or ""
        pm_name = name or ""
        style_url = getattr(feature, "styleUrl", None) or ""
        style_colour = resolve_style_colour(style_url, style_map)
        records.append(
            PlacemarkRecord(
                path=parent_path,     # parent path acts as "layer" container
                name=pm_name,
                description=desc,
                geom=geom,
                style_colour_rgb=style_colour
            )
        )

    # Recurse into children
    for child in getattr(feature, "features", lambda: [])():
        walk_features(child, parent_path=this_path, records=records, style_map=style_map)

def resolve_style_colour(style_url: str, style_map: Dict[str, Tuple[int,int,int]]) -> Optional[Tuple[int,int,int]]:
    """
    styleUrl often looks like "#styleId". We map it to rgb if known.
    """
    if not style_url:
        return None
    sid = style_url.strip()
    if sid.startswith("#"):
        sid = sid[1:]
    return style_map.get(sid)

def kml_aabbggrr_to_rgb(hexstr: str) -> Optional[Tuple[int,int,int]]:
    """
    KML uses aabbggrr (alpha, blue, green, red) hex.
    Example: blue with full alpha = 'ffff0000' -> bb=ff, gg=00, rr=00 => (0,0,255)
    """
    if not hexstr:
        return None
    s = hexstr.strip().lower()
    s = s.replace("#", "")
    if not re.fullmatch(r"[0-9a-f]{8}", s):
        return None
    aa = int(s[0:2], 16)
    bb = int(s[2:4], 16)
    gg = int(s[4:6], 16)
    rr = int(s[6:8], 16)
    # ignore alpha for matching
    return (rr, gg, bb)

def extract_style_colours_fallback_xml(kml_bytes: bytes) -> Dict[str, Tuple[int,int,int]]:
    """
    Fallback extraction via XML:
      <Style id="..."><PolyStyle><color>...</color></PolyStyle> ...
      <LineStyle><color>...</color></LineStyle>
    Prefer PolyStyle colour if present, else LineStyle.
    """
    ns = {
        "kml": "http://www.opengis.net/kml/2.2"
    }
    root = etree.fromstring(kml_bytes)
    out: Dict[str, Tuple[int,int,int]] = {}

    for style in root.findall(".//kml:Style", namespaces=ns):
        sid = style.get("id")
        if not sid:
            continue

        poly_col = style.find(".//kml:PolyStyle/kml:color", namespaces=ns)
        line_col = style.find(".//kml:LineStyle/kml:color", namespaces=ns)

        rgb = None
        if poly_col is not None and poly_col.text:
            rgb = kml_aabbggrr_to_rgb(poly_col.text)
        if rgb is None and line_col is not None and line_col.text:
            rgb = kml_aabbggrr_to_rgb(line_col.text)

        if rgb is not None:
            out[sid] = rgb

    return out


# ----------------------------
# Matching + export
# ----------------------------
def choose_best_match(records: List[PlacemarkRecord], clue: str) -> Tuple[Optional[str], List[PlacemarkRecord]]:
    """
    Returns:
      - matched container path (folder path) OR None
      - list of matched placemarks to export
    Rules:
      - If only one "layer" (container path) exists, take it without matching.
      - Else score candidates with hierarchical + name/desc.
      - If clue includes a colour word (e.g. "blue"), prefer those placemarks; if none match colour, fallback.
    """
    if not records:
        return None, []

    # Determine distinct container paths (treat parent folder path as "layer")
    paths = sorted({r.path for r in records})
    if len(paths) == 1:
        matched_path = paths[0]
        matched = [r for r in records if r.path == matched_path]
        return matched_path, matched

    tokens = tokenise_clue(clue)
    colour_word = extract_colour_word(tokens)
    tokens_wo_colour = remove_colour_tokens(tokens)

    # If clue is basically just a colour + generic word, we still want to match by colour.
    target_rgb = COLOUR_TARGETS.get(colour_word) if colour_word else None

    scored: List[Tuple[float, PlacemarkRecord]] = []
    for r in records:
        s = score_record(r, tokens_wo_colour)
        # colour preference (not mandatory unless user wants it)
        if target_rgb is not None:
            if colour_matches(r.style_colour_rgb, target_rgb):
                s += 6.0
            else:
                s -= 1.0
        scored.append((s, r))

    scored.sort(key=lambda x: x[0], reverse=True)
    best_score, best_rec = scored[0]

    # Choose the best container path: we export all placemarks under that container path
    best_path = best_rec.path

    # If clue includes a colour, and there are placemarks of the same container that match colour, filter to them.
    matched = [r for r in records if r.path == best_path]
    if target_rgb is not None:
        colour_matched = [r for r in matched if colour_matches(r.style_colour_rgb, target_rgb)]
        if colour_matched:
            matched = colour_matched

    return best_path, matched

def export_to_kml(placemarks: List[PlacemarkRecord], out_kml_path: str):
    """
    Export matched placemarks to a KML using GeoPandas.
    """
    if not placemarks:
        raise ValueError("No placemarks to export")

    rows = []
    for pm in placemarks:
        # fastkml geometry is shapely already (most of the time),
        # but ensure it is a shapely geometry by passing through shape() if it’s geo-interface.
        geom = pm.geom
        try:
            # if it's already a shapely geometry, it will have geom_type
            _ = geom.geom_type
            shp = geom
        except Exception:
            shp = shape(geom)

        rows.append({
            "Name": pm.name or "matched",
            "Description": pm.description or "",
            "geometry": shp
        })

    gdf = gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:4326")

    os.makedirs(os.path.dirname(out_kml_path), exist_ok=True)
    # Some GDAL builds need the layer name param; GeoPandas handles it.
    gdf.to_file(out_kml_path, driver="KML")


# ----------------------------
# Main driver: read spreadsheet, process each row
# ----------------------------
def process_spreadsheet(
    spreadsheet_path: str,
    kml_base_dir: str,
    out_dir: str,
    kml_col: str = "kml_file",
    clue_col: str = "clue",
    out_col: str = "matched_kml",
):
    df = pd.read_excel(spreadsheet_path) if spreadsheet_path.lower().endswith((".xlsx", ".xls")) else pd.read_csv(spreadsheet_path)
    if out_col not in df.columns:
        df[out_col] = ""

    for idx, row in df.iterrows():
        kml_name = str(row.get(kml_col, "")).strip()
        clue = str(row.get(clue_col, "")).strip()

        if not kml_name or kml_name.lower() == "nan":
            continue

        kml_path = kml_name if os.path.isabs(kml_name) else os.path.join(kml_base_dir, kml_name)
        if not os.path.exists(kml_path):
            print(f"[WARN] Missing KML: {kml_path}")
            continue

        try:
            records, _ = parse_kml_file(kml_path)
            if not records:
                print(f"[WARN] No placemarks found in {kml_path}")
                continue

            matched_path, matched_placemarks = choose_best_match(records, clue)

            if not matched_placemarks:
                print(f"[WARN] No match for row {idx} ({kml_name} | {clue})")
                continue

            base = os.path.splitext(os.path.basename(kml_path))[0]
            safe_hint = re.sub(r"[^a-zA-Z0-9]+", "_", (matched_path or "match"))[:80].strip("_")
            out_name = f"{base}__{safe_hint}.kml"
            out_path = os.path.join(out_dir, out_name)

            export_to_kml(matched_placemarks, out_path)
            df.at[idx, out_col] = out_name

            print(f"[OK] row {idx}: matched='{matched_path}' -> {out_name} (features={len(matched_placemarks)})")

        except Exception as e:
            print(f"[ERR] row {idx}: {kml_name} failed: {e}")

    # Write back
    if spreadsheet_path.lower().endswith((".xlsx", ".xls")):
        out_xlsx = spreadsheet_path.replace(".xlsx", "_updated.xlsx").replace(".xls", "_updated.xls")
        df.to_excel(out_xlsx, index=False)
        print(f"\nSaved: {out_xlsx}")
    else:
        out_csv = spreadsheet_path.replace(".csv", "_updated.csv")
        df.to_csv(out_csv, index=False)
        print(f"\nSaved: {out_csv}")


if __name__ == "__main__":
    # ---- EDIT THESE ----
    SPREADSHEET = "input.xlsx"
    KML_BASE_DIR = "./kmls"      # folder containing input KMLs
    OUT_DIR = "./matched_kmls"   # folder to write output KMLs

    process_spreadsheet(
        spreadsheet_path=SPREADSHEET,
        kml_base_dir=KML_BASE_DIR,
        out_dir=OUT_DIR,
        kml_col="kml_file",
        clue_col="clue",
        out_col="matched_kml",
    )
```

---

## How the matching works (based on what you described)

* **Traverses deep**: builds a record for every placemark, with its **full folder path**.
* **Hierarchical clue matching**: if clue has multiple consecutive words, it tries to match **longest phrases to higher folders first**, then the remaining tokens to deeper folders.
* **Colour clues** (e.g. “blue area”):

  * detects `blue` in the clue
  * tries to use KML style colours (KML `aabbggrr` format)
  * prefers placemarks styled “close enough” to blue
* **Single-layer shortcut**:

  * if the KML effectively contains only **one container path**, it exports that without trying to match.

---

## Run it

```bash
python match_kml_layers.py
```

Outputs:

* `./matched_kmls/<original>__<matched_path>.kml`
* spreadsheet updated as `input_updated.xlsx` with `matched_kml` column filled

---

### If your “layer information” is not folder/name/description

Sometimes the “clue” actually matches other fields (ExtendedData, schema fields). If that’s the case, tell me what the clue tends to match (e.g., `ExtendedData/Data[@name="..."]`), and I’ll adjust the parser to extract those attributes and include them in scoring.
