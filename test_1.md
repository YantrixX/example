

## Req

pandas
geopandas
fastkml
lxml
shapely
openpyxl
fiona


---

## Python script

Save as `match_kml_layers.py`

```python
import os
import re
import math
import argparse
import logging
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple
from pathlib import Path
import difflib
import hashlib

import pandas as pd
import geopandas as gpd
from shapely.geometry import shape

from fastkml import kml
from lxml import etree

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# ----------------------------
# Config
# ----------------------------
STOPWORDS = {
    "area", "region", "polygon", "poly", "boundary", "layer", "zone", "the", "a", "an", "of", "and", "to", "in"
}
COLOUR_WORDS = {"blue", "red", "green", "yellow", "orange", "purple", "pink", "black", "white", "grey", "gray"}
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
    path: str
    name: str
    description: str
    geom: Any
    style_colour_rgb: Optional[Tuple[int, int, int]]
    feature_obj: Any

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
    return [t for t in tokens if t not in STOPWORDS]

def extract_colour_word(tokens: List[str]) -> Optional[str]:
    for t in tokens:
        if t in COLOUR_WORDS:
            return t
    return None

def remove_colour_tokens(tokens: List[str]) -> List[str]:
    return [t for t in tokens if t not in COLOUR_WORDS]

def ngrams(tokens: List[str], n: int) -> List[str]:
    return [" ".join(tokens[i:i+n]) for i in range(0, max(0, len(tokens)-n+1))]

def cosineish_bonus(x: int) -> float:
    return 1.0 + math.log1p(max(0, x))

def get_features(obj):
    """Shim for fastkml 0.x vs 1.x features property/method."""
    f = getattr(obj, "features", None)
    if callable(f):
        return f()
    elif f is not None:
        try:
            return list(f)
        except TypeError:
            pass
    return []

def score_path_match(path_segments: List[str], tokens: List[str]) -> float:
    """Sliding split + fuzzy matching across segments."""
    if not tokens:
        return 0.0
    segs = [normalise_text(s) for s in path_segments if s]
    if not segs:
        return 0.0

    N = len(tokens)
    best_score = -999.0

    def match_seq(toks, seg_idx):
        if seg_idx >= len(segs) or not toks:
            return 0.0
        phrase = " ".join(toks)
        seg = segs[seg_idx]
        if phrase in seg:
            return float(len(toks)) * 3.0 * cosineish_bonus(len(toks))
        m = difflib.SequenceMatcher(None, phrase, seg)
        if m.ratio() > 0.8:
            return float(len(toks)) * 2.0 * cosineish_bonus(len(toks))
        return 0.0

    # 1. Try splitting (consecutive higher layer, rest lower layer)
    if N > 1:
        for k in range(1, N):
            high_toks = tokens[:k]
            low_toks = tokens[k:]
            for i in range(len(segs)):
                for j in range(i+1, len(segs)):
                    score_i = match_seq(high_toks, i)
                    score_j = match_seq(low_toks, j)
                    s = score_i + score_j
                    if s > best_score:
                        best_score = s

    # 2. Try single segment matching
    for i in range(len(segs)):
        s = match_seq(tokens, i)
        if s > best_score:
            best_score = s

    # 3. Fallback unordered lookup
    if best_score <= 0.0:
        unordered = 0.0
        for t in tokens:
            t_matched = False
            for s in segs:
                if t in s or difflib.SequenceMatcher(None, t, s).ratio() > 0.85:
                    unordered += 1.0
                    t_matched = True
                    break
            if not t_matched:
                unordered -= 0.2
        best_score = max(best_score, unordered)

    return best_score / max(1, N)

def score_record(rec: PlacemarkRecord, clue_tokens: List[str]) -> float:
    path_segments = rec.path.split("/") if rec.path else []
    base = score_path_match(path_segments, clue_tokens)

    N = max(1, len(clue_tokens))
    name_text = normalise_text(rec.name)
    desc_text = normalise_text(rec.description)
    
    for n, w in [(3, 2.0), (2, 1.4), (1, 1.0)]:
        for phrase in ngrams(clue_tokens, n):
            if phrase and (phrase in name_text):
                base += (w * (n * 2.5)) / N
            if phrase and (phrase in desc_text):
                base += ((w * 0.8) * (n * 2.0)) / N

    return base

def colour_distance(a: Tuple[int,int,int], b: Tuple[int,int,int]) -> float:
    return math.sqrt(sum((a[i]-b[i])**2 for i in range(3)))

def colour_matches(rec_rgb: Optional[Tuple[int,int,int]], target_rgb: Tuple[int,int,int]) -> bool:
    if rec_rgb is None:
        return False
    return colour_distance(rec_rgb, target_rgb) <= 90.0

# ----------------------------
# KML Parsing
# ----------------------------
def parse_kml_file(kml_path: str) -> Tuple[List[PlacemarkRecord], Dict[str, Tuple[int,int,int]]]:
    kml_bytes = Path(kml_path).read_bytes()
    k = kml.KML()
    try:
        # fastkml 0.x
        k.from_string(kml_bytes)
    except TypeError:
        # fastkml 1.x / fallback
        import io
        k = kml.KML.parse(io.BytesIO(kml_bytes))

    style_map = extract_style_colours_fallback_xml(kml_bytes)
    records: List[PlacemarkRecord] = []
    
    for doc in get_features(k):
        walk_features(doc, parent_path="", records=records, style_map=style_map)
        
    return records, style_map

def walk_features(feature: Any, parent_path: str, records: List[PlacemarkRecord], style_map: Dict[str, Tuple[int,int,int]]):
    name = getattr(feature, "name", None) or ""
    this_path = parent_path
    if name:
        this_path = f"{parent_path}/{name}".strip("/") if parent_path else name

    geom = getattr(feature, "geometry", None)
    if geom is not None:
        desc = getattr(feature, "description", None) or ""
        style_url = getattr(feature, "styleUrl", None) or ""
        records.append(
            PlacemarkRecord(
                path=parent_path,
                name=name,
                description=desc,
                geom=geom,
                style_colour_rgb=resolve_style_colour(style_url, style_map),
                feature_obj=feature
            )
        )

    for child in get_features(feature):
        walk_features(child, parent_path=this_path, records=records, style_map=style_map)

def resolve_style_colour(style_url: str, style_map: Dict[str, Tuple[int,int,int]]) -> Optional[Tuple[int,int,int]]:
    if not style_url:
        return None
    sid = style_url.strip().lstrip("#")
    return style_map.get(sid)

def kml_aabbggrr_to_rgb(hexstr: str) -> Optional[Tuple[int,int,int]]:
    if not hexstr:
        return None
    s = hexstr.strip().lower().replace("#", "")
    if len(s) == 6:
        s = "ff" + s  # missing alpha
    if not re.fullmatch(r"[0-9a-f]{8}", s):
        return None
    bb, gg, rr = int(s[2:4], 16), int(s[4:6], 16), int(s[6:8], 16)
    return (rr, gg, bb)

def extract_style_colours_fallback_xml(kml_bytes: bytes) -> Dict[str, Tuple[int,int,int]]:
    """Resolves both direct <Style> and indrect <StyleMap>."""
    ns = {"kml": "http://www.opengis.net/kml/2.2"}
    try:
        root = etree.fromstring(kml_bytes)
    except etree.XMLSyntaxError:
        return {}
        
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

    for smap in root.findall(".//kml:StyleMap", namespaces=ns):
        sid = smap.get("id")
        if not sid:
            continue
        for pair in smap.findall(".//kml:Pair", namespaces=ns):
            key = pair.find("kml:key", namespaces=ns)
            if key is not None and key.text == "normal":
                styleUrl = pair.find("kml:styleUrl", namespaces=ns)
                if styleUrl is not None and styleUrl.text:
                    link_id = styleUrl.text.lstrip("#")
                    if link_id in out:
                        out[sid] = out[link_id]
                break
    return out

# ----------------------------
# Matching + export
# ----------------------------
def choose_best_match(records: List[PlacemarkRecord], clue: str) -> Tuple[Optional[str], List[PlacemarkRecord]]:
    if not records:
        return None, []

    paths = sorted({r.path for r in records})
    tokens = tokenise_clue(clue)
    colour_word = extract_colour_word(tokens)
    target_rgb = COLOUR_TARGETS.get(colour_word) if colour_word else None

    # Single-layer logic
    if len(paths) == 1:
        matched_path = paths[0]
        matched = records
        if target_rgb:
            col_matched = [r for r in matched if colour_matches(r.style_colour_rgb, target_rgb)]
            if col_matched:
                matched = col_matched
        return matched_path, matched

    tokens_wo_colour = remove_colour_tokens(tokens)
    scored: List[Tuple[float, PlacemarkRecord]] = []
    
    for r in records:
        s = score_record(r, tokens_wo_colour)
        if target_rgb is not None:
            if colour_matches(r.style_colour_rgb, target_rgb):
                s += 6.0
            else:
                s -= 1.0
        scored.append((s, r))

    scored.sort(key=lambda x: x[0], reverse=True)
    best_score, best_rec = scored[0]
    best_path = best_rec.path

    # Extract all from the winning path
    matched = [r for r in records if r.path == best_path]
    if target_rgb is not None:
        colour_matched = [r for r in matched if colour_matches(r.style_colour_rgb, target_rgb)]
        if colour_matched:
            matched = colour_matched

    return best_path, matched

def export_to_kml(placemarks: List[PlacemarkRecord], source_kml_path: str, out_kml_path: str):
    """Style-preserving export blending lxml with fastkml."""
    if not placemarks:
        raise ValueError("No placemarks to export")

    out_path = Path(out_kml_path).resolve()
    out_dir = out_path.parent
    if out_dir:
        out_dir.mkdir(parents=True, exist_ok=True)
        
    try:
        source_bytes = Path(source_kml_path).read_bytes()
        ns = {"kml": "http://www.opengis.net/kml/2.2"}
        tree = etree.fromstring(source_bytes)
        
        root = etree.Element("{http://www.opengis.net/kml/2.2}kml", nsmap={None: "http://www.opengis.net/kml/2.2"})
        doc = etree.SubElement(root, "{http://www.opengis.net/kml/2.2}Document")
        
        for style in tree.findall(".//kml:Style", namespaces=ns):
            doc.append(style)
        for smap in tree.findall(".//kml:StyleMap", namespaces=ns):
            doc.append(smap)
            
        for pm in placemarks:
            try:
                # Fastkml to_string
                pm_xml_str = pm.feature_obj.to_string()
                if isinstance(pm_xml_str, bytes):
                    pm_xml_str = pm_xml_str.decode("utf-8")
                # Remove kml wrappers fastkml might insert
                pm_xml_str = re.sub(r'xmlns(:\w+)?="[^"]+"', '', pm_xml_str) 
                pm_elem = etree.fromstring(pm_xml_str.encode("utf-8"))
                
                # strip namespaces to match target document nicely
                for elem in pm_elem.getiterator():
                    elem.tag = etree.QName(elem).localname
                
                doc.append(pm_elem)
            except Exception as e:
                logger.warning(f"Failed to copy XML for '{pm.name}': {e}")
                
        tree_out = etree.ElementTree(root)
        tree_out.write(str(out_path), pretty_print=True, xml_declaration=True, encoding="UTF-8")
        return
        
    except Exception as e:
        logger.error(f"Style-preserving export failed ({e}). Falling back to GeoPandas.")

    # Geopandas fallback
    rows = []
    for pm in placemarks:
        try:
            shp = getattr(pm.geom, "geom_type", None) and pm.geom or shape(pm.geom)
            rows.append({"Name": pm.name or "matched", "Description": pm.description or "", "geometry": shp})
        except Exception:
            pass
            
    if rows:
        gdf = gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:4326")
        gdf.to_file(str(out_path), driver="KML")

# ----------------------------
# Main Driver
# ----------------------------
def process_spreadsheet(args):
    spreadsheet_path = Path(args.spreadsheet)
    if not spreadsheet_path.exists():
        logger.error(f"Spreadsheet not found: {spreadsheet_path}")
        return

    ext = spreadsheet_path.suffix.lower()
    if ext in (".xlsx", ".xls"):
        df = pd.read_excel(spreadsheet_path)
    else:
        df = pd.read_csv(spreadsheet_path)

    if args.out_col not in df.columns:
        df[args.out_col] = ""

    out_dir = Path(args.out_dir)

    for idx, row in df.iterrows():
        kml_name = str(row.get(args.kml_col, "")).strip()
        clue = str(row.get(args.clue_col, "")).strip()

        if not kml_name or kml_name.lower() == "nan":
            continue

        kml_path = Path(kml_name)
        if not kml_path.is_absolute():
            kml_path = Path(args.kml_base_dir) / kml_name

        if not kml_path.exists():
            logger.warning(f"Row {idx}: Missing KML - {kml_path}")
            continue

        try:
            records, _ = parse_kml_file(str(kml_path))
            if not records:
                logger.warning(f"Row {idx}: No placemarks found in {kml_path.name}")
                continue

            matched_path, matched_placemarks = choose_best_match(records, clue)

            if not matched_placemarks:
                logger.warning(f"Row {idx}: No match for ({kml_name} | {clue})")
                continue

            safe_hint = re.sub(r"[^a-zA-Z0-9]+", "_", (matched_path or "match"))[:40].strip("_")
            row_hash = hashlib.md5(f"{idx}-{clue}".encode()).hexdigest()[:4]
            out_name = f"{kml_path.stem}_{safe_hint}_{row_hash}.kml"
            out_path = out_dir / out_name

            logger.info(f"Row {idx}: matched='{matched_path}' -> {out_name} (features={len(matched_placemarks)})")

            if not args.dry_run:
                export_to_kml(matched_placemarks, str(kml_path), str(out_path))
                df.at[idx, args.out_col] = out_name

        except Exception as e:
            logger.error(f"Row {idx}: {kml_name} failed: {e}")

    if args.dry_run:
        logger.info("Dry-run complete. No files saved.")
        return

    out_xlsx = spreadsheet_path.with_stem(f"{spreadsheet_path.stem}_updated")
    if ext in (".xlsx", ".xls"):
        df.to_excel(out_xlsx, index=False)
    else:
        df.to_csv(out_xlsx, index=False)
    logger.info(f"Saved updated spreadsheet to: {out_xlsx}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract relevant KML layers based on text clues.")
    parser.add_argument("--spreadsheet", default="input.xlsx", help="Path to input spreadsheet")
    parser.add_argument("--kml-base-dir", default="./kmls", help="Base directory for input KMLs")
    parser.add_argument("--out-dir", default="./matched_kmls", help="Directory to save extracted KMLs")
    parser.add_argument("--kml-col", default="kml_file", help="Column name containing KML filename")
    parser.add_argument("--clue-col", default="clue", help="Column name containing text clue")
    parser.add_argument("--out-col", default="matched_kml", help="Column name to write output filenames")
    parser.add_argument("--dry-run", action="store_true", help="Find matches without writing files")

    args = parser.parse_args()
    process_spreadsheet(args)
```

---

## Run it

```bash
python extract-layers.py --spreadsheet data.xlsx --kml-base-dir ./my_kmls

```

