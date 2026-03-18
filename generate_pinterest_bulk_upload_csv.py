"""
Cancel Stigma — Pinterest Bulk Upload CSV Generator
====================================================
Recursively scans UGC category folders (and their color-coded subfolders)
using os.walk(), maps each image to a Pinterest Predicts 2026 trend based on
the subfolder colour name, and writes a 21-pin scheduled CSV ready for
Pinterest's bulk upload tool.

URL format:  BASE/CATEGORY/COLOR/FILENAME  (or BASE/CATEGORY/FILENAME for
             images sitting directly in a category folder)
All path segments are URL-encoded so spaces become %20.

Run:
    python generate_pinterest_bulk_upload_csv.py
    python generate_pinterest_bulk_upload_csv.py --start-date 2026-04-01
    python generate_pinterest_bulk_upload_csv.py --base-url https://...
"""

import argparse
import os
import re
from datetime import date, datetime, time, timedelta
from urllib.parse import quote

import pandas as pd


# ── Constants ──────────────────────────────────────────────────────────────────

PINTEREST_HEADERS = [
    "Title",
    "Media URL",
    "Pinterest board",
    "Description",
    "Link",
    "Publish date",
    "Alt Text",
]

BASE_URL = "https://raw.githubusercontent.com/Ankit-RH-24/Cancel-stigma-Pintrest/main"

IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".gif"}

# Ordered so that multi-word keys (e.g. "dark blue") are checked before "blue".
CATEGORY_FOLDERS = [
    "UGC - Basic tees",
    "UGC - cropped hoodie + misfits",
    "UGC - socks",
    "UGC caps",
]

# How many images to pull from each category (total = 21).
PER_FOLDER_TARGET: dict[str, int] = {
    "ugc - basic tees": 6,
    "ugc - cropped hoodie + misfits": 5,
    "ugc - socks": 5,
    "ugc caps": 5,
}

# ── Trend configuration ────────────────────────────────────────────────────────
# Keyed by colour keyword found inside the subfolder name.
# Longer/more-specific keys must appear before shorter ones that are substrings
# so that "dark blue" wins over "blue" — handled by _detect_color_key() ordering.
#
# Each entry:
#   name        – trend label
#   trend_hook  – punchy hook phrase used as the title opener
#   keywords    – Pinterest Predicts 2026 keywords (used in description)
#   desc        – description template; {product} is replaced at runtime

TREND_CONFIG: dict[str, dict] = {
    # ── Cool Blue ──
    "dark blue": dict(
        name="Cool Blue",
        trend_hook="Glacier Aesthetic",
        keywords=["glacier aesthetic", "subzero sophistication", "icy blue"],
        desc=(
            "Embrace the Cool Blue trend with this {product} from Cancel Stigma — "
            "glacier aesthetics and subzero sophistication woven into every thread. "
            "Styled in icy blue for those who dress ahead of the curve."
        ),
    ),
    "blue": dict(
        name="Cool Blue",
        trend_hook="Glacier Aesthetic",
        keywords=["glacier aesthetic", "subzero sophistication", "icy blue"],
        desc=(
            "Embrace the Cool Blue trend with this {product} from Cancel Stigma — "
            "glacier aesthetics meet subzero sophistication in every detail. "
            "Icy blue hues for those who run cold and dress colder."
        ),
    ),
    # ── Vamp Romantic ──
    "black": dict(
        name="Vamp Romantic",
        trend_hook="Dark Romantic",
        keywords=["dark romantic", "after-dark glamour", "haunting and heartbreaking"],
        desc=(
            "Step into the Vamp Romantic era with this jet black {product} from Cancel Stigma — "
            "dark romantic energy, after-dark glamour, and something hauntingly heartbreaking "
            "in every stitch. Aesthetic clothing for those who wear their mood."
        ),
    ),
    # ── Khaki Coded ──
    "brown": dict(
        name="Khaki Coded",
        trend_hook="Utility Chic",
        keywords=["utility streetwear", "paleontologist aesthetic", "earth tones"],
        desc=(
            "Go Khaki Coded with this earth-toned {product} from Cancel Stigma — "
            "utility streetwear collides with the paleontologist aesthetic for a look "
            "that's rugged, intentional, and deeply wearable."
        ),
    ),
    "tan": dict(
        name="Khaki Coded",
        trend_hook="Utility Chic",
        keywords=["utility streetwear", "paleontologist aesthetic", "earth tones"],
        desc=(
            "Lean into the Khaki Coded trend with this tan {product} from Cancel Stigma — "
            "a perfect blend of utility streetwear and paleontologist aesthetic. "
            "Earth tones have never felt this intentional."
        ),
    ),
    # ── Laced Up ──
    "white": dict(
        name="Laced Up",
        trend_hook="Softly Stitched",
        keywords=["softly stitched", "unexpected elegance", "lace details"],
        desc=(
            "Discover the Laced Up trend with this softly stitched {product} from Cancel Stigma — "
            "unexpected elegance woven into every lace detail. "
            "A quiet piece that speaks volumes in your capsule wardrobe."
        ),
    ),
    "cream": dict(
        name="Laced Up",
        trend_hook="Softly Stitched",
        keywords=["softly stitched", "unexpected elegance", "lace details"],
        desc=(
            "The Laced Up trend gets a cream-toned moment with this {product} from Cancel Stigma — "
            "softly stitched, wrapped in unexpected elegance, finished with lace details "
            "that reward a closer look."
        ),
    ),
    # ── Poetcore ──
    "oversized": dict(
        name="Poetcore",
        trend_hook="Poet Aesthetic",
        keywords=["inner wordsmith", "vintage blazers", "the poet aesthetic"],
        desc=(
            "Channel your inner wordsmith with this oversized {product} from Cancel Stigma — "
            "Poetcore energy, vintage blazer soul, and the poet aesthetic from collar to hem. "
            "Draped, literary, and deeply personal."
        ),
    ),
    # ── Pink / Green fallback to Laced Up / Khaki Coded respectively ──
    "pink": dict(
        name="Laced Up",
        trend_hook="Blush Romantic",
        keywords=["softly stitched", "unexpected elegance", "lace details"],
        desc=(
            "The Laced Up trend takes a blush turn with this pink {product} from Cancel Stigma — "
            "softly stitched with unexpected elegance and a whisper of lace details. "
            "Feminine, refined, and effortlessly aesthetic."
        ),
    ),
    "green": dict(
        name="Khaki Coded",
        trend_hook="Earth Tones",
        keywords=["utility streetwear", "paleontologist aesthetic", "earth tones"],
        desc=(
            "Go earthy with this green {product} from Cancel Stigma — Khaki Coded utility streetwear "
            "meets the paleontologist aesthetic in every stitch. "
            "Earth tones, intentional design, and attitude to match."
        ),
    ),
}

# Aesthetic hooks cycled through pins for title variety.
# Format already includes the separator so titles read naturally.
AESTHETIC_HOOKS = [
    "| Minimalist Vibe",
    "| 2026 Style",
    "| Cancel Stigma",
    "| Wardrobe Essential",
    "| Streetwear Edit",
    "| Aesthetic Choice",
]

# Per-category fallback colour key when a subfolder name doesn't match any TREND_CONFIG key
# (also used for images sitting directly at the category-folder root).
CATEGORY_DEFAULT_COLOR: dict[str, str] = {
    "ugc - basic tees": "blue",               # → Cool Blue
    "ugc - cropped hoodie + misfits": "black", # → Vamp Romantic
    "ugc - socks": "white",                    # → Laced Up
    "ugc caps": "brown",                       # → Khaki Coded
}


# ── Helpers ────────────────────────────────────────────────────────────────────

def _norm(name: str) -> str:
    """Collapse multiple spaces and strip; used for case-insensitive key matching."""
    return re.sub(r"\s+", " ", name.strip())


def _is_image(filename: str) -> bool:
    return os.path.splitext(filename)[1].lower() in IMAGE_EXTS


def _find_actual_folder(expected: str) -> str:
    """
    Locate the category folder on disk, tolerating spacing differences (e.g. double spaces).
    """
    key = _norm(expected).lower()
    for entry in os.listdir("."):
        if os.path.isdir(entry) and _norm(entry).lower() == key:
            return entry
    raise FileNotFoundError(
        f'Expected folder "{expected}" not found in the current directory '
        f"(searched normalized key: {key!r})."
    )


def _detect_color_key(subfolder: str, actual_cat: str) -> str:
    """
    Determine the TREND_CONFIG key for a given subfolder name.

    Checks TREND_CONFIG keys in declaration order (longer/more-specific first)
    so "dark blue" is matched before "blue".  Falls back to the per-category
    default if the subfolder name doesn't contain any known colour keyword.
    """
    sf_lower = _norm(subfolder).lower()
    for color_key in TREND_CONFIG:          # dict preserves insertion order (Python 3.7+)
        if color_key in sf_lower:
            return color_key
    return CATEGORY_DEFAULT_COLOR.get(_norm(actual_cat).lower(), "blue")


def _product_label(actual_cat: str) -> str:
    k = _norm(actual_cat).lower()
    if "tees" in k:
        return "Basic Tee"
    if "hoodie" in k:
        return "Cropped Hoodie"
    if "socks" in k:
        return "Socks"
    if "cap" in k:
        return "Cap"
    return "Apparel"


def _raw_url(base: str, *segments: str) -> str:
    """
    Build a raw GitHub URL, URL-encoding each path segment independently
    so that spaces become %20 and other reserved characters are also safe.
    Empty segments (e.g. no subfolder) are silently skipped.
    """
    encoded = "/".join(quote(s, safe="") for s in segments if s)
    return f"{base.rstrip('/')}/{encoded}"


def _make_title(config: dict, color_key: str, product: str, filename: str, pin_index: int) -> str:
    """
    Build a unique title from three parts:
      1. Trend Hook  — e.g. "Glacier Aesthetic"
      2. Color/Product — e.g. "Dark Blue Basic Tee"
      3. Aesthetic Hook — cycled from AESTHETIC_HOOKS by pin_index

    The filename stem (first 6 chars) is kept as a fallback; the dedup pass
    in generate_csv appends it only when a collision is detected.
    """
    trend_hook = config["trend_hook"]
    color_label = color_key.replace("-", " ").title()
    color_product = f"{color_label} {product}"
    hook = AESTHETIC_HOOKS[pin_index % len(AESTHETIC_HOOKS)]
    return f"{trend_hook}: {color_product} {hook}"


def _make_description(config: dict, product: str) -> str:
    return config["desc"].format(product=product.lower())


def _make_alt_text(config: dict, product: str) -> str:
    kw1, kw2 = (config["keywords"] + [""])[:2]
    return f"Cancel Stigma {product} — {config['name']} style: {kw1} and {kw2}."


# ── Core logic ─────────────────────────────────────────────────────────────────

def _walk_images(per_folder_target: dict[str, int]) -> list[tuple[str, str, str]]:
    """
    Use os.walk() to recursively discover images across all category folders and
    their colour-coded subfolders.

    Returns a list of (actual_category, subfolder_or_empty, filename) tuples.
    - subfolder is "" for images sitting directly in the category folder root.
    - subfolder is the relative path (e.g. "black", "dark blue") for images
      inside a colour subfolder.
    - Per-folder target limits how many images are taken from each category.
    """
    results: list[tuple[str, str, str]] = []

    for cat in CATEGORY_FOLDERS:
        actual = _find_actual_folder(cat)
        target = per_folder_target.get(_norm(cat).lower(), 5)
        folder_count = 0

        for dirpath, _dirs, filenames in os.walk(actual):
            if folder_count >= target:
                break
            rel = os.path.relpath(dirpath, actual)
            subfolder = "" if rel == "." else rel

            for fn in sorted(filenames):
                if folder_count >= target:
                    break
                if _is_image(fn):
                    results.append((actual, subfolder, fn))
                    folder_count += 1

    return results


def _build_schedule(start: date, days: int = 7) -> list[str]:
    slots = [time(9, 0, 0), time(14, 0, 0), time(19, 0, 0)]
    out: list[str] = []
    for d in range(days):
        day = start + timedelta(days=d)
        for t in slots:
            out.append(datetime.combine(day, t).strftime("%Y-%m-%d %H:%M:%S"))
    return out


# ── Public API ─────────────────────────────────────────────────────────────────

def generate_csv(
    output_csv: str = "cancel_stigma_pins.csv",
    store_link: str = "https://cancelstigma.com",
    board: str = "Aesthetic Clothing",
    start_date: date = date(2026, 3, 20),
    base_url: str = BASE_URL,
) -> pd.DataFrame:
    per_folder_target: dict[str, int] = {
        "ugc - basic tees": 6,
        "ugc - cropped hoodie + misfits": 5,
        "ugc - socks": 5,
        "ugc caps": 5,
    }

    images = _walk_images(per_folder_target)[:21]

    if len(images) < 21:
        raise RuntimeError(
            f"Found only {len(images)} images across all folders; "
            "need exactly 21 to fill 3 posts/day × 7 days. "
            "Check that your UGC folders contain enough image files."
        )

    publish_dates = _build_schedule(start=start_date, days=7)  # always 21 slots

    rows: list[dict] = []
    for pin_idx, ((actual_cat, subfolder, filename), publish_date) in enumerate(
        zip(images, publish_dates), start=1
    ):
        color_key = _detect_color_key(subfolder, actual_cat)
        config = TREND_CONFIG[color_key]
        product = _product_label(actual_cat)

        # Build Media URL:
        #   with subfolder → BASE/CATEGORY/COLOR/FILENAME
        #   without subfolder → BASE/CATEGORY/FILENAME
        segments = [actual_cat, subfolder, filename] if subfolder else [actual_cat, filename]
        media_url = _raw_url(base_url, *segments)

        rows.append(
            {
                "Title": _make_title(config, color_key, product, filename, pin_idx),
                "Media URL": media_url,
                "Pinterest board": board.strip(),
                "Description": _make_description(config, product),
                "Link": store_link,
                "Publish date": publish_date,
                "Alt Text": _make_alt_text(config, product),
                # Store filename stem for dedup fallback (dropped before CSV export).
                "_filename_stem": os.path.splitext(filename)[0][:6],
            }
        )

    # ── Deduplication pass ────────────────────────────────────────────────────
    # If any two rows share the same Title, append the filename stem to make
    # every title 100% unique, regardless of how many images share a trend+color.
    seen: dict[str, int] = {}
    for row in rows:
        t = row["Title"]
        seen[t] = seen.get(t, 0) + 1

    collision_counts: dict[str, int] = {}
    for row in rows:
        t = row["Title"]
        if seen[t] > 1:
            stem = row["_filename_stem"]
            row["Title"] = f"{t} — {stem}"
        del row["_filename_stem"]       # remove internal helper key before export

    df = pd.DataFrame(rows, columns=PINTEREST_HEADERS)
    df.to_csv(output_csv, index=False, encoding="utf-8")
    return df


# ── CLI ────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Generate a Pinterest Bulk Upload CSV for Cancel Stigma. "
            "Recursively scans UGC category folders and their colour-coded subfolders, "
            "maps each image to a Pinterest Predicts 2026 trend, and schedules 21 pins "
            "at 09:00, 14:00, and 19:00 over 7 days."
        )
    )
    parser.add_argument(
        "--base-url",
        default=BASE_URL,
        help=(
            "GitHub raw base URL. Category, subfolder, and filename are appended "
            "with %%20 encoding. Default: %(default)s"
        ),
    )
    parser.add_argument(
        "--output",
        default="cancel_stigma_pins.csv",
        help="Output CSV filename. Default: %(default)s",
    )
    parser.add_argument(
        "--link",
        default="https://cancelstigma.com",
        help="Destination link for all pins. Default: %(default)s",
    )
    parser.add_argument(
        "--board",
        default="Aesthetic Clothing",
        help='Pinterest board name. Default: "%(default)s"',
    )
    parser.add_argument(
        "--start-date",
        default="2026-03-20",
        help="First publish date (YYYY-MM-DD). Default: %(default)s",
    )

    args = parser.parse_args()
    start = datetime.strptime(args.start_date, "%Y-%m-%d").date()

    df = generate_csv(
        output_csv=args.output,
        store_link=args.link,
        board=args.board,
        start_date=start,
        base_url=args.base_url,
    )

    print(f"\n✓ Wrote {args.output}")
    print(f"  {len(df)} pins  |  {args.start_date} → 7 days  |  09:00 / 14:00 / 19:00 daily")
    print(f"\nTrend breakdown:")
    trend_counts = df["Title"].str.extract(r"^([^:]+):")[0].value_counts()
    for title_prefix, count in trend_counts.items():
        print(f"  {title_prefix!s:<45} {count} pins")
    print(f"\nSample Media URL:\n  {df['Media URL'].iloc[0]}\n")


if __name__ == "__main__":
    main()
