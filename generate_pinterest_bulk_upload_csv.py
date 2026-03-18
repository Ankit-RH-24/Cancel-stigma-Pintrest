import argparse
import os
import re
from datetime import date, datetime, time, timedelta
from urllib.parse import quote

import pandas as pd


PINTEREST_HEADERS = [
    "Title",
    "Description",
    "Link",
    "Media URL",
    "Board",
    "Publish Date",
    "Alt Text",
]


def _normalize_folder_key(name: str) -> str:
    """Normalize folder names so minor spacing differences don't break mapping."""
    return re.sub(r"\s+", " ", name.strip())


def _discover_actual_folders(expected_folders: list[str]) -> dict[str, str]:
    """
    Return mapping: normalized_expected -> actual_folder_name_on_disk.
    Tolerates spacing differences (e.g., double spaces).
    """
    existing = {}
    try:
        entries = [e for e in os.listdir(".") if os.path.isdir(e)]
    except FileNotFoundError:
        entries = []

    normalized_existing = {_normalize_folder_key(e): e for e in entries}
    for exp in expected_folders:
        key = _normalize_folder_key(exp)
        if key in normalized_existing:
            existing[key] = normalized_existing[key]
        else:
            raise FileNotFoundError(
                f'Expected folder "{exp}" not found in current directory. '
                f"Looked for normalized key {key!r}."
            )
    return existing


def _is_image(filename: str) -> bool:
    ext = os.path.splitext(filename)[1].lower()
    return ext in {".jpg", ".jpeg", ".png", ".webp", ".gif"}


def _list_images(folder: str) -> list[str]:
    try:
        files = os.listdir(folder)
    except FileNotFoundError:
        return []
    images = [f for f in files if os.path.isfile(os.path.join(folder, f)) and _is_image(f)]
    images.sort()
    return images


def _github_raw_media_url(username: str, folder: str, filename: str) -> str:
    # Encode each path segment so spaces become %20 (and other reserved chars are safe)
    encoded_folder = quote(folder, safe="")
    encoded_filename = quote(filename, safe="")
    return (
        f"https://raw.githubusercontent.com/{username}/cancel-stigma-assets/main/"
        f"{encoded_folder}/{encoded_filename}"
    )


def _product_from_folder(folder_key: str) -> str:
    k = folder_key.lower()
    if "tees" in k:
        return "basic tee"
    if "hoodie" in k:
        return "cropped hoodie"
    if "socks" in k:
        return "socks"
    if "caps" in k or "cap" in k:
        return "cap"
    return "apparel"


def _slug(s: str) -> str:
    s = os.path.splitext(s)[0]
    s = re.sub(r"[_\-]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _make_title(trend_name: str, product: str, focus_keywords: list[str], filename: str, n: int) -> str:
    core_kw = ", ".join(focus_keywords[:3])
    stem = _slug(filename)
    # Keep titles punchy; still SEO-rich and unique.
    return f"{trend_name} {product} ({core_kw}) | {stem} #{n}"


def _make_description(trend_name: str, focus_line: str, product: str, filename: str) -> str:
    stem = _slug(filename)
    # Must mention Cancel Stigma in every description per requirements.
    return (
        f"{trend_name} vibes for your wardrobe: {product} styled for {focus_line}. "
        f"Shop Cancel Stigma for aesthetic clothing that feels bold, intentional, and wearable. "
        f"Save this pin for outfit inspo, capsule styling, and trending looks. "
        f"Image: {stem}."
    )


def _make_alt_text(trend_name: str, focus_line: str, product: str) -> str:
    return f"Cancel Stigma {product} photo inspired by {trend_name} — {focus_line}."


def _build_schedule(start: date, days: int = 7) -> list[str]:
    slots = [time(9, 0, 0), time(14, 0, 0), time(19, 0, 0)]
    out: list[str] = []
    for d in range(days):
        day = start + timedelta(days=d)
        for t in slots:
            out.append(datetime.combine(day, t).strftime("%Y-%m-%d %H:%M:%S"))
    return out


def generate_csv(
    github_username: str,
    output_csv: str = "cancel_stigma_pins.csv",
    store_link: str = "https://cancelstigma.com",
    board: str = "Aesthetic Clothing",
    start_date: date = date(2026, 3, 20),
) -> pd.DataFrame:
    expected_folders = [
        "UGC - Basic tees",
        "UGC - cropped hoodie + misfits",
        "UGC - socks",
        "UGC caps",
    ]

    # Folder -> (trend name, focus line, focus keywords)
    trend_map = {
        _normalize_folder_key("UGC - cropped hoodie + misfits"): (
            "Vamp Romantic",
            "dark romantic, haunting, jet black",
            ["dark romantic", "haunting", "jet black", "goth aesthetic", "romantic streetwear"],
        ),
        _normalize_folder_key("UGC - Basic tees"): (
            "Cool Blue",
            "glacier aesthetic, subzero sophistication, icy blue",
            ["glacier aesthetic", "subzero sophistication", "icy blue", "cool-toned outfit", "minimal street style"],
        ),
        _normalize_folder_key("UGC caps"): (
            "Khaki Coded",
            "utility, field jacket, paleontologist aesthetic",
            ["utility style", "field jacket vibe", "khaki coded", "workwear aesthetic", "outdoor streetwear"],
        ),
        _normalize_folder_key("UGC - socks"): (
            "Laced Up",
            "softly stitched, unexpected elegance",
            ["softly stitched", "unexpected elegance", "laced up", "quiet luxury", "elevated basics"],
        ),
    }

    actual_folders = _discover_actual_folders(expected_folders)

    # Select 21 images total, 5–6 from each folder (6 + 5 + 5 + 5 = 21).
    per_folder_target = {
        _normalize_folder_key("UGC - Basic tees"): 6,
        _normalize_folder_key("UGC - cropped hoodie + misfits"): 5,
        _normalize_folder_key("UGC - socks"): 5,
        _normalize_folder_key("UGC caps"): 5,
    }

    selections: list[tuple[str, str]] = []  # (normalized_folder_key, filename)
    for folder_key, actual in actual_folders.items():
        imgs = _list_images(actual)
        take = per_folder_target.get(folder_key, 5)
        selections.extend([(folder_key, f) for f in imgs[:take]])

    # If some folder had fewer images than desired, top up from other folders (up to 6 per folder).
    if len(selections) < 21:
        counts = {}
        for folder_key, _ in selections:
            counts[folder_key] = counts.get(folder_key, 0) + 1

        for folder_key, actual in actual_folders.items():
            cap = 6
            imgs = _list_images(actual)
            already = {fn for fk, fn in selections if fk == folder_key}
            for f in imgs:
                if len(selections) >= 21:
                    break
                if f in already:
                    continue
                if counts.get(folder_key, 0) >= cap:
                    break
                selections.append((folder_key, f))
                counts[folder_key] = counts.get(folder_key, 0) + 1

    selections = selections[:21]
    if len(selections) < 21:
        raise RuntimeError(
            f"Found only {len(selections)} images across folders; need 21 to fill 3/day for 7 days."
        )

    publish_dates = _build_schedule(start=start_date, days=7)
    if len(publish_dates) != 21:
        raise RuntimeError("Internal scheduling error: expected 21 publish slots.")

    rows = []
    for idx, ((folder_key, filename), publish_date) in enumerate(zip(selections, publish_dates), start=1):
        trend_name, focus_line, focus_keywords = trend_map[folder_key]
        product = _product_from_folder(folder_key)
        actual_folder = actual_folders[folder_key]
        media_url = _github_raw_media_url(github_username, actual_folder, filename)

        rows.append(
            {
                "Title": _make_title(trend_name, product, focus_keywords, filename, idx),
                "Description": _make_description(trend_name, focus_line, product, filename),
                "Link": store_link,
                "Media URL": media_url,
                "Board": board,
                "Publish Date": publish_date,
                "Alt Text": _make_alt_text(trend_name, focus_line, product),
            }
        )

    df = pd.DataFrame(rows, columns=PINTEREST_HEADERS)
    df.to_csv(output_csv, index=False)
    return df


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a Pinterest Bulk Upload CSV for Cancel Stigma UGC folders."
    )
    parser.add_argument(
        "--github-username",
        required=True,
        help='GitHub username used in raw URL: https://raw.githubusercontent.com/[username]/cancel-stigma-assets/main/...',
    )
    parser.add_argument("--output", default="cancel_stigma_pins.csv", help="Output CSV filename.")
    parser.add_argument("--link", default="https://cancelstigma.com", help="Default destination link for pins.")
    parser.add_argument("--board", default="Aesthetic Clothing", help="Pinterest board name.")
    parser.add_argument(
        "--start-date",
        default="2026-03-20",
        help="Start date (YYYY-MM-DD). Posts scheduled 3/day for 7 days.",
    )

    args = parser.parse_args()
    start = datetime.strptime(args.start_date, "%Y-%m-%d").date()

    generate_csv(
        github_username=args.github_username,
        output_csv=args.output,
        store_link=args.link,
        board=args.board,
        start_date=start,
    )
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
