from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TEMPLATE_ROOT = ROOT / "warehouse" / "templates"
SCAN_ROOTS = [
    ROOT / "warehouse" / "views",
    ROOT / "warehouse" / "templates",
    ROOT / "warehouse" / "templatetags",
]


def iter_text_files(root: Path):
    for path in root.rglob("*"):
        if path.is_file() and path.suffix in {".py", ".html"}:
            yield path


def main() -> None:
    templates = sorted(
        str(path.relative_to(TEMPLATE_ROOT)).replace("\\", "/")
        for path in TEMPLATE_ROOT.rglob("*.html")
    )

    scanned_text = []
    for root in SCAN_ROOTS:
        for path in iter_text_files(root):
            scanned_text.append(path.read_text(encoding="utf-8", errors="ignore"))
    source = "\n".join(scanned_text)

    literal_refs = set(re.findall(r"""["']([A-Za-z0-9_./-]+\.html)["']""", source))
    tag_refs = set(
        re.findall(
            r"""{%\s*(?:extends|include)\s+["']([^"']+\.html)["']\s*%}""",
            source,
        )
    )
    refs = literal_refs | tag_refs
    maybe_unused = [template for template in templates if template not in refs]

    print(f"TEMPLATE_TOTAL={len(templates)}")
    print(f"REFERENCED_LITERAL_OR_TAG_TOTAL={len(refs)}")
    print(f"POSSIBLY_UNUSED_TOTAL={len(maybe_unused)}")
    print()
    print("POSSIBLY_UNUSED_TEMPLATES")
    for template in maybe_unused:
        print(template)


if __name__ == "__main__":
    main()
