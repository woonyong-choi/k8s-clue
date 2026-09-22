import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DOCS_ROOT = REPO_ROOT / "docs"
README = DOCS_ROOT / "README.md"
ROOT_README = REPO_ROOT / "README.md"

LOCAL_MD_LINK = re.compile(r"\[[^\]]+\]\(([^)]+\.md(?:#[^)]+)?)\)")


def test_docs_readme_links_all_docs_within_three_levels() -> None:
    assert README.exists(), "docs/README.md must be the documentation root"
    assert ROOT_README.exists(), "README.md must be the repository entrypoint"

    docs = sorted(
        path.resolve()
        for path in DOCS_ROOT.rglob("*.md")
        if path != README
    )
    assert docs, "docs/README.md should link at least one docs/*.md file"

    seen: dict[Path, int] = {README.resolve(): 0}
    queue = [README.resolve()]
    while queue:
        current = queue.pop(0)
        depth = seen[current]
        if depth >= 3:
            continue
        for target in _local_markdown_links(current):
            if target not in seen or seen[target] > depth + 1:
                seen[target] = depth + 1
                queue.append(target)

    missing = [str(path.relative_to(DOCS_ROOT)) for path in docs if path not in seen]
    assert not missing, "docs/README.md is missing <=3-level links to: " + ", ".join(missing)


def _local_markdown_links(path: Path) -> list[Path]:
    links: list[Path] = []
    for match in LOCAL_MD_LINK.finditer(path.read_text(encoding="utf-8")):
        href = match.group(1).split("#", 1)[0]
        if href.startswith(("http://", "https://", "mailto:")):
            continue
        target = (path.parent / href).resolve()
        if target.is_file() and target.is_relative_to(DOCS_ROOT.resolve()):
            links.append(target)
    return links
