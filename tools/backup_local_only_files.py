"""git が持っていない手元のファイル（未追跡・.gitignore 除外）を退避コピーする。

リポジトリの整理（配置換え・不要物の削除）の前に、git に入っていない実体
（旧版の .bak、mod_spatialite_old/、Windows 用 DLL 群、作業用 sqlite 等）を
データ置き場へ複製し、SHA-256 の目録（manifest.tsv）を残す。

- コピー元のファイルは動かさない・変更しない（読むだけ）。
- コピー先が既に存在する場合は上書きせずに停止する。
- 再生成できるもの（.venv/、__pycache__/、*.egg-info/）は対象外。

使い方（リポジトリ直下で）:
    uv run python tools/backup_local_only_files.py <ラベル>
    例: uv run python tools/backup_local_only_files.py pre_revival_20261113

コピー先: ~/data/mod_spatialite/backup/<ラベル>/
"""

from __future__ import annotations

import argparse
import hashlib
import shutil
import subprocess
import sys
from pathlib import Path

DATA_DIR = Path.home() / "data" / "mod_spatialite"
EXCLUDE_TOP: set[str] = {".venv", "__pycache__"}
EXCLUDE_SUFFIX_TOP: tuple[str, ...] = (".egg-info",)


def list_local_only_files(p_repo: Path) -> list[Path]:
    """git が追跡していないファイル（.gitignore 除外分を含む）を相対パスで返す。"""
    s_out = subprocess.run(
        ["git", "-C", str(p_repo), "ls-files", "--others", "-z"],
        check=True,
        capture_output=True,
    ).stdout.decode("utf-8")
    l_files: list[Path] = []
    for s_rel in filter(None, s_out.split("\0")):
        p_rel = Path(s_rel)
        s_top = p_rel.parts[0]
        if s_top in EXCLUDE_TOP or s_top.endswith(EXCLUDE_SUFFIX_TOP):
            continue
        if "__pycache__" in p_rel.parts:
            continue
        l_files.append(p_rel)
    return sorted(l_files)


def sha256_of(p_file: Path) -> str:
    o_hash = hashlib.sha256()
    with p_file.open("rb") as f:
        for b_chunk in iter(lambda: f.read(1 << 20), b""):
            o_hash.update(b_chunk)
    return o_hash.hexdigest()


def backup(p_repo: Path, s_label: str) -> Path:
    p_dest = DATA_DIR / "backup" / s_label
    if p_dest.exists():
        sys.exit(f"停止: コピー先が既に存在します（上書きしません）: {p_dest}")
    l_files = list_local_only_files(p_repo)
    p_dest.mkdir(parents=True)
    l_rows: list[str] = ["path\tbytes\tsha256_src\tsha256_copy"]
    for p_rel in l_files:
        p_src = p_repo / p_rel
        p_out = p_dest / p_rel
        p_out.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(p_src, p_out)
        s_src = sha256_of(p_src)
        s_copy = sha256_of(p_out)
        if s_src != s_copy:
            sys.exit(f"停止: コピー後の SHA-256 が一致しません: {p_rel}")
        l_rows.append(f"{p_rel.as_posix()}\t{p_src.stat().st_size}\t{s_src}\t{s_copy}")
    s_head = subprocess.run(
        ["git", "-C", str(p_repo), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    (p_dest / "manifest.tsv").write_text("\n".join(l_rows) + "\n", encoding="utf-8")
    (p_dest / "README.txt").write_text(
        f"mod_spatialite リポジトリの git 外ファイルの退避コピー\n"
        f"ラベル: {s_label}\nコピー時の HEAD: {s_head}\n件数: {len(l_files)}\n"
        f"目録: manifest.tsv（コピー元・コピー先の SHA-256 は一致を確認済み）\n",
        encoding="utf-8",
    )
    print(f"{len(l_files)} 件を {p_dest} へコピーしました（HEAD {s_head[:7]}）。")
    return p_dest


def main() -> None:
    o_parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    o_parser.add_argument("label", help="コピー先のフォルダ名（例: pre_revival_20261113）")
    o_args = o_parser.parse_args()
    p_repo = Path(__file__).resolve().parent.parent
    backup(p_repo, o_args.label)


if __name__ == "__main__":
    main()
