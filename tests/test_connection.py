"""接続の作法（環境変数・ファイルの作成・初期化・ページサイズ・切断）の確認。"""

import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest


def _run(s_code: str, d_env: dict[str, str] | None = None) -> str:
    d_env_full = {k: v for k, v in os.environ.items() if k != "SPATIALITE_SECURITY"}
    d_env_full.update(d_env or {})
    o_res = subprocess.run(
        [sys.executable, "-c", s_code],
        capture_output=True,
        text=True,
        check=True,
        env=d_env_full,
    )
    return o_res.stdout.strip()


S_SHOW_SECURITY = (
    "import os; from mod_spatialite import SpatiaLiteConnection;"
    "SpatiaLiteConnection(':memory:').close();"
    "print(os.environ['SPATIALITE_SECURITY'])"
)


def test_security_default_is_relaxed() -> None:
    assert _run(S_SHOW_SECURITY) == "relaxed"


def test_security_respects_existing_value() -> None:
    assert _run(S_SHOW_SECURITY, {"SPATIALITE_SECURITY": "strict"}) == "strict"
