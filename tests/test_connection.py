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


def _has_metadata(p_db: Path) -> bool:
    with sqlite3.connect(p_db) as c:
        return bool(
            c.execute(
                "SELECT count(*) FROM sqlite_master WHERE name = 'spatial_ref_sys'"
            ).fetchone()[0]
        )


def test_new_file_with_plain_path(tmp_path: Path) -> None:
    from mod_spatialite import SpatiaLiteConnection

    p_db = tmp_path / "new.sqlite"
    SpatiaLiteConnection(str(p_db)).close()
    assert p_db.is_file() and _has_metadata(p_db)


def test_new_file_with_plain_path_and_uri_flag(tmp_path: Path) -> None:
    """普通のパスに uri=True を付けた形（利用側の実例）でも新しく作られ、初期化される。"""
    from mod_spatialite import SpatiaLiteConnection

    p_db = tmp_path / "new_uri_flag.sqlite"
    SpatiaLiteConnection(str(p_db), uri=True).close()
    assert p_db.is_file() and _has_metadata(p_db)


def test_readonly_uri_on_existing_db(tmp_path: Path) -> None:
    from mod_spatialite import SpatiaLiteConnection

    p_db = tmp_path / "ro.sqlite"
    SpatiaLiteConnection(str(p_db)).close()
    con = SpatiaLiteConnection(f"file:{p_db}?mode=ro", uri=True)
    assert con.execute("SELECT spatialite_version()").fetchone()[0]
    with pytest.raises(sqlite3.OperationalError):
        con.execute("CREATE TABLE t (a)")
    con.close()
    assert not any(s.startswith("file:") for s in os.listdir("."))


def test_readonly_uri_on_missing_db_fails_without_creating(tmp_path: Path) -> None:
    from mod_spatialite import SpatiaLiteConnection

    p_db = tmp_path / "missing.sqlite"
    with pytest.raises(sqlite3.OperationalError):
        SpatiaLiteConnection(f"file:{p_db}?mode=ro", uri=True)
    assert not p_db.exists()


def test_existing_db_is_not_reinitialized(tmp_path: Path, capfd) -> None:
    from mod_spatialite import SpatiaLiteConnection

    p_db = tmp_path / "existing.sqlite"
    SpatiaLiteConnection(str(p_db)).close()
    capfd.readouterr()
    SpatiaLiteConnection(str(p_db)).close()
    assert "already exists" not in capfd.readouterr().err


def test_metadata_check_looks_at_main_only(tmp_path: Path) -> None:
    """ATTACH 先が初期化済みでも、主接続（main）が未初期化なら main を初期化する。"""
    from mod_spatialite import SpatiaLiteConnection

    p_attached = tmp_path / "attached.sqlite"
    SpatiaLiteConnection(str(p_attached)).close()
    p_main = tmp_path / "main_plain.sqlite"

    class _Plain(sqlite3.Connection):  # コンストラクタでの初期化を通らない接続
        get_spatialite_version = SpatiaLiteConnection.get_spatialite_version

    con = sqlite3.connect(p_main, factory=_Plain)
    con.execute(f"ATTACH DATABASE '{p_attached}' AS a")
    SpatiaLiteConnection.initialize(con)  # 判定と初期化だけを適用する
    con.close()
    assert _has_metadata(p_main)


def test_page_size_for_new_db(tmp_path: Path) -> None:
    from mod_spatialite import SpatiaLiteConnection

    p_db = tmp_path / "page.sqlite"
    con = SpatiaLiteConnection(str(p_db), i_page_size=65536)
    assert con.execute("PRAGMA page_size").fetchone()[0] == 65536
    con.close()


def test_page_size_ignored_for_existing_db(tmp_path: Path) -> None:
    from mod_spatialite import SpatiaLiteConnection

    p_db = tmp_path / "page_existing.sqlite"
    SpatiaLiteConnection(str(p_db)).close()
    con = SpatiaLiteConnection(str(p_db), i_page_size=65536)
    assert con.execute("PRAGMA page_size").fetchone()[0] == 4096
    assert con.set_page_size(65536) == 65536
    con.close()


def test_disconnect_defaults_do_not_vacuum(tmp_path: Path, monkeypatch) -> None:
    from mod_spatialite import SpatiaLiteConnection

    l_called: list[str] = []
    monkeypatch.setattr(
        SpatiaLiteConnection, "vacuum", lambda self: l_called.append("vacuum")
    )
    monkeypatch.setattr(
        SpatiaLiteConnection,
        "upgrade_db",
        lambda self, *a, **k: l_called.append("upgrade"),
    )
    SpatiaLiteConnection(str(tmp_path / "d1.sqlite")).disconnect()
    assert l_called == []
    SpatiaLiteConnection(str(tmp_path / "d2.sqlite")).disconnect(
        flg_vacuum=True, flg_upgrade=True
    )
    assert l_called == ["vacuum", "upgrade"]
