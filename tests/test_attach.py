"""attach_db（パス・URI・読み取り専用）の確認。"""

import sqlite3
from pathlib import Path

import pytest

from mod_spatialite import SpatiaLiteConnection


@pytest.fixture()
def p_other(tmp_path: Path) -> Path:
    p = tmp_path / "other db.sqlite"  # 空白を含むパス
    with sqlite3.connect(p) as c:
        c.execute("CREATE TABLE x (i INTEGER)")
    return p


def test_attach_by_path_is_writable(tmp_path: Path, p_other: Path) -> None:
    con = SpatiaLiteConnection(str(tmp_path / "m.sqlite"))
    con.attach_db(p_other, "o")
    con.execute("INSERT INTO o.x VALUES (1)")
    con.close()


def test_attach_readonly_flag(tmp_path: Path, p_other: Path) -> None:
    con = SpatiaLiteConnection(str(tmp_path / "m_ro.sqlite"), uri=True)
    con.attach_db(p_other, "o", flg_readonly=True)
    assert con.execute("SELECT count(*) FROM o.x").fetchone()[0] == 0
    with pytest.raises(sqlite3.OperationalError):
        con.execute("INSERT INTO o.x VALUES (1)")
    con.close()


def test_attach_uri_string(tmp_path: Path, p_other: Path) -> None:
    con = SpatiaLiteConnection(str(tmp_path / "m_uri.sqlite"), uri=True)
    con.attach_db(p_other.as_uri().replace("file://", "file:") + "?mode=ro", "o")
    with pytest.raises(sqlite3.OperationalError):
        con.execute("INSERT INTO o.x VALUES (1)")
    con.close()


def test_uri_without_uri_connection_is_rejected(tmp_path: Path, p_other: Path) -> None:
    con = SpatiaLiteConnection(str(tmp_path / "m_nouri.sqlite"))
    with pytest.raises(ValueError):
        con.attach_db(p_other, "o", flg_readonly=True)
    with pytest.raises(ValueError):
        con.attach_db(f"file:{p_other}?mode=ro", "o")
    con.close()
    assert sorted(x.name for x in tmp_path.iterdir()) == [
        "m_nouri.sqlite",
        "other db.sqlite",
    ]


def test_missing_path_warns_and_returns(tmp_path: Path, caplog) -> None:
    con = SpatiaLiteConnection(str(tmp_path / "m_missing.sqlite"))
    assert con.attach_db(tmp_path / "nope.sqlite", "o") is None
    assert any("not exist" in r.getMessage() for r in caplog.records)
    con.close()
