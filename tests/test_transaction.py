"""transaction() コンテキストマネージャの確認。"""

import sqlite3
from pathlib import Path

import pytest

from mod_spatialite import SpatiaLiteConnection


def _count(p_db: Path) -> int:
    with sqlite3.connect(p_db) as c:
        return c.execute("SELECT count(*) FROM t").fetchone()[0]


def test_commit_on_success(tmp_path: Path) -> None:
    p_db = tmp_path / "tx.sqlite"
    con = SpatiaLiteConnection(str(p_db))
    con.execute("CREATE TABLE t (a INTEGER)")
    with con.transaction():
        con.executemany("INSERT INTO t VALUES (?)", [(i,) for i in range(1000)])
        assert con.in_transaction
    assert not con.in_transaction
    con.close()
    assert _count(p_db) == 1000


def test_rollback_on_exception(tmp_path: Path) -> None:
    p_db = tmp_path / "tx_rb.sqlite"
    con = SpatiaLiteConnection(str(p_db))
    con.execute("CREATE TABLE t (a INTEGER)")
    with pytest.raises(ValueError):
        with con.transaction():
            con.execute("INSERT INTO t VALUES (1)")
            raise ValueError("boom")
    assert not con.in_transaction
    con.close()
    assert _count(p_db) == 0


def test_nested_transaction_is_an_error(tmp_path: Path) -> None:
    con = SpatiaLiteConnection(str(tmp_path / "tx_nest.sqlite"))
    con.begin()
    with pytest.raises(sqlite3.OperationalError):
        with con.transaction():
            pass
    con.rollback()
    con.close()
