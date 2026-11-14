"""create_table の flg_temp の確認。"""

from pathlib import Path

from mod_spatialite import SpatiaLiteConnection


def _schemas_of(con, s_tbl: str) -> list[str]:
    l_out = []
    for s_schema in ("main", "temp"):
        n = con.execute(
            f"SELECT count(*) FROM {s_schema}.sqlite_master WHERE name = ?", (s_tbl,)
        ).fetchone()[0]
        if n:
            l_out.append(s_schema)
    return l_out


def test_flg_temp_without_coldef(tmp_path: Path) -> None:
    con = SpatiaLiteConnection(str(tmp_path / "t1.sqlite"))
    con.create_table("tbl_tmp1", flg_temp=True)
    assert _schemas_of(con, "tbl_tmp1") == ["temp"]
    con.close()


def test_flg_temp_with_coldef(tmp_path: Path) -> None:
    con = SpatiaLiteConnection(str(tmp_path / "t2.sqlite"))
    con.create_table("tbl_tmp2", ll_coldef=[["a", "INTEGER"]], flg_temp=True)
    assert _schemas_of(con, "tbl_tmp2") == ["temp"]
    con.create_table("tbl_perm", ll_coldef=[["a", "INTEGER"]])
    assert _schemas_of(con, "tbl_perm") == ["main"]
    con.close()
