"""ch_exists_table の完全一致と create_geometry_view の確認。"""

from pathlib import Path

import pytest

from mod_spatialite import SpatiaLiteConnection


@pytest.fixture()
def con(tmp_path: Path):
    c = SpatiaLiteConnection(str(tmp_path / "v.sqlite"))
    c.create_table("Gt_Site", ll_coldef=[["name", "TEXT"]])
    gt = c.add_geomcol("Gt_Site", i_epsg=4326, s_geomtype="POINT")
    c.execute(
        f'INSERT INTO "Gt_Site" (name, "{gt.gc}") VALUES (?, MakePoint(141.3, 43.0, 4326))',
        ("a",),
    )
    yield c
    c.close()


def test_exact_match(con) -> None:
    con.create_table("gtXsite", ll_coldef=[["a", "INTEGER"]])
    assert con.ch_exists_table("gt_site")  # LIKE: 大小を区別しない
    assert con.ch_exists_table("gt_site", flg_exact=True)
    assert not con.ch_exists_table("gt_ite", flg_exact=True)
    assert con.ch_exists_table("gtXsite", flg_exact=True)
    assert not con.ch_exists_table("gt%", flg_exact=True)


def test_like_wildcard_is_kept_by_default(con) -> None:
    con.create_table("gtXa", ll_coldef=[["a", "INTEGER"]])
    assert con.ch_exists_table("gt_a")  # 従来どおり _ はワイルドカード
    assert not con.ch_exists_table("gt_a", flg_exact=True)


def _registered(con) -> list[tuple]:
    return [
        tuple(r)
        for r in con.execute(
            "SELECT view_name, view_geometry, view_rowid, f_table_name,"
            " f_geometry_column FROM views_geometry_columns"
        )
    ]


def test_create_geometry_view_with_uppercase_names(con) -> None:
    gt = con.get_gtbl_from_metadata("Gt_Site")
    s_sel = f'SELECT "PK_UID", name, "{gt.gc}" FROM "Gt_Site"'
    con.create_geometry_view("Gv_Site", s_sel, gt)
    assert _registered(con) == [
        ("gv_site", gt.gc.lower(), "pk_uid", "gt_site", gt.gc.lower())
    ]
    assert con.execute('SELECT count(*) FROM "Gv_Site"').fetchone()[0] == 1
    # 作り直し（登録行を消してから再登録する）
    con.create_geometry_view("Gv_Site", s_sel, gt)
    assert len(_registered(con)) == 1


def test_overwrite_does_not_drop_a_table(con) -> None:
    gt = con.get_gtbl_from_metadata("Gt_Site")
    con.create_table("gv_clash", ll_coldef=[["a", "INTEGER"]])
    with pytest.raises(Exception):
        con.create_geometry_view(
            "gv_clash", f'SELECT "PK_UID", "{gt.gc}" FROM "Gt_Site"', gt
        )
    assert con.ch_exists_table("gv_clash", flg_exact=True)
