"""ATTACH 先のスキーマを指定するヘルパーの確認。"""

from pathlib import Path

import pytest

from mod_spatialite import GTBL, SpatiaLiteConnection


@pytest.fixture()
def con(tmp_path: Path):
    p_other = tmp_path / "other.sqlite"
    o = SpatiaLiteConnection(str(p_other))
    o.create_table("gt_site", ll_coldef=[["name", "TEXT"]])
    o.add_geomcol("gt_site", i_epsg=4326, s_geomtype="POINT")
    o.execute('CREATE VIEW v_site AS SELECT * FROM "gt_site"')
    o.close()

    c = SpatiaLiteConnection(str(tmp_path / "main.sqlite"), uri=True)
    c.create_table("tbl_local", ll_coldef=[["a", "INTEGER"]])
    c.attach_db(p_other, "o", flg_readonly=True)
    yield c
    c.close()


def test_get_tbllist_by_schema(con) -> None:
    assert "gt_site" not in con.get_tbllist()  # 従来どおり main のみ
    assert "gt_site" in con.get_tbllist(s_schema="o")
    assert "v_site" not in con.get_tbllist(s_schema="o")
    assert "v_site" in con.get_tbllist(s_schema="o", flg_include_views=True)


def test_ch_exists_table_by_schema(con) -> None:
    assert con.ch_exists_table("tbl_local")
    assert not con.ch_exists_table("gt_site")
    assert con.ch_exists_table("gt_site", s_schema="o")
    assert not con.ch_exists_table("v_site", s_schema="o")
    assert con.ch_exists_table("v_site", s_schema="o", flg_include_views=True)


def test_get_gtbl_from_attached_metadata(con) -> None:
    assert con.get_gtbl_from_metadata("gt_site") is None  # main には無い
    gt = con.get_gtbl_from_metadata("gt_site", s_schema="o")
    assert isinstance(gt, GTBL)
    assert (gt.name, gt.gc, gt.epsg, gt.type_abr) == (
        "gt_site",
        "geom_pt_4326",
        4326,
        "pt",
    )
    assert con.get_gtbl_from_metadata("GT_SITE", s_schema="o") is not None


def test_columns_and_primary_key_by_schema(con) -> None:
    l_cols = [d["name"] for d in con.get_columnsdef("gt_site", s_schema="o")]
    assert l_cols[:2] == ["PK_UID", "name"]
    assert con.ch_exists_col("gt_site", "name", s_schema="o")
    assert con.get_primary_key("gt_site", s_schema="o") == "PK_UID"
