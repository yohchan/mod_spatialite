"""シェープの追記取り込み（zipshps2spatialite・shp2spatialite の append モード）の列名の確認。"""

import zipfile
from pathlib import Path

import pytest
from test_zip_import import _make_zips

from mod_spatialite import SpatiaLiteConnection


def _geom_meta(con, s_tbl: str) -> list[tuple]:
    return [
        tuple(r)
        for r in con.execute(
            "SELECT f_geometry_column, geometry_type FROM geometry_columns"
            " WHERE f_table_name = ?",
            (s_tbl.lower(),),
        )
    ]


@pytest.mark.parametrize(
    ("flg_multi", "s_gc", "i_type"),
    [(False, "geom_pg_4612", 3), (True, "geom_mpg_4612", 6)],
)
def test_append_names_geometry_column(
    tmp_path: Path, flg_multi: bool, s_gc: str, i_type: int
) -> None:
    p_zipdir = _make_zips(tmp_path, 3)
    con = SpatiaLiteConnection(":memory:")
    gt = con.zipshps2spatialite(
        p_zipdir, "gt_m", 4612, "CP932", flg_multi=flg_multi, p_dir_tmp=tmp_path
    )
    assert (gt.name, gt.gc) == ("gt_m", s_gc)
    assert _geom_meta(con, "gt_m") == [(s_gc, i_type)]
    assert con.execute("SELECT count(*) FROM gt_m").fetchone()[0] == 9
    con.close()


def _extract_shps(p_zipdir: Path, p_out: Path) -> list[Path]:
    for p_zip in sorted(p_zipdir.glob("*.zip")):
        zipfile.ZipFile(p_zip).extractall(p_out)
    return sorted(p_out.rglob("*.shp"))


def test_append_to_existing_table_keeps_its_column_name(tmp_path: Path) -> None:
    """以前の版で作った表（列名 geometry）に追記しても、その列名のまま追記できる。"""
    l_shp = _extract_shps(_make_zips(tmp_path, 2), tmp_path / "x")
    con = SpatiaLiteConnection(":memory:")
    con.execute(
        f"SELECT ImportSHP('{l_shp[0].with_suffix('')}', 'gt_old', 'CP932', 4612)"
    )
    con.add_column("gt_old", "shp_name", "TEXT")
    gt = con.shp2spatialite(l_shp[1], 4612, "CP932", "gt_old", flg_append=True)
    assert gt.gc.lower() == "geometry"
    assert con.execute("SELECT count(*) FROM gt_old").fetchone()[0] == 6
    con.close()


def test_append_with_explicit_column_name(tmp_path: Path) -> None:
    l_shp = _extract_shps(_make_zips(tmp_path, 2), tmp_path / "x")
    con = SpatiaLiteConnection(":memory:")
    for p_shp in l_shp:
        gt = con.shp2spatialite(
            p_shp, 4612, "CP932", "gt_e", s_gc_dst="pg_4612", flg_append=True
        )
    assert gt.gc == "pg_4612"
    assert con.execute("SELECT count(*) FROM gt_e").fetchone()[0] == 6
    con.close()


def test_non_append_multi(tmp_path: Path) -> None:
    l_shp = _extract_shps(_make_zips(tmp_path, 1), tmp_path / "x")
    con = SpatiaLiteConnection(":memory:")
    gt = con.shp2spatialite(
        l_shp[0], 4612, "CP932", "gt_n", flg_multi=True, flg_append=False
    )
    assert (gt.gc, gt.type_abr) == ("geom_mpg_4612", "mpg")
    assert _geom_meta(con, "gt_n") == [("geom_mpg_4612", 6)]
    con.close()
