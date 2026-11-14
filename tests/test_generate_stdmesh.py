"""generate_stdmesh（SQL で一括生成）の確認。

メッシュコードは各区画の中心から get_mesh_index で求めたコードと、区画は
get_stdmeshcode2wkt（tenth は手計算の値）と一致することを確かめる。
"""

import pytest

from mod_spatialite import SpatiaLiteConnection
from mod_spatialite import standardmesh as ms

T_EXTENT = (43.13, 141.42, 43.02, 141.28)  # 札幌付近（北, 東, 南, 西）


def _rows(con, gt) -> list[tuple]:
    return con.execute(
        f"""SELECT mesh_code, MbrMinX("{gt.gc}"), MbrMinY("{gt.gc}"),
        MbrMaxX("{gt.gc}"), MbrMaxY("{gt.gc}") FROM "{gt.name}" ORDER BY mesh_code"""
    ).fetchall()


@pytest.mark.parametrize(
    "mesh_level", [1, 2, "5x", "2x", 3, "half", "quarter", "eighth", "tenth"]
)
def test_codes_and_cells(mesh_level) -> None:
    con = SpatiaLiteConnection(":memory:")
    if mesh_level in (1, 2, "5x", "2x"):
        # 大きめのレベルは範囲を広げる（角がメッシュ境界に乗らない値にする）
        t_ext = (44.11, 142.41, 42.91, 140.61)
    else:
        t_ext = T_EXTENT
    gt = con.generate_stdmesh("gt_mesh", t_ext, mesh_level=mesh_level)
    l_rows = _rows(con, gt)
    assert l_rows
    f_unitlon, f_unitlat = ms.get_unitsize(mesh_level)
    for i_code, x0, y0, x1, y1 in l_rows:
        assert x1 - x0 == pytest.approx(f_unitlon)
        assert y1 - y0 == pytest.approx(f_unitlat)
        s_code = ms.get_mesh_index((x0 + x1) / 2, (y0 + y1) / 2, mesh_level)
        assert str(i_code) == s_code
        if mesh_level != "tenth":  # 10 桁は quarter とみなされるため tenth は除く
            _, t_min, t_max = ms.get_stdmeshcode2wkt(str(i_code))
            assert (x0, y0) == pytest.approx(t_min)
            assert (x1, y1) == pytest.approx(t_max)
    # 範囲の四隅を含むメッシュがすべて入っている
    s_codes = {str(r[0]) for r in l_rows}
    f_n, f_e, f_s, f_w = t_ext
    for f_lon, f_lat in ((f_w, f_s), (f_e, f_s), (f_w, f_n), (f_e, f_n)):
        assert ms.get_mesh_index(f_lon, f_lat, mesh_level) in s_codes
    con.close()


def test_tenth_hand_calculated() -> None:
    """1/10 細分の手計算の例: 3次メッシュ 64412277 の南西端から緯度 3・経度 7 番目の区画。

    64412277 の桁: 1次 緯度 64・経度 41、2次 緯度 2・経度 2、3次 緯度 7・経度 7。
    南西端: 経度 100 + 41 + 2/8 + 7/80、緯度 64/1.5 + 2/12 + 7/120。
    """
    f_lon0 = 141 + 2 / 8 + 7 / 80 + 7 / 800
    f_lat0 = 64 / 1.5 + 2 / 12 + 7 / 120 + 3 / 1200
    con = SpatiaLiteConnection(":memory:")
    f_lon, f_lat = f_lon0 + 1 / 1600, f_lat0 + 1 / 2400  # 区画の中心
    gt = con.generate_stdmesh("gt_t", (f_lat, f_lon, f_lat, f_lon), mesh_level="tenth")
    ((i_code, x0, y0, x1, y1),) = _rows(con, gt)
    assert i_code == 6441227737
    assert (x0, y0, x1, y1) == pytest.approx(
        (f_lon0, f_lat0, f_lon0 + 1 / 800, f_lat0 + 1 / 1200)
    )
    con.close()


def test_point_on_boundary_goes_to_northeast_cell() -> None:
    """境界上の点（浮動小数で表せない経度 140.6 等）は北東側のメッシュに入る。"""
    con = SpatiaLiteConnection(":memory:")
    gt = con.generate_stdmesh("gt_e", (42.95, 140.6, 42.95, 140.6), mesh_level="2x")
    ((i_code, x0, *_),) = _rows(con, gt)
    assert x0 == pytest.approx(140.6)
    con.close()


def test_boundary_points_and_count() -> None:
    """北端・東端がメッシュ境界に一致する場合は、その北側・東側のメッシュも含む（従来どおり）。"""
    con = SpatiaLiteConnection(":memory:")
    # 3次メッシュの境界にちょうど乗る範囲: 経度 2 区画分、緯度 3 区画分
    f_w, f_s = 141.0, 43.0
    f_e, f_n = f_w + 2 / 80, f_s + 3 / 120
    gt = con.generate_stdmesh("gt_b", (f_n, f_e, f_s, f_w), mesh_level=3)
    assert con.execute('SELECT count(*) FROM "gt_b"').fetchone()[0] == (3 + 1) * (2 + 1)
    assert (
        con.execute(
            "SELECT count(*) FROM geometry_columns WHERE f_table_name = 'gt_b'"
        ).fetchone()[0]
        == 1
    )
    con.close()


def test_invalid_level_returns_zero() -> None:
    con = SpatiaLiteConnection(":memory:")
    assert con.generate_stdmesh("gt_x", T_EXTENT, mesh_level="twentieth") == 0
    con.close()
