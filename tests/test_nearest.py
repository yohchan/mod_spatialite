"""get_nearest（最近傍）の確認。総当たりの結果と一致すること。"""

import random

import pytest

from mod_spatialite import SpatiaLiteConnection


def _make(con, s_tbl: str, i_epsg: int, s_type: str, l_rows: list) -> object:
    con.create_table(s_tbl, ll_coldef=[["name", "TEXT"]])
    gt = con.add_geomcol(s_tbl, i_epsg=i_epsg, s_geomtype=s_type)
    con.executemany(
        f'INSERT INTO "{s_tbl}" (name, "{gt.gc}") VALUES (?, GeomFromText(?, {i_epsg}))',
        l_rows,
    )
    return gt


def _pts(o_rand, n, x0, x1, y0, y1, s_prefix):
    return [
        (f"{s_prefix}{i}", f"POINT({o_rand.uniform(x0, x1)} {o_rand.uniform(y0, y1)})")
        for i in range(n)
    ]


def _brute(con, gt_src, gt_dst, b_geo: bool) -> dict:
    s_d = f'ST_Distance(s."{gt_src.gc}", d."{gt_dst.gc}"{", 1" if b_geo else ""})'
    d_out = {}
    for (i_src,) in con.execute(f'SELECT "PK_UID" FROM "{gt_src.name}"').fetchall():
        d_out[i_src] = con.execute(
            f"""SELECT d."PK_UID", {s_d} AS dd FROM "{gt_src.name}" s, "{gt_dst.name}" d
            WHERE s."PK_UID" = ? ORDER BY dd LIMIT 1""",
            (i_src,),
        ).fetchone()
    return d_out


@pytest.mark.parametrize(
    ("i_epsg", "box"),
    [
        (4612, (139.0, 146.0, 41.0, 46.0)),
        (6680, (-200000.0, 200000.0, -200000.0, 200000.0)),
    ],
)
def test_matches_brute_force(i_epsg: int, box: tuple) -> None:
    o_rand = random.Random(7)
    con = SpatiaLiteConnection(":memory:")
    gt_dst = _make(con, "gt_dst", i_epsg, "POINT", _pts(o_rand, 800, *box, "d"))
    gt_src = _make(con, "gt_src", i_epsg, "POINT", _pts(o_rand, 150, *box, "s"))
    b_geo = i_epsg == 4612
    assert con.get_nearest(gt_src, gt_dst, "nn_id", "nn_dist") == 150
    d_exp = _brute(con, gt_src, gt_dst, b_geo)
    for i_src, i_nn, f_dist in con.execute(
        'SELECT "PK_UID", nn_id, nn_dist FROM gt_src'
    ):
        assert i_nn == d_exp[i_src][0]
        assert f_dist == pytest.approx(d_exp[i_src][1])
    con.close()


def test_points_to_lines_geodesic() -> None:
    o_rand = random.Random(3)
    con = SpatiaLiteConnection(":memory:")
    l_lines = []
    for i in range(60):
        x, y = o_rand.uniform(140, 145), o_rand.uniform(41, 45)
        l_lines.append((f"l{i}", f"LINESTRING({x} {y}, {x + 0.05} {y + 0.03})"))
    gt_dst = _make(con, "gt_ln", 4612, "LINESTRING", l_lines)
    gt_src = _make(con, "gt_pt", 4612, "POINT", _pts(o_rand, 80, 140, 145, 41, 45, "s"))
    con.get_nearest(gt_src, gt_dst, "nn_id", "nn_dist")
    d_exp = _brute(con, gt_src, gt_dst, True)
    for i_src, i_nn in con.execute('SELECT "PK_UID", nn_id FROM gt_pt'):
        assert i_nn == d_exp[i_src][0]
    con.close()


def test_null_geometry_empty_dst_and_no_index() -> None:
    con = SpatiaLiteConnection(":memory:")
    gt_dst = _make(con, "gt_e", 4612, "POINT", [])
    gt_src = _make(con, "gt_s", 4612, "POINT", [("a", "POINT(141 43)")])
    con.execute('INSERT INTO gt_s (name) VALUES ("null_geom")')
    with pytest.raises(ValueError):
        con.get_nearest(gt_src, gt_dst, "nn_id", flg_create_index=False)
    assert con.get_nearest(gt_src, gt_dst, "nn_id") == 2  # 索引を作って実行
    assert [r[0] for r in con.execute("SELECT nn_id FROM gt_s")] == [None, None]
    con.close()


def test_get_nearest_neighbour_writes_rowid() -> None:
    o_rand = random.Random(11)
    con = SpatiaLiteConnection(":memory:")
    gt_dst = _make(
        con, "gt_d2", 4612, "POINT", _pts(o_rand, 200, 140, 145, 41, 45, "d")
    )
    gt_src = _make(con, "gt_s2", 4612, "POINT", _pts(o_rand, 30, 140, 145, 41, 45, "s"))
    con.get_nearest_neighbour(gt_src, gt_dst, "nn_rowid", use_virtual_knn=True)
    assert (
        con.execute("SELECT count(*) FROM gt_s2 WHERE nn_rowid IS NULL").fetchone()[0]
        == 0
    )
    con.close()


def test_planar_on_geographic_matches_old_behavior() -> None:
    """経緯度の SRID で flg_geodesic=False（度のまま平面距離）なら、旧実装の総当たりと一致する。"""
    o_rand = random.Random(5)
    con = SpatiaLiteConnection(":memory:")
    gt_dst = _make(
        con, "gt_d3", 4612, "POINT", _pts(o_rand, 500, 139, 146, 41, 46, "d")
    )
    gt_src = _make(
        con, "gt_s3", 4612, "POINT", _pts(o_rand, 120, 139, 146, 41, 46, "s")
    )
    con.get_nearest(gt_src, gt_dst, "nn_id", flg_geodesic=False)
    d_exp = _brute(con, gt_src, gt_dst, False)
    for i_src, i_nn in con.execute('SELECT "PK_UID", nn_id FROM gt_s3'):
        assert i_nn == d_exp[i_src][0]
    con.close()


@pytest.mark.parametrize("f_lat", [43.0, 45.0, 70.0])
def test_longitude_shrinks_with_latitude(f_lat: float) -> None:
    """経度方向に離れた候補が、緯度方向の候補より測地線で近い場合でも取りこぼさない。

    緯度方向の候補（度の差は小さい）を KNN2 が先に見つけても、経度方向の 1 度が緯度に
    よって縮む分だけ探索範囲を広げているため、経度方向の真の最近傍を選べること。
    """
    import math

    con = SpatiaLiteConnection(":memory:")
    f_dlat = 0.5
    # 経度方向の候補を、緯度方向の候補より測地線で約 1% 近くなる位置に置く
    f_dlon = f_dlat * 0.99 / math.cos(math.radians(f_lat))
    gt_dst = _make(
        con,
        "gt_dl",
        4612,
        "POINT",
        [
            ("north", f"POINT(141.0 {f_lat + f_dlat})"),
            ("east", f"POINT({141.0 + f_dlon} {f_lat})"),
        ],
    )
    gt_src = _make(con, "gt_sl", 4612, "POINT", [("s", f"POINT(141.0 {f_lat})")])
    con.get_nearest(gt_src, gt_dst, "nn_id", "nn_dist")
    d_exp = _brute(con, gt_src, gt_dst, True)
    i_nn, f_dist = con.execute("SELECT nn_id, nn_dist FROM gt_sl").fetchone()
    assert (i_nn, f_dist) == (d_exp[1][0], pytest.approx(d_exp[1][1]))
    assert (
        con.execute("SELECT name FROM gt_dl WHERE PK_UID = ?", (i_nn,)).fetchone()[0]
        == "east"
    )
    con.close()
