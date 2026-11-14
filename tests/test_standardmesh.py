# 標準地域メッシュ（mod_standardmesh）とメッシュ生成（generate_stdmesh）の回帰テスト
# 実行: uv run python tests/test_standardmesh.py （pytest でも実行可）
# 期待値は JIS X 0410 の区画の定義（1次: 緯度40分×経度1度、2次: 1/8、3次: 1/10、
# 分割地域メッシュ: 2分割ごとに 1=南西 2=南東 3=北西 4=北東、5倍: 2次を2分割）から手計算した。
import math

from mod_spatialite import standardmesh as ms

# メッシュコード -> (lon_min, lat_min, lon_max, lat_max)
D_EXPECTED: dict[str, tuple[float, float, float, float]] = {
    "5339": (139.0, 53 / 1.5, 140.0, 54 / 1.5),
    "533945": (139.625, 35 + 2 / 3, 139.75, 35.75),
    "53394536": (139.7, 35 + 2 / 3 + 0.025, 139.7125, 35.7),
    "533945361": (139.7, 35 + 2 / 3 + 0.025, 139.70625, 35.7 - 1 / 240),  # 1/2 南西
    "533945364": (139.70625, 35.7 - 1 / 240, 139.7125, 35.7),  # 1/2 北東
    "5339453612": (
        139.703125,
        35 + 2 / 3 + 0.025,
        139.70625,
        35.7 - 1 / 240 - 1 / 480,
    ),  # 1/4
    "5339452": (139.6875, 35 + 2 / 3, 139.75, 35 + 2 / 3 + 1 / 24),  # 5倍 南東
}


def _bbox(s_meshcode: str) -> tuple[float, float, float, float]:
    _, t_min, t_max = ms.get_stdmeshcode2wkt(s_meshcode)
    return (t_min[0], t_min[1], t_max[0], t_max[1])


def test_stdmeshcode2wkt_extent() -> None:
    for s_code, t_expected in D_EXPECTED.items():
        t_actual = _bbox(s_code)
        assert all(
            math.isclose(a, e, abs_tol=1e-9) for a, e in zip(t_actual, t_expected)
        ), f"{s_code}: {t_actual} != {t_expected}"


def test_stdmeshcode2wkt_roundtrip() -> None:
    # 区画の中心点のメッシュコードが元のコードに戻ること
    d_level: dict[str, int | str] = {
        "5339": 1,
        "533945": 2,
        "53394536": 3,
        "533945361": "half",
        "533945364": "half",
        "5339453612": "quarter",
        "5339452": "5x",
    }
    for s_code, level in d_level.items():
        lon_min, lat_min, lon_max, lat_max = _bbox(s_code)
        s_back = ms.get_mesh_index(
            (lon_min + lon_max) / 2, (lat_min + lat_max) / 2, level
        )
        assert s_back == s_code, f"{s_code} -> {s_back}"


def test_stdmeshcode2wkt_unsupported() -> None:
    assert ms.get_stdmeshcode2wkt("533945361234") == tuple()  # 1/20 細分は未実装


def test_generate_stdmesh() -> None:
    from mod_spatialite import SpatiaLiteConnection

    con = SpatiaLiteConnection(":memory:")
    # 3次メッシュ 53394536 の中に収まる範囲で 1/2 メッシュを生成 -> 4区画
    gt = con.generate_stdmesh(
        "gt_mesh", (35.699, 139.712, 35.692, 139.701), mesh_level="half"
    )
    l_rows = con.execute(
        f'SELECT mesh_code, MbrMinX("{gt.gc}"), MbrMinY("{gt.gc}") FROM "{gt.name}" ORDER BY mesh_code'
    ).fetchall()
    assert [str(r[0]) for r in l_rows] == [
        "533945361",
        "533945362",
        "533945363",
        "533945364",
    ]
    con.close()


def _random_codes(i_n: int, mesh_level: int | str, i_seed: int = 0) -> list[str]:
    # 日本付近の範囲からランダムな点を取り、get_mesh_index でメッシュコードにする
    import random

    rng = random.Random(i_seed)
    return [
        ms.get_mesh_index(
            rng.uniform(122.0, 153.9), rng.uniform(20.0, 45.9), mesh_level
        )
        for _ in range(i_n)
    ]


def test_meshcode_sql_matches_wkt() -> None:
    # SQL 式の区画・中心点が get_stdmeshcode2wkt と一致すること（TEXT 列・INTEGER 列の両方）
    from mod_spatialite import SpatiaLiteConnection

    con = SpatiaLiteConnection(":memory:")
    for level in (1, 2, 3, "half", "quarter"):
        l_codes = _random_codes(500, level) + [
            c
            for c in D_EXPECTED
            if ms.detect_meshlevel(c) == level or (level == "quarter" and len(c) == 10)
        ]
        con.execute("DROP TABLE IF EXISTS t")
        con.execute("CREATE TABLE t (c_text TEXT, c_int INTEGER)")
        con.executemany("INSERT INTO t VALUES (?, ?)", [(c, int(c)) for c in l_codes])
        for s_col in ("c_text", "c_int"):
            s_mbr = ms.get_meshcode_sql_mbr(s_col, level, 4612)
            s_pt = ms.get_meshcode_sql_point(s_col, level, 4612)
            l_rows = con.execute(
                f"SELECT c_text, MbrMinX(g), MbrMinY(g), MbrMaxX(g), MbrMaxY(g), X(p), Y(p), SRID(g), SRID(p)"
                f" FROM (SELECT c_text, {s_mbr} AS g, {s_pt} AS p FROM t)"
            ).fetchall()
            for row in l_rows:
                t_exp = _bbox(row[0])
                t_act = tuple(row[1:5])
                assert all(
                    math.isclose(a, e, abs_tol=1e-9) for a, e in zip(t_act, t_exp)
                ), f"{level} {s_col} {row[0]}: {t_act} != {t_exp}"
                assert math.isclose(row[5], (t_exp[0] + t_exp[2]) / 2, abs_tol=1e-9)
                assert math.isclose(row[6], (t_exp[1] + t_exp[3]) / 2, abs_tol=1e-9)
                assert row[7] == 4612 and row[8] == 4612
                # 中心点のメッシュコードが元に戻ること
                assert ms.get_mesh_index(row[5], row[6], level) == row[0]
    con.close()


def test_meshcode_sql_invalid_level() -> None:
    try:
        ms.get_meshcode_sql_mbr("c", "5x", 4612)
    except ValueError:
        return
    raise AssertionError("ValueError was not raised for unsupported mesh_level")


if __name__ == "__main__":
    test_stdmeshcode2wkt_extent()
    test_stdmeshcode2wkt_roundtrip()
    test_stdmeshcode2wkt_unsupported()
    test_generate_stdmesh()
    test_meshcode_sql_matches_wkt()
    test_meshcode_sql_invalid_level()
    print("all standardmesh tests passed.")


def test_get_mesh_index_point_on_boundary_goes_to_northeast() -> None:
    """区画の南西角（境界上の点）は、その区画（北東側）のコードになる。

    旧実装は浮動小数の誤差で、2 倍地域メッシュより細かいレベルの多くの角で南西隣を返していた。
    """
    import random

    o_rand = random.Random(1)
    for mesh_level, i_div in ms.D_MESH_GRID_DIV.items():
        for _ in range(300):
            i_lat = o_rand.randrange(int(20 * 1.5 * i_div), int(46 * 1.5 * i_div))
            i_lon = o_rand.randrange(22 * i_div, 54 * i_div)
            f_lon, f_lat = 100 + i_lon / i_div, i_lat / (1.5 * i_div)
            s_code = ms.get_mesh_index(f_lon, f_lat, mesh_level)
            # 区画の中心のコードと同じ（＝角の点が北東側の区画に入っている）
            s_center = ms.get_mesh_index(
                f_lon + 0.5 / i_div, f_lat + 0.5 / (1.5 * i_div), mesh_level
            )
            assert s_code == s_center, (mesh_level, f_lon, f_lat)


def test_get_mesh_index_known_codes() -> None:
    # 区画の内部の点（境界から外れた点）は従来と同じ値（旧実装で求めた値）
    assert ms.get_mesh_index(141.354, 43.0621, 1) == "6441"
    assert ms.get_mesh_index(141.354, 43.0621, 3) == "64414278"
    assert ms.get_mesh_index(141.354, 43.0621, "half") == "644142781"
    assert ms.get_mesh_index(139.000798, 37.333743, "tenth") == "5639000000"
    # 経度 141.35 は 3 次メッシュの境界上（0.35 / 0.0125 = 28）: 東側の区画になる
    # （旧実装は浮動小数の誤差で西隣の 64414277 を返していた）
    assert ms.get_mesh_index(141.35, 43.06, 3) == "64414278"
    assert ms.get_mesh_index(141.35, 43.06, "nope") == "invalid mesh_level"
