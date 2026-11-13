# 標準地域メッシュ（mod_standardmesh）とメッシュ生成（generate_stdmesh）の回帰テスト
# 実行: uv run python tests/test_standardmesh.py （pytest でも実行可）
# 期待値は JIS X 0410 の区画の定義（1次: 緯度40分×経度1度、2次: 1/8、3次: 1/10、
# 分割地域メッシュ: 2分割ごとに 1=南西 2=南東 3=北西 4=北東、5倍: 2次を2分割）から手計算した。
import math

import mod_standardmesh as ms

# メッシュコード -> (lon_min, lat_min, lon_max, lat_max)
D_EXPECTED: dict[str, tuple[float, float, float, float]] = {
    "5339": (139.0, 53 / 1.5, 140.0, 54 / 1.5),
    "533945": (139.625, 35 + 2 / 3, 139.75, 35.75),
    "53394536": (139.7, 35 + 2 / 3 + 0.025, 139.7125, 35.7),
    "533945361": (139.7, 35 + 2 / 3 + 0.025, 139.70625, 35.7 - 1 / 240),  # 1/2 南西
    "533945364": (139.70625, 35.7 - 1 / 240, 139.7125, 35.7),  # 1/2 北東
    "5339453612": (139.703125, 35 + 2 / 3 + 0.025, 139.70625, 35.7 - 1 / 240 - 1 / 480),  # 1/4
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
        s_back = ms.get_mesh_index((lon_min + lon_max) / 2, (lat_min + lat_max) / 2, level)
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
    assert [str(r[0]) for r in l_rows] == ["533945361", "533945362", "533945363", "533945364"]
    con.close()


if __name__ == "__main__":
    test_stdmeshcode2wkt_extent()
    test_stdmeshcode2wkt_roundtrip()
    test_stdmeshcode2wkt_unsupported()
    test_generate_stdmesh()
    print("all standardmesh tests passed.")
