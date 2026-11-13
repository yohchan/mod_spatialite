# zip のシェープ・dbf の取り込みの回帰テスト
# 実行: uv run python tests/test_zip_import.py （pytest でも実行可）
# zip のあるディレクトリを読み取り専用にしても取り込めること（zip の隣に何も書かないこと）と、
# CP932 の属性が文字化けしないことを確かめる。
import os
import stat
import tempfile
import zipfile
from pathlib import Path

from mod_spatialite import SpatiaLiteConnection


def _make_zips(p_dir: Path, i_n: int = 2) -> Path:
    # SpatiaLite の ExportSHP で CP932 のシェープを作り、zip（中に階層あり）にまとめる
    p_zipdir = p_dir / "zips"
    p_zipdir.mkdir()
    con = SpatiaLiteConnection(":memory:")
    con.execute("CREATE TABLE m (mesh_id TEXT, city TEXT, pop INTEGER)")
    con.execute("SELECT AddGeometryColumn('m', 'geom', 4612, 'POLYGON', 'XY')")
    for i_zip in range(1, i_n + 1):
        con.execute("DELETE FROM m")
        for i in range(3):
            con.execute(
                "INSERT INTO m VALUES (?, '札幌市', ?, BuildMbr(141, 43, 141.01, 43.01, 4612))",
                (f"64414{i_zip}{i}01", i * 10),
            )
        p_base = p_dir / f"m_{i_zip:02d}"
        con.execute(f"SELECT ExportSHP('m', 'geom', '{p_base}', 'CP932')")
        with zipfile.ZipFile(p_zipdir / f"m_{i_zip:02d}.zip", "w") as zf:
            for s_ext in ("shp", "shx", "dbf", "prj"):
                p_file = Path(f"{p_base}.{s_ext}")
                if p_file.exists():
                    zf.write(p_file, f"sub/m_{i_zip:02d}.{s_ext}")
                    p_file.unlink()
    con.close()
    return p_zipdir


def _readonly(p_dir: Path) -> None:
    os.chmod(p_dir, stat.S_IRUSR | stat.S_IXUSR)


def test_zipdbfs2spatialite() -> None:
    with tempfile.TemporaryDirectory() as s_dir:
        p_dir = Path(s_dir)
        p_zipdir = _make_zips(p_dir)
        _readonly(p_zipdir)
        try:
            con = SpatiaLiteConnection(":memory:")
            i_n = con.zipdbfs2spatialite(p_zipdir, "tbl_m", "CP932", p_dir_tmp=p_dir)
            assert i_n == 2
            l_rows = con.execute(
                "SELECT mesh_id, city, pop, dbf_name FROM tbl_m ORDER BY mesh_id"
            ).fetchall()
            assert len(l_rows) == 6
            assert {r["city"] for r in l_rows} == {"札幌市"}
            assert {r["dbf_name"] for r in l_rows} == {"m_01", "m_02"}
            assert sorted(os.listdir(p_zipdir)) == ["m_01.zip", "m_02.zip"]
            con.close()
        finally:
            os.chmod(p_zipdir, stat.S_IRWXU)


def test_zipshps2spatialite_readonly_dir() -> None:
    with tempfile.TemporaryDirectory() as s_dir:
        p_dir = Path(s_dir)
        p_zipdir = _make_zips(p_dir)
        _readonly(p_zipdir)
        try:
            con = SpatiaLiteConnection(":memory:")
            gt = con.zipshps2spatialite(
                str(p_zipdir), "gt_m", 4612, "CP932", p_dir_tmp=p_dir
            )
            assert gt is not None
            assert con.execute("SELECT count(*) FROM gt_m").fetchone()[0] == 6
            assert con.execute("SELECT DISTINCT city FROM gt_m").fetchone()[0] == "札幌市"
            assert sorted(os.listdir(p_zipdir)) == ["m_01.zip", "m_02.zip"]
            # 一時ディレクトリが残っていないこと
            assert sorted(os.listdir(p_dir)) == ["zips"]
            con.close()
        finally:
            os.chmod(p_zipdir, stat.S_IRWXU)


if __name__ == "__main__":
    test_zipdbfs2spatialite()
    test_zipshps2spatialite_readonly_dir()
    print("all zip import tests passed.")
