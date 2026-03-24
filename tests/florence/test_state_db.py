from pathlib import Path

from florence.state.db import SQLiteConnectionAdapter, _rewrite_placeholders, _split_sql_script, connect_florence_db


def test_connect_florence_db_uses_sqlite_adapter_for_path(tmp_path):
    conn = connect_florence_db(tmp_path / "florence.db")
    assert isinstance(conn, SQLiteConnectionAdapter)
    cursor = conn.execute("SELECT 1 AS value")
    row = cursor.fetchone()
    assert row is not None
    assert row["value"] == 1
    conn.close()


def test_rewrite_placeholders_converts_sqlite_style_params():
    assert _rewrite_placeholders("SELECT * FROM households WHERE id = ? AND status = ?") == (
        "SELECT * FROM households WHERE id = %s AND status = %s"
    )


def test_split_sql_script_skips_empty_statements():
    statements = list(_split_sql_script("SELECT 1;  ;\nSELECT 2;"))
    assert statements == ["SELECT 1", "SELECT 2"]
