"""SQLite transaction lifetime must not defer file cleanup to cyclic GC."""
import sqlite3
import pytest
from transitionbench.store import JobStore

@pytest.mark.parametrize('abort',[False,True])
def test_transaction_closes_connection_and_preserves_commit_or_rollback(tmp_path,abort):
    store=JobStore(tmp_path)
    try:
        with store.connect() as connection:
            connection.execute("INSERT INTO progress(run_id,body) VALUES('lifetime','{}')")
            if abort:raise ValueError('abort transaction')
    except ValueError:
        assert abort
    with pytest.raises(sqlite3.ProgrammingError,match='closed'):
        connection.execute('SELECT 1')
    with store.connect() as check:
        count=check.execute("SELECT COUNT(*) FROM progress WHERE run_id='lifetime'").fetchone()[0]
    assert count==(0 if abort else 1)
