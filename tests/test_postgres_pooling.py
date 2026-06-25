from __future__ import annotations

from worker_health.pool_classifier_web.storage import PostgresStorage


class FakeCursor:
    def __init__(self, row=None):
        self.row = row or {"cnt": 0}
        self.statements = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.statements.append((sql, params))

    def fetchone(self):
        return self.row


class FakeConnection:
    def __init__(self):
        self.commits = 0
        self.rollbacks = 0
        self.cursor_count = 0

    def cursor(self):
        self.cursor_count += 1
        return FakeCursor({"cnt": 2})

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class FakeConnectionContext:
    def __init__(self, conn):
        self.conn = conn
        self.exits = 0

    def __enter__(self):
        return self.conn

    def __exit__(self, exc_type, exc, tb):
        self.exits += 1
        return False


class FakePool:
    def __init__(self):
        self.contexts = []

    def connection(self):
        ctx = FakeConnectionContext(FakeConnection())
        self.contexts.append(ctx)
        return ctx


def _storage(pool):
    storage = PostgresStorage("test/provisioner", "postgresql://example")
    storage._pool = pool
    return storage


def test_read_operations_release_pooled_connection_immediately():
    pool = FakePool()
    storage = _storage(pool)

    assert storage.count_workers() == 2
    assert storage.count_workers() == 2

    assert len(pool.contexts) == 2
    assert [ctx.exits for ctx in pool.contexts] == [1, 1]
    assert storage._tx_conn is None


def test_writes_share_connection_until_commit():
    pool = FakePool()
    storage = _storage(pool)

    storage.upsert_worker("w1", "g1")
    storage.increment_failure("w1", "2026-06-25T00:00:00+00:00", "bad_device")

    assert len(pool.contexts) == 1
    assert pool.contexts[0].exits == 0

    conn = pool.contexts[0].conn
    storage.commit()

    assert conn.commits == 1
    assert pool.contexts[0].exits == 1
    assert storage._tx_conn is None
