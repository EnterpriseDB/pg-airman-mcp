import pytest

from pg_airman_mcp.sql import SqlDriver, execute_comment_on


class FakeSqlDriver(SqlDriver):
    def __init__(self):  # type: ignore[override]
        # Bypass normal initialization; provide minimal attributes
        self.conn = object()
        self.is_pool = False
        self.calls = []

    async def execute_query(self, query, params=None, force_readonly=False):  # type: ignore[override]
        self.calls.append((query, params, force_readonly))
        return None


@pytest.mark.asyncio
async def test_execute_comment_on_table_basic():
    # happy path
    driver = FakeSqlDriver()
    await execute_comment_on(driver, "TABLE", ["public", "my_table"], "hello")
    assert len(driver.calls) == 1
    query, params, force_readonly = driver.calls[0]
    assert query == 'COMMENT ON TABLE "public"."my_table" IS \'hello\''
    assert params is None
    assert force_readonly is False


@pytest.mark.asyncio
async def test_execute_comment_on_identifier_quoting():
    driver = FakeSqlDriver()
    await execute_comment_on(driver, "TABLE", ['sch"ema', 'tab"le'], "cmt")
    query, params, _ = driver.calls[0]
    # Internal quotes should be doubled
    assert '"sch""ema"' in query and '"tab""le"' in query
    assert "'cmt'" in query
    assert params is None


@pytest.mark.asyncio
async def test_execute_comment_on_invalid_kind():
    driver = FakeSqlDriver()
    with pytest.raises(ValueError):
        await execute_comment_on(driver, "INDEX", ["public", "t"], "oops")


@pytest.mark.asyncio
async def test_execute_comment_on_empty_identifier():
    driver = FakeSqlDriver()
    with pytest.raises(ValueError):
        await execute_comment_on(driver, "TABLE", [""], "oops")


@pytest.mark.asyncio
async def test_execute_comment_on_length_cap():
    driver = FakeSqlDriver()
    long_comment = "x" * 6000
    with pytest.raises(ValueError):
        await execute_comment_on(driver, "TABLE", ["public", "t"], long_comment)


@pytest.mark.asyncio
async def test_execute_comment_on_single_quote_escaping():
    driver = FakeSqlDriver()
    comment_text = "O'Reilly"
    await execute_comment_on(driver, "TABLE", ["public", "books"], comment_text)
    assert len(driver.calls) == 1
    query, params, _ = driver.calls[0]
    # Expect doubled internal single quote
    assert "COMMENT ON TABLE \"public\".\"books\" IS 'O''Reilly'" == query
    assert params is None
