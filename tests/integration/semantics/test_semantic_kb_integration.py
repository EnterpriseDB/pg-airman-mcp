"""Integration tests for semantic KB tools against a real AIDB instance.

These tests require a running PostgreSQL instance with the AIDB extension.
Set AIDB_TEST_DSN to the connection string, e.g.:
    export AIDB_TEST_DSN="postgresql://postgres@localhost:28816/aidb"

The tests create their own schema, model, and semantic KB, then clean up
after themselves. They mirror the patterns in the AIDB regression tests
at aidb/tests/pg_regress/sql/semantic_kb_creation.sql.
"""

import logging
import os

import psycopg.errors
import pytest
import pytest_asyncio

from pg_airman_mcp.semantics.semantic_kb_tools import (
    get_semantic_kb_stats,
    list_semantic_kbs,
    refresh_semantic_kb,
    search_semantic_kb,
)
from pg_airman_mcp.sql import SafeSqlDriver, SqlDriver

logger = logging.getLogger(__name__)

AIDB_TEST_DSN = os.environ.get(
    "AIDB_TEST_DSN", "postgresql://postgres@localhost:28816/aidb"
)

# Skip entire module if AIDB is not reachable
pytestmark = pytest.mark.skipif(
    os.environ.get("SKIP_AIDB_TESTS", "1") == "1",
    reason="AIDB integration tests disabled (set SKIP_AIDB_TESTS=0 to run)",
)


@pytest_asyncio.fixture
async def sql_driver():
    """Create a SqlDriver connected to the AIDB instance."""
    driver = SqlDriver(engine_url=AIDB_TEST_DSN)
    driver.connect()
    try:
        yield driver
    finally:
        if hasattr(driver, "conn") and driver.conn is not None:
            await driver.conn.close()


@pytest_asyncio.fixture
async def semantic_kb(sql_driver):
    """Set up a test schema, model, and semantic KB for the test module.

    Mirrors the setup from semantic_kb_creation.sql:
    - Creates a dummy model
    - Creates a test schema with customers, orders, and a view
    - Creates a semantic KB in Live mode
    - Yields the KB name
    - Cleans up everything afterwards
    """
    kb_name = "mcp_test_skb"
    schema_name = "mcp_test_schema"
    model_name = "mcp_test_model"

    # Setup: create model, schema, tables, comments, KB
    setup_sqls = [
        f"SELECT aidb.create_model('{model_name}', 'dummy')",
        f"CREATE SCHEMA IF NOT EXISTS {schema_name}",
        f"""CREATE TABLE {schema_name}.customers (
            id SERIAL PRIMARY KEY,
            email VARCHAR(255) NOT NULL,
            first_name VARCHAR(100),
            last_name VARCHAR(100)
        )""",
        f"COMMENT ON TABLE {schema_name}.customers IS 'Customer information table'",
        f"COMMENT ON COLUMN {schema_name}.customers.email IS 'Customer email address'",
        f"""CREATE TABLE {schema_name}.orders (
            id SERIAL PRIMARY KEY,
            customer_id INTEGER REFERENCES {schema_name}.customers(id),
            total_amount NUMERIC(10, 2) NOT NULL,
            status VARCHAR(50) DEFAULT 'pending'
        )""",
        f"COMMENT ON TABLE {schema_name}.orders IS 'Customer orders table'",
        f"""CREATE VIEW {schema_name}.customer_orders AS
            SELECT c.id, c.email, COUNT(o.id) as order_count
            FROM {schema_name}.customers c
            LEFT JOIN {schema_name}.orders o ON c.id = o.customer_id
            GROUP BY c.id, c.email""",
        f"COMMENT ON VIEW {schema_name}.customer_orders IS 'Customer order summary'",
        f"""SELECT aidb.create_semantic_kb(
            '{kb_name}', '{model_name}',
            ARRAY['{schema_name}'], 'Live', FALSE, NULL
        )""",
    ]

    for sql in setup_sqls:
        await sql_driver.execute_query(sql)

    logger.info(f"Created test semantic KB '{kb_name}' in schema '{schema_name}'")
    yield {"kb_name": kb_name, "schema_name": schema_name, "model_name": model_name}

    # Teardown
    teardown_sqls = [
        f"SELECT aidb.delete_semantic_kb('{kb_name}')",
        f"DROP VIEW IF EXISTS {schema_name}.customer_orders",
        f"DROP TABLE IF EXISTS {schema_name}.orders",
        f"DROP TABLE IF EXISTS {schema_name}.customers",
        f"DROP SCHEMA IF EXISTS {schema_name}",
    ]
    for sql in teardown_sqls:
        try:
            await sql_driver.execute_query(sql)
        except Exception as e:
            logger.warning(f"Teardown error (non-fatal): {e}")

    logger.info("Cleaned up test semantic KB and schema")


# ============================================
# list_semantic_kbs
# ============================================


@pytest.mark.asyncio
async def test_list_semantic_kbs(sql_driver, semantic_kb):
    """Test that our test KB appears in the list."""
    results = await list_semantic_kbs(sql_driver)

    assert len(results) >= 1
    kb_names = [r["name"] for r in results]
    assert semantic_kb["kb_name"] in kb_names

    # Check returned fields
    our_kb = next(r for r in results if r["name"] == semantic_kb["kb_name"])
    assert our_kb["model_name"] == semantic_kb["model_name"]
    assert semantic_kb["schema_name"] in our_kb["schemas"]


# ============================================
# get_semantic_kb_stats
# ============================================


@pytest.mark.asyncio
async def test_get_semantic_kb_stats(sql_driver, semantic_kb):
    """Test KB stats match expected entity counts.

    With customers (5 cols), orders (4 cols), customer_orders view (3 cols):
    - 2 tables + 1 view = 3 relations
    - 5 + 4 + 3 = 12 columns
    - total = 3 + 12 = 15
    """
    result = await get_semantic_kb_stats(sql_driver, semantic_kb["kb_name"])

    assert result["tables"] == 2
    assert result["views"] == 1
    assert result["columns"] >= 10  # at least 10 columns across all relations
    assert result["total"] == result["tables"] + result["views"] + result["columns"]
    assert result["pending"] == 0


@pytest.mark.asyncio
async def test_get_semantic_kb_stats_nonexistent(sql_driver, semantic_kb):
    """Test stats for a nonexistent KB raises an error.

    AIDB raises UndefinedTable when the KB's internal metadata table
    doesn't exist, rather than returning empty results.
    """
    with pytest.raises(psycopg.errors.UndefinedTable):
        await get_semantic_kb_stats(sql_driver, "nonexistent_kb_xyz")


# ============================================
# search_semantic_kb (no entity_types filter)
# ============================================


@pytest.mark.asyncio
async def test_search_semantic_kb_all_entities(sql_driver, semantic_kb):
    """Test unfiltered search returns tables, views, and columns."""
    results = await search_semantic_kb(
        sql_driver,
        semantic_kb["kb_name"],
        query="",
        min_similarity=0.0,
        limit=100,
    )

    assert len(results) > 0

    entity_types = {r["entity_type"] for r in results}
    assert "Table" in entity_types
    assert "Column" in entity_types

    # All results should belong to our test schema
    for r in results:
        assert r["schema_name"] == semantic_kb["schema_name"]


@pytest.mark.asyncio
async def test_search_semantic_kb_returns_column_name(sql_driver, semantic_kb):
    """Test that column_name is populated for Column entities."""
    results = await search_semantic_kb(
        sql_driver,
        semantic_kb["kb_name"],
        query="",
        min_similarity=0.0,
        limit=100,
    )

    columns = [r for r in results if r["entity_type"] == "Column"]
    assert len(columns) > 0

    for col in columns:
        assert col["column_name"] is not None


# ============================================
# search_semantic_kb with entity_types filter
# ============================================


@pytest.mark.asyncio
async def test_search_semantic_kb_tables_only(sql_driver, semantic_kb):
    """Test filtering by Table entity type uses get_entity_definitions."""
    results = await search_semantic_kb(
        sql_driver,
        semantic_kb["kb_name"],
        query="",
        min_similarity=0.0,
        entity_types=["Table"],
        limit=100,
    )

    assert len(results) == 2  # customers, orders
    for r in results:
        assert r["entity_type"] == "Table"

    table_names = {r["relation_name"] for r in results}
    assert "customers" in table_names
    assert "orders" in table_names


@pytest.mark.asyncio
async def test_search_semantic_kb_column_filter_fallback(sql_driver, semantic_kb):
    """Test that Column in entity_types falls back to get_metadata with filtering."""
    results = await search_semantic_kb(
        sql_driver,
        semantic_kb["kb_name"],
        query="",
        min_similarity=0.0,
        entity_types=["Column"],
        limit=100,
    )

    assert len(results) > 0
    for r in results:
        assert r["entity_type"] == "Column"
        assert r["column_name"] is not None


@pytest.mark.asyncio
async def test_search_semantic_kb_mixed_filter(sql_driver, semantic_kb):
    """Test filtering by Column + Table uses get_metadata fallback."""
    results = await search_semantic_kb(
        sql_driver,
        semantic_kb["kb_name"],
        query="",
        min_similarity=0.0,
        entity_types=["Column", "Table"],
        limit=100,
    )

    entity_types = {r["entity_type"] for r in results}
    assert entity_types <= {"Column", "Table"}
    assert "Column" in entity_types
    assert "Table" in entity_types

    # Column results should have real column_name
    columns = [r for r in results if r["entity_type"] == "Column"]
    for col in columns:
        assert col["column_name"] is not None


@pytest.mark.asyncio
async def test_search_semantic_kb_limit(sql_driver, semantic_kb):
    """Test that limit is respected."""
    results = await search_semantic_kb(
        sql_driver,
        semantic_kb["kb_name"],
        query="",
        min_similarity=0.0,
        limit=3,
    )

    assert len(results) <= 3


# ============================================
# refresh_semantic_kb
# ============================================


@pytest.mark.asyncio
async def test_refresh_semantic_kb(sql_driver, semantic_kb):
    """Test refreshing the semantic KB."""
    result = await refresh_semantic_kb(sql_driver, semantic_kb["kb_name"])

    assert "Refreshed" in result
    assert semantic_kb["kb_name"] in result

    # Stats should still be valid after refresh
    stats = await get_semantic_kb_stats(sql_driver, semantic_kb["kb_name"])
    assert stats["total"] > 0
    assert stats["pending"] == 0


# ============================================
# SafeSqlDriver (restricted mode) integration tests
# Verifies that %s placeholder normalization works
# end-to-end against a real AIDB instance.
# ============================================


@pytest_asyncio.fixture
async def safe_driver():
    """Create a SafeSqlDriver connected to the AIDB instance."""
    base = SqlDriver(engine_url=AIDB_TEST_DSN)
    base.connect()
    driver = SafeSqlDriver(sql_driver=base, timeout=30)
    try:
        yield driver
    finally:
        if hasattr(base, "conn") and base.conn is not None:
            await base.conn.close()


@pytest.mark.asyncio
async def test_list_kbs_restricted(safe_driver, semantic_kb):
    """list_semantic_kbs works through SafeSqlDriver."""
    results = await list_semantic_kbs(safe_driver)

    assert len(results) >= 1
    kb_names = [r["name"] for r in results]
    assert semantic_kb["kb_name"] in kb_names


@pytest.mark.asyncio
async def test_get_kb_stats_restricted(safe_driver, semantic_kb):
    """get_semantic_kb_stats works through SafeSqlDriver."""
    result = await get_semantic_kb_stats(safe_driver, semantic_kb["kb_name"])

    assert result["tables"] == 2
    assert result["views"] == 1
    assert result["columns"] >= 10
    assert result["total"] > 0
    assert result["pending"] == 0


@pytest.mark.asyncio
async def test_search_kb_no_filter_restricted(safe_driver, semantic_kb):
    """search_semantic_kb (no filter) works through SafeSqlDriver."""
    results = await search_semantic_kb(
        safe_driver,
        semantic_kb["kb_name"],
        query="",
        min_similarity=0.0,
        limit=100,
    )

    assert len(results) > 0
    entity_types = {r["entity_type"] for r in results}
    assert "Table" in entity_types
    assert "Column" in entity_types


@pytest.mark.asyncio
async def test_search_kb_table_filter_restricted(safe_driver, semantic_kb):
    """search_semantic_kb with Table filter works through SafeSqlDriver."""
    results = await search_semantic_kb(
        safe_driver,
        semantic_kb["kb_name"],
        query="",
        min_similarity=0.0,
        entity_types=["Table"],
        limit=100,
    )

    assert len(results) == 2
    for r in results:
        assert r["entity_type"] == "Table"


@pytest.mark.asyncio
async def test_search_kb_column_filter_restricted(safe_driver, semantic_kb):
    """search_semantic_kb with Column filter works through SafeSqlDriver."""
    results = await search_semantic_kb(
        safe_driver,
        semantic_kb["kb_name"],
        query="",
        min_similarity=0.0,
        entity_types=["Column"],
        limit=100,
    )

    assert len(results) > 0
    for r in results:
        assert r["entity_type"] == "Column"
        assert r["column_name"] is not None


@pytest.mark.asyncio
async def test_refresh_kb_restricted_is_rejected(safe_driver, semantic_kb):
    """refresh_semantic_kb is a write op and fails in restricted mode.

    SafeSqlDriver forces read-only transactions, so AIDB's internal
    TRUNCATE during refresh is correctly rejected.
    """
    with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
        await refresh_semantic_kb(safe_driver, semantic_kb["kb_name"])
