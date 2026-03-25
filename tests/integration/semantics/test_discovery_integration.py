"""Integration tests for discover_schema_for_question against a real AIDB instance.

These tests require a running PostgreSQL instance with the AIDB extension.
Set AIDB_TEST_DSN to the connection string, e.g.:
    export AIDB_TEST_DSN="postgresql://bilge.ince@localhost:28816/aidb"

The tests reuse the same test schema/model/KB pattern from the other integration
tests. They create a KB and aliases, then verify that discover_schema_for_question
returns both schema metadata and alias results in a single call.
"""

import logging
import os

import pytest
import pytest_asyncio

from pg_airman_mcp.semantics.discovery_tools import discover_schema_for_question
from pg_airman_mcp.semantics.semantic_alias_tools import create_semantic_alias
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
    """Set up a test schema, model, semantic KB, and aliases.

    Creates:
    - A dummy embedding model
    - A test schema with customers and orders tables (with comments)
    - A semantic KB in Live mode
    - Two semantic aliases for discovery
    - Sample data for execute tests
    - Yields KB metadata
    - Cleans up everything afterwards
    """
    kb_name = "mcp_discovery_test_skb"
    schema_name = "mcp_discovery_test_schema"
    model_name = "mcp_discovery_test_model"

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
        (
            f"COMMENT ON COLUMN {schema_name}.customers.email IS "
            "'Customer email address'"
        ),
        f"""CREATE TABLE {schema_name}.orders (
            id SERIAL PRIMARY KEY,
            customer_id INTEGER REFERENCES {schema_name}.customers(id),
            total_amount NUMERIC(10, 2) NOT NULL,
            status VARCHAR(50) DEFAULT 'pending'
        )""",
        f"COMMENT ON TABLE {schema_name}.orders IS 'Customer orders table'",
        f"""SELECT aidb.create_semantic_kb(
            '{kb_name}', '{model_name}',
            ARRAY['{schema_name}'], 'Live', FALSE, NULL
        )""",
        f"""INSERT INTO {schema_name}.customers
            (email, first_name, last_name)
            VALUES
            ('alice@example.com', 'Alice', 'Smith'),
            ('bob@example.com', 'Bob', 'Jones')""",
        f"""INSERT INTO {schema_name}.orders
            (customer_id, total_amount, status)
            VALUES
            (1, 100.00, 'completed'),
            (1, 50.00, 'pending'),
            (2, 200.00, 'completed')""",
    ]

    for sql in setup_sqls:
        await sql_driver.execute_query(sql)

    # Create aliases that the discovery tool should find
    await create_semantic_alias(
        sql_driver,
        kb_name=kb_name,
        name="mcp_discovery_test_revenue",
        description="Calculate total revenue from completed orders",
        query_text=(
            f"SELECT SUM(total_amount) AS total_revenue "
            f"FROM {schema_name}.orders WHERE status = 'completed'"
        ),
    )
    await create_semantic_alias(
        sql_driver,
        kb_name=kb_name,
        name="mcp_discovery_test_customers",
        description="List all customers with their email addresses",
        query_text=(
            f"SELECT first_name, last_name, email "
            f"FROM {schema_name}.customers ORDER BY last_name"
        ),
    )

    logger.info(f"Created test semantic KB '{kb_name}' with aliases")
    yield {
        "kb_name": kb_name,
        "schema_name": schema_name,
        "model_name": model_name,
    }

    # Teardown: delete aliases first, then KB, then schema
    alias_cleanup = [
        "mcp_discovery_test_revenue",
        "mcp_discovery_test_customers",
    ]
    for alias_name in alias_cleanup:
        try:
            await sql_driver.execute_query(
                f"SELECT aidb.delete_semantic_alias('{alias_name}')"
            )
        except Exception:
            pass

    teardown_sqls = [
        f"SELECT aidb.delete_semantic_kb('{kb_name}')",
        f"DROP TABLE IF EXISTS {schema_name}.orders",
        f"DROP TABLE IF EXISTS {schema_name}.customers",
        f"DROP SCHEMA IF EXISTS {schema_name}",
    ]
    for sql in teardown_sqls:
        try:
            await sql_driver.execute_query(sql)
        except Exception as e:
            logger.warning(f"Teardown error (non-fatal): {e}")

    logger.info("Cleaned up discovery test KB, aliases, and schema")


# ============================================
# discover_schema_for_question - combined results
# ============================================


@pytest.mark.asyncio
async def test_discover_returns_schema_and_aliases(sql_driver, semantic_kb):
    """Test that discovery returns both schema metadata and aliases."""
    results = await discover_schema_for_question(
        sql_driver,
        kb_name=semantic_kb["kb_name"],
        question="customer orders revenue",
        min_similarity=0.0,
        max_results=50,
    )

    assert "schema_metadata" in results
    assert "existing_aliases" in results
    assert len(results["schema_metadata"]) > 0
    assert len(results["existing_aliases"]) > 0


@pytest.mark.asyncio
async def test_discover_schema_metadata_has_expected_fields(sql_driver, semantic_kb):
    """Test that schema metadata entries have the expected structure."""
    results = await discover_schema_for_question(
        sql_driver,
        kb_name=semantic_kb["kb_name"],
        question="customer email",
        min_similarity=0.0,
        max_results=50,
    )

    assert len(results["schema_metadata"]) > 0
    entry = results["schema_metadata"][0]
    assert "schema_name" in entry
    assert "relation_name" in entry
    assert "entity_type" in entry
    assert "definition" in entry
    assert "similarity" in entry


@pytest.mark.asyncio
async def test_discover_alias_results_have_expected_fields(sql_driver, semantic_kb):
    """Test that alias entries have the expected structure."""
    results = await discover_schema_for_question(
        sql_driver,
        kb_name=semantic_kb["kb_name"],
        question="revenue",
        min_similarity=0.0,
        max_results=50,
    )

    assert len(results["existing_aliases"]) > 0
    alias = results["existing_aliases"][0]
    assert "name" in alias
    assert "query_text" in alias
    assert "similarity" in alias


@pytest.mark.asyncio
async def test_discover_finds_relevant_tables(sql_driver, semantic_kb):
    """Test that discovery finds tables relevant to the question."""
    results = await discover_schema_for_question(
        sql_driver,
        kb_name=semantic_kb["kb_name"],
        question="orders",
        min_similarity=0.0,
        max_results=50,
    )

    relation_names = [
        r["relation_name"]
        for r in results["schema_metadata"]
        if r["entity_type"] == "Table"
    ]
    assert "orders" in relation_names


@pytest.mark.asyncio
async def test_discover_finds_relevant_aliases(sql_driver, semantic_kb):
    """Test that discovery finds aliases relevant to the question."""
    results = await discover_schema_for_question(
        sql_driver,
        kb_name=semantic_kb["kb_name"],
        question="revenue",
        min_similarity=0.0,
        max_results=50,
    )

    alias_names = [a["name"] for a in results["existing_aliases"]]
    assert "mcp_discovery_test_revenue" in alias_names


# ============================================
# discover_schema_for_question - include_aliases=False
# ============================================


@pytest.mark.asyncio
async def test_discover_without_aliases(sql_driver, semantic_kb):
    """Test that include_aliases=False skips alias search."""
    results = await discover_schema_for_question(
        sql_driver,
        kb_name=semantic_kb["kb_name"],
        question="customer",
        include_aliases=False,
        min_similarity=0.0,
        max_results=50,
    )

    assert len(results["schema_metadata"]) > 0
    assert results["existing_aliases"] == []


# ============================================
# discover_schema_for_question - limits and similarity
# ============================================


@pytest.mark.asyncio
async def test_discover_respects_max_results(sql_driver, semantic_kb):
    """Test that max_results limits results per category."""
    results = await discover_schema_for_question(
        sql_driver,
        kb_name=semantic_kb["kb_name"],
        question="customer",
        min_similarity=0.0,
        max_results=2,
    )

    assert len(results["schema_metadata"]) <= 2
    assert len(results["existing_aliases"]) <= 2


@pytest.mark.asyncio
async def test_discover_high_similarity_filters_results(sql_driver, semantic_kb):
    """Test that a high similarity threshold filters out weak matches."""
    results_low = await discover_schema_for_question(
        sql_driver,
        kb_name=semantic_kb["kb_name"],
        question="customer",
        min_similarity=0.0,
        max_results=50,
    )
    results_high = await discover_schema_for_question(
        sql_driver,
        kb_name=semantic_kb["kb_name"],
        question="customer",
        min_similarity=0.99,
        max_results=50,
    )

    # High threshold should return fewer or equal results
    assert len(results_high["schema_metadata"]) <= len(results_low["schema_metadata"])


# ============================================
# SafeSqlDriver (restricted mode)
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
async def test_discover_works_in_restricted_mode(safe_driver, semantic_kb):
    """discover_schema_for_question works through SafeSqlDriver (read-only)."""
    results = await discover_schema_for_question(
        safe_driver,
        kb_name=semantic_kb["kb_name"],
        question="customer orders",
        min_similarity=0.0,
        max_results=50,
    )

    assert "schema_metadata" in results
    assert "existing_aliases" in results
    assert isinstance(results["schema_metadata"], list)
    assert isinstance(results["existing_aliases"], list)


@pytest.mark.asyncio
async def test_discover_without_aliases_restricted(safe_driver, semantic_kb):
    """discover_schema_for_question with include_aliases=False in restricted mode."""
    results = await discover_schema_for_question(
        safe_driver,
        kb_name=semantic_kb["kb_name"],
        question="orders",
        include_aliases=False,
        min_similarity=0.0,
        max_results=50,
    )

    assert len(results["schema_metadata"]) > 0
    assert results["existing_aliases"] == []
