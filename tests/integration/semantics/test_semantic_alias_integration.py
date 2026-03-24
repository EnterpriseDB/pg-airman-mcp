"""Integration tests for semantic alias tools against a real AIDB instance.

These tests require a running PostgreSQL instance with the AIDB extension.
Set AIDB_TEST_DSN to the connection string, e.g.:
    export AIDB_TEST_DSN="postgresql://postgres@localhost:28816/aidb"

The tests reuse the same test schema/model/KB pattern from the KB integration
tests. They create aliases, search for them, execute them, and clean up.
"""

import logging
import os

import psycopg
import pytest
import pytest_asyncio

from pg_airman_mcp.semantics.semantic_alias_tools import (
    create_semantic_alias,
    delete_semantic_alias,
    execute_semantic_alias,
    list_semantic_aliases,
    search_semantic_aliases,
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
    """Set up a test schema, model, and semantic KB.

    Creates:
    - A dummy embedding model
    - A test schema with customers and orders tables
    - A semantic KB in Live mode
    - Yields KB metadata
    - Cleans up everything afterwards
    """
    kb_name = "mcp_alias_test_skb"
    schema_name = "mcp_alias_test_schema"
    model_name = "mcp_alias_test_model"

    setup_sqls = [
        f"SELECT aidb.create_model('{model_name}', 'dummy')",
        f"CREATE SCHEMA IF NOT EXISTS {schema_name}",
        f"""CREATE TABLE {schema_name}.customers (
            id SERIAL PRIMARY KEY,
            email VARCHAR(255) NOT NULL,
            first_name VARCHAR(100),
            last_name VARCHAR(100)
        )""",
        (f"COMMENT ON TABLE {schema_name}.customers IS 'Customer information table'"),
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
        (f"COMMENT ON TABLE {schema_name}.orders IS 'Customer orders table'"),
        f"""SELECT aidb.create_semantic_kb(
            '{kb_name}', '{model_name}',
            ARRAY['{schema_name}'], 'Live', FALSE, NULL
        )""",
        # Insert sample data for execute tests
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

    logger.info(f"Created test semantic KB '{kb_name}' in schema '{schema_name}'")
    yield {
        "kb_name": kb_name,
        "schema_name": schema_name,
        "model_name": model_name,
    }

    # Teardown: delete aliases first, then KB, then schema
    alias_cleanup = [
        "mcp_test_customer_by_email",
        "mcp_test_order_totals",
        "mcp_test_all_customers",
    ]
    for alias_name in alias_cleanup:
        try:
            await sql_driver.execute_query(
                f"SELECT aidb.delete_semantic_alias('{alias_name}')"
            )
        except Exception:
            pass  # alias may not exist

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

    logger.info("Cleaned up test semantic KB, aliases, and schema")


# ============================================
# create_semantic_alias
# ============================================


@pytest.mark.asyncio
async def test_create_semantic_alias_basic(sql_driver, semantic_kb):
    """Test creating a simple alias without parameters."""
    result = await create_semantic_alias(
        sql_driver,
        kb_name=semantic_kb["kb_name"],
        name="mcp_test_all_customers",
        description="List all customers in the system",
        query_text=(
            f"SELECT id, email, first_name, last_name "
            f"FROM {semantic_kb['schema_name']}.customers"
        ),
    )

    assert "Created" in result
    assert "mcp_test_all_customers" in result


@pytest.mark.asyncio
async def test_create_semantic_alias_with_parameters(sql_driver, semantic_kb):
    """Test creating an alias with typed parameters."""
    result = await create_semantic_alias(
        sql_driver,
        kb_name=semantic_kb["kb_name"],
        name="mcp_test_customer_by_email",
        description="Find a customer by their email address",
        query_text=(
            f"SELECT * FROM {semantic_kb['schema_name']}.customers "
            "WHERE email = ${email}"
        ),
        parameters=[
            {
                "name": "email",
                "param_type": "text",
                "description": "Customer email to search for",
            }
        ],
    )

    assert "Created" in result
    assert "mcp_test_customer_by_email" in result


@pytest.mark.asyncio
async def test_create_semantic_alias_duplicate_fails(sql_driver, semantic_kb):
    """Test that creating a duplicate alias raises an error."""
    # Create first
    await create_semantic_alias(
        sql_driver,
        kb_name=semantic_kb["kb_name"],
        name="mcp_test_order_totals",
        description="Total order amounts by customer",
        query_text=(
            f"SELECT customer_id, SUM(total_amount) "
            f"FROM {semantic_kb['schema_name']}.orders "
            "GROUP BY customer_id"
        ),
    )

    # Duplicate should fail
    with pytest.raises(psycopg.Error):
        await create_semantic_alias(
            sql_driver,
            kb_name=semantic_kb["kb_name"],
            name="mcp_test_order_totals",
            description="Duplicate",
            query_text="SELECT 1",
        )


# ============================================
# list_semantic_aliases
# ============================================


@pytest.mark.asyncio
async def test_list_semantic_aliases(sql_driver, semantic_kb):
    """Test listing aliases after creation."""
    # Create an alias first
    await create_semantic_alias(
        sql_driver,
        kb_name=semantic_kb["kb_name"],
        name="mcp_test_all_customers",
        description="List all customers",
        query_text=(f"SELECT * FROM {semantic_kb['schema_name']}.customers"),
    )

    results = await list_semantic_aliases(sql_driver)

    alias_names = [r["name"] for r in results]
    assert "mcp_test_all_customers" in alias_names


# ============================================
# search_semantic_aliases
# ============================================


@pytest.mark.asyncio
async def test_search_semantic_aliases_finds_match(sql_driver, semantic_kb):
    """Test that searching for aliases returns relevant results."""
    # Create an alias
    await create_semantic_alias(
        sql_driver,
        kb_name=semantic_kb["kb_name"],
        name="mcp_test_customer_by_email",
        description="Find a customer by their email address",
        query_text=(
            f"SELECT * FROM {semantic_kb['schema_name']}.customers "
            "WHERE email = ${email}"
        ),
        parameters=[
            {
                "name": "email",
                "param_type": "text",
                "description": "Customer email to search for",
            }
        ],
    )

    results = await search_semantic_aliases(
        sql_driver,
        semantic_kb["kb_name"],
        query="find customer by email",
        min_similarity=0.0,
        limit=10,
    )

    assert len(results) > 0
    alias_names = [r["name"] for r in results]
    assert "mcp_test_customer_by_email" in alias_names


@pytest.mark.asyncio
async def test_search_semantic_aliases_no_results(sql_driver, semantic_kb):
    """Test search with high similarity threshold returns no results."""
    results = await search_semantic_aliases(
        sql_driver,
        semantic_kb["kb_name"],
        query="completely unrelated xyz quantum physics",
        min_similarity=0.99,
        limit=10,
    )

    # With a very high threshold and unrelated query, expect no matches
    assert len(results) == 0


# ============================================
# search diagnostic (bug reproduction)
# ============================================


@pytest.mark.asyncio
async def test_create_alias_stores_model_and_vector(sql_driver, semantic_kb):
    """Verify that create_semantic_alias stores model_name and description_vector.

    Bug reproduction: search_semantic_aliases returns no results even with
    min_similarity=0.0. This test checks the internal state of the alias
    table to diagnose whether the model/vector were stored correctly.
    """
    await create_semantic_alias(
        sql_driver,
        kb_name=semantic_kb["kb_name"],
        name="mcp_test_all_customers",
        description="List all customers in the system",
        query_text=(f"SELECT id, email FROM {semantic_kb['schema_name']}.customers"),
    )

    # Check internal state: model_name and description_vector
    rows = await sql_driver.execute_query(
        """
        SELECT name, model_name,
               description_vector IS NOT NULL AS has_vector
        FROM aidb_internal.semantic_aliases
        WHERE name = %s
        """,
        ["mcp_test_all_customers"],
    )

    assert rows, "Alias was not found in aidb_internal.semantic_aliases"
    alias = rows[0].cells
    assert alias["model_name"] == semantic_kb["model_name"], (
        f"Expected model_name='{semantic_kb['model_name']}', "
        f"got '{alias['model_name']}'"
    )
    assert alias["has_vector"] is True, (
        "description_vector is NULL — alias will be invisible to search"
    )


@pytest.mark.asyncio
async def test_search_finds_alias_after_create(sql_driver, semantic_kb):
    """End-to-end: create an alias then search for it immediately.

    Bug reproduction: search_semantic_aliases returns empty even with
    min_similarity=0.0 right after creating an alias.
    """
    await create_semantic_alias(
        sql_driver,
        kb_name=semantic_kb["kb_name"],
        name="mcp_test_all_customers",
        description="List all customers in the system",
        query_text=(f"SELECT id, email FROM {semantic_kb['schema_name']}.customers"),
    )

    results = await search_semantic_aliases(
        sql_driver,
        semantic_kb["kb_name"],
        query="list all customers",
        min_similarity=0.0,
        limit=100,
    )

    assert len(results) > 0, (
        "search_semantic_aliases returned no results with min_similarity=0.0 "
        "immediately after create_semantic_alias"
    )
    alias_names = [r["name"] for r in results]
    assert "mcp_test_all_customers" in alias_names


# ============================================
# execute_semantic_alias
# ============================================


@pytest.mark.asyncio
async def test_execute_semantic_alias_no_params(sql_driver, semantic_kb):
    """Test executing an alias without parameters."""
    await create_semantic_alias(
        sql_driver,
        kb_name=semantic_kb["kb_name"],
        name="mcp_test_all_customers",
        description="List all customers",
        query_text=(
            f"SELECT id, email FROM {semantic_kb['schema_name']}.customers ORDER BY id"
        ),
    )

    results = await execute_semantic_alias(sql_driver, "mcp_test_all_customers")

    assert len(results) == 2
    assert results[0]["email"] == "alice@example.com"
    assert results[1]["email"] == "bob@example.com"


@pytest.mark.asyncio
async def test_execute_semantic_alias_with_params(sql_driver, semantic_kb):
    """Test executing an alias with parameter substitution."""
    await create_semantic_alias(
        sql_driver,
        kb_name=semantic_kb["kb_name"],
        name="mcp_test_customer_by_email",
        description="Find customer by email",
        query_text=(
            f"SELECT id, email, first_name FROM "
            f"{semantic_kb['schema_name']}.customers "
            "WHERE email = ${email}"
        ),
        parameters=[
            {
                "name": "email",
                "param_type": "text",
                "description": "Email",
            }
        ],
    )

    results = await execute_semantic_alias(
        sql_driver,
        "mcp_test_customer_by_email",
        parameters={"email": "alice@example.com"},
    )

    assert len(results) == 1
    assert results[0]["first_name"] == "Alice"


@pytest.mark.asyncio
async def test_execute_semantic_alias_nonexistent(sql_driver, semantic_kb):
    """Test executing a nonexistent alias raises an error."""
    with pytest.raises(psycopg.Error):
        await execute_semantic_alias(sql_driver, "nonexistent_alias_xyz")


# ============================================
# delete_semantic_alias
# ============================================


@pytest.mark.asyncio
async def test_delete_semantic_alias(sql_driver, semantic_kb):
    """Test deleting an alias removes it from the list."""
    # Create
    await create_semantic_alias(
        sql_driver,
        kb_name=semantic_kb["kb_name"],
        name="mcp_test_all_customers",
        description="List all customers",
        query_text=(f"SELECT * FROM {semantic_kb['schema_name']}.customers"),
    )

    # Verify it exists
    aliases = await list_semantic_aliases(sql_driver)
    alias_names = [a["name"] for a in aliases]
    assert "mcp_test_all_customers" in alias_names

    # Delete
    result = await delete_semantic_alias(sql_driver, "mcp_test_all_customers")
    assert "Deleted" in result

    # Verify it's gone
    aliases = await list_semantic_aliases(sql_driver)
    alias_names = [a["name"] for a in aliases]
    assert "mcp_test_all_customers" not in alias_names


# ============================================
# SafeSqlDriver (restricted mode) tests
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
async def test_search_aliases_restricted(safe_driver, semantic_kb):
    """search_semantic_aliases works through SafeSqlDriver."""
    results = await search_semantic_aliases(
        safe_driver,
        semantic_kb["kb_name"],
        query="customer",
        min_similarity=0.0,
        limit=10,
    )

    # May or may not have results depending on aliases created,
    # but the call itself should not fail
    assert isinstance(results, list)


@pytest.mark.asyncio
async def test_list_aliases_restricted(safe_driver, semantic_kb):
    """list_semantic_aliases works through SafeSqlDriver."""
    results = await list_semantic_aliases(safe_driver)

    assert isinstance(results, list)


@pytest.mark.asyncio
async def test_create_alias_restricted_blocked_by_default(safe_driver, semantic_kb):
    """create_semantic_alias fails in restricted mode (flag=false)."""
    with pytest.raises(ValueError, match="not permitted in restricted mode"):
        await create_semantic_alias(
            safe_driver,
            kb_name=semantic_kb["kb_name"],
            name="should_fail",
            description="Should not be created",
            query_text="SELECT 1",
        )


@pytest.mark.asyncio
async def test_delete_alias_restricted_blocked_by_default(safe_driver, semantic_kb):
    """delete_semantic_alias fails in restricted mode (flag=false)."""
    with pytest.raises(ValueError, match="not permitted in restricted mode"):
        await delete_semantic_alias(safe_driver, "should_fail")
