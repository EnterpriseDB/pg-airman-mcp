"""Comment management tools for semantic metadata."""

import logging

from pg_airman_mcp.sql import SafeSqlDriver, SqlDriver, execute_comment_on

logger = logging.getLogger(__name__)


async def remove_object_comment(
    sql_driver: SqlDriver | SafeSqlDriver,
    schema_name: str,
    object_type: str,
    object_name: str,
    column_name: str | None = None,
) -> str:
    """Remove the comment from a database object by setting it to NULL.

    Args:
        sql_driver: SqlDriver or SafeSqlDriver instance.
        schema_name: Schema containing the object.
        object_type: Type of object: 'table', 'view', or 'column'.
        object_name: Name of the table or view.
        column_name: Column name (required when object_type is 'column').

    Returns:
        Success message indicating the comment was removed.

    Raises:
        ValueError: If object_type is invalid or column_name missing for column type.
    """
    allowed_types = {"table": "TABLE", "view": "VIEW", "column": "COLUMN"}
    kind = allowed_types.get(object_type.lower())
    if not kind:
        raise ValueError(
            f"Unsupported object type '{object_type}'. "
            "Use 'table', 'view', or 'column'."
        )

    if kind == "COLUMN":
        if not column_name:
            raise ValueError("column_name is required when object_type is 'column'")
        identifiers = [schema_name, object_name, column_name]
        display_name = f"{schema_name}.{object_name}.{column_name}"
    else:
        identifiers = [schema_name, object_name]
        display_name = f"{schema_name}.{object_name}"

    # Pass None to execute_comment_on to generate COMMENT ON ... IS NULL
    await execute_comment_on(sql_driver, kind, identifiers, None)

    logger.info(f"Removed comment from {object_type} '{display_name}'")
    return f"Removed comment from {object_type} '{display_name}'"
