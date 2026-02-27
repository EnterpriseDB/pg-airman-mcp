"""Unit tests for server utility functions and handlers."""

import argparse
import signal
from unittest.mock import AsyncMock, MagicMock, patch

import mcp.types as types
import pytest

from pg_airman_mcp.server import (
    format_error_response,
    format_text_response,
    handle_transport_exit,
    shutdown,
    signal_handler,
)


class TestResponseFormatters:
    """Test response formatting utility functions."""

    def test_format_text_response_with_string(self):
        """Test formatting text response with string input."""
        result = format_text_response("test message")
        assert len(result) == 1
        content = result[0]
        assert isinstance(content, types.TextContent)
        assert content.type == "text"
        assert content.text == "test message"

    def test_format_text_response_with_dict(self):
        """Test formatting text response with dict input."""
        test_dict = {"key": "value", "number": 42}
        result = format_text_response(test_dict)
        assert len(result) == 1
        content = result[0]
        assert isinstance(content, types.TextContent)
        assert content.type == "text"
        assert "key" in content.text
        assert "value" in content.text

    def test_format_text_response_with_list(self):
        """Test formatting text response with list input."""
        test_list = [1, 2, 3, "test"]
        result = format_text_response(test_list)
        assert len(result) == 1
        content = result[0]
        assert isinstance(content, types.TextContent)
        assert content.type == "text"
        assert "[1, 2, 3, 'test']" in content.text

    def test_format_text_response_with_none(self):
        """Test formatting text response with None input."""
        result = format_text_response(None)
        assert len(result) == 1
        content = result[0]
        assert isinstance(content, types.TextContent)
        assert content.text == "None"

    def test_format_text_response_with_int(self):
        """Test formatting text response with integer input."""
        result = format_text_response(42)
        assert len(result) == 1
        content = result[0]
        assert isinstance(content, types.TextContent)
        assert content.text == "42"

    def test_format_error_response(self):
        """Test formatting error response."""
        result = format_error_response("Something went wrong")
        assert len(result) == 1
        content = result[0]
        assert isinstance(content, types.TextContent)
        assert content.type == "text"
        assert content.text == "Error: Something went wrong"

    def test_format_error_response_with_exception_message(self):
        """Test formatting error response with exception-like message."""
        error_msg = "Database connection failed: timeout"
        result = format_error_response(error_msg)
        content = result[0]
        assert isinstance(content, types.TextContent)
        assert content.text == f"Error: {error_msg}"


class TestSignalHandlers:
    """Test signal handling for graceful shutdown."""

    def test_signal_handler_stdio_transport(self):
        """Test signal handler calls sys.exit() for stdio transport."""
        with patch("pg_airman_mcp.server.is_stdio_transport", True):
            with patch("pg_airman_mcp.server.sys.exit") as mock_exit:
                signal_handler(signal.SIGINT, None)
                mock_exit.assert_called_once_with(0)

    def test_signal_handler_sse_transport(self):
        """Test signal handler sets shutdown_event for SSE transport."""
        with patch("pg_airman_mcp.server.is_stdio_transport", False):
            with patch("pg_airman_mcp.server.shutdown_event") as mock_event:
                signal_handler(signal.SIGTERM, None)
                mock_event.set.assert_called_once()

    def test_signal_handler_http_transport(self):
        """Test signal handler sets shutdown_event for HTTP transport."""
        with patch("pg_airman_mcp.server.is_stdio_transport", False):
            with patch("pg_airman_mcp.server.shutdown_event") as mock_event:
                signal_handler(signal.SIGINT, None)
                mock_event.set.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_transport_exit_stdio(self):
        """Test handle_transport_exit calls sys.exit for stdio."""
        with patch("pg_airman_mcp.server.is_stdio_transport", True):
            with patch("pg_airman_mcp.server.sys.exit") as mock_exit:
                await handle_transport_exit(0)
                mock_exit.assert_called_once_with(0)

    @pytest.mark.asyncio
    async def test_handle_transport_exit_stdio_error_code(self):
        """Test handle_transport_exit with error code for stdio."""
        with patch("pg_airman_mcp.server.is_stdio_transport", True):
            with patch("pg_airman_mcp.server.sys.exit") as mock_exit:
                await handle_transport_exit(1)
                mock_exit.assert_called_once_with(1)

    @pytest.mark.asyncio
    async def test_handle_transport_exit_non_stdio(self):
        """Test handle_transport_exit calls shutdown for non-stdio."""
        with patch("pg_airman_mcp.server.is_stdio_transport", False):
            with patch("pg_airman_mcp.server.shutdown") as mock_shutdown:
                # Make shutdown a coroutine
                mock_shutdown.return_value = AsyncMock()
                await handle_transport_exit(0)
                # shutdown should be called (not sys.exit)
                mock_shutdown.assert_called_once()

    @pytest.mark.asyncio
    async def test_shutdown_closes_db_connection(self):
        """Test shutdown properly closes database connections."""
        with patch("pg_airman_mcp.server.db_connection") as mock_db:
            mock_db.close = AsyncMock()
            await shutdown()
            mock_db.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_shutdown_handles_db_close_error(self):
        """Test shutdown handles database close errors gracefully."""
        with (
            patch("pg_airman_mcp.server.db_connection") as mock_db,
            patch("pg_airman_mcp.server.logger") as mock_logger,
        ):
            mock_db.close = AsyncMock(side_effect=Exception("Close failed"))
            # Should not raise, should log error
            await shutdown()
            mock_logger.error.assert_called()

    @pytest.mark.asyncio
    async def test_shutdown_with_signal(self):
        """Test shutdown logs signal when provided."""
        with (
            patch("pg_airman_mcp.server.db_connection") as mock_db,
            patch("pg_airman_mcp.server.logger") as mock_logger,
        ):
            mock_db.close = AsyncMock()
            mock_signal = MagicMock()
            mock_signal.name = "SIGTERM"
            await shutdown(sig=mock_signal)
            # Should log the signal name
            mock_logger.info.assert_any_call("Received exit signal SIGTERM")


class TestToolParameterValidation:
    """Test tool parameter validation with Pydantic."""

    @pytest.mark.asyncio
    async def test_analyze_workload_indexes_default_max_size(self):
        """Test analyze_workload_indexes uses default max_index_size_mb."""
        from pg_airman_mcp.server import analyze_workload_indexes

        with (
            patch("pg_airman_mcp.server.get_sql_driver") as mock_driver,
            patch("pg_airman_mcp.server.DatabaseTuningAdvisor"),
            patch("pg_airman_mcp.server.TextPresentation") as mock_presentation,
        ):
            mock_sql_driver = AsyncMock()
            mock_driver.return_value = mock_sql_driver

            mock_presentation_instance = MagicMock()
            mock_presentation_instance.analyze_workload = AsyncMock(
                return_value="result"
            )
            mock_presentation.return_value = mock_presentation_instance

            await analyze_workload_indexes()

            # Check that analyze_workload was called with default
            call_kwargs = mock_presentation_instance.analyze_workload.call_args.kwargs
            assert call_kwargs["max_index_size_mb"] == 10000

    @pytest.mark.asyncio
    async def test_analyze_query_indexes_requires_list(self):
        """Test analyze_query_indexes requires queries to be a list."""
        from pydantic import ValidationError

        from pg_airman_mcp.server import analyze_query_indexes

        # Should raise ValidationError for non-list
        with pytest.raises(ValidationError):
            await analyze_query_indexes(queries="not a list")  # type: ignore

    @pytest.mark.asyncio
    async def test_analyze_query_indexes_empty_list_error(self):
        """Test analyze_query_indexes returns error for empty list."""
        from pg_airman_mcp.server import analyze_query_indexes

        result = await analyze_query_indexes(queries=[])
        # Should return error response
        assert len(result) == 1
        content = result[0]
        assert isinstance(content, types.TextContent)
        assert "Error:" in content.text
        assert "non-empty" in content.text

    @pytest.mark.asyncio
    async def test_analyze_query_indexes_max_queries_limit(self):
        """Test analyze_query_indexes enforces MAX_NUM_INDEX_TUNING_QUERIES."""
        from pg_airman_mcp.server import analyze_query_indexes

        # Create 11 queries (exceeds limit of 10)
        many_queries = [f"SELECT {i}" for i in range(11)]

        result = await analyze_query_indexes(queries=many_queries)
        # Should return error response
        assert len(result) == 1
        content = result[0]
        assert isinstance(content, types.TextContent)
        assert "Error:" in content.text
        assert "up to 10" in content.text


class TestToolErrorHandling:
    """Test error handling in tool functions."""

    @pytest.mark.asyncio
    async def test_list_schemas_db_error(self):
        """Test list_schemas handles database errors."""
        from pg_airman_mcp.server import list_schemas

        with patch("pg_airman_mcp.server.get_sql_driver") as mock_driver:
            mock_sql_driver = AsyncMock()
            mock_sql_driver.execute_query = AsyncMock(
                side_effect=Exception("Database connection failed")
            )
            mock_driver.return_value = mock_sql_driver

            result = await list_schemas()

            # Should return error response
            assert len(result) == 1
            content = result[0]
            assert isinstance(content, types.TextContent)
            assert "Error:" in content.text
            assert "Database connection failed" in content.text

    @pytest.mark.asyncio
    async def test_explain_query_invalid_sql(self):
        """Test explain_query handles invalid SQL."""
        from pg_airman_mcp.server import explain_query

        with (
            patch("pg_airman_mcp.server.get_sql_driver") as mock_driver,
            patch("pg_airman_mcp.server.ExplainPlanTool") as mock_tool,
            patch(
                "pg_airman_mcp.server.check_hypopg_installation_status"
            ) as mock_hypopg,
        ):
            mock_sql_driver = AsyncMock()
            mock_driver.return_value = mock_sql_driver

            # Mock HypoPG check
            mock_hypopg.return_value = (True, "")

            mock_explain_instance = MagicMock()
            mock_explain_instance.explain = AsyncMock(
                side_effect=Exception("Syntax error in SQL")
            )
            mock_tool.return_value = mock_explain_instance

            # Must explicitly pass default values for Field() parameters
            result = await explain_query(
                sql="INVALID SQL;;;", analyze=False, hypothetical_indexes=[]
            )

            # Should return error response
            assert len(result) == 1
            content = result[0]
            assert isinstance(content, types.TextContent)
            assert "Error:" in content.text
            assert "Syntax error" in content.text

    @pytest.mark.asyncio
    async def test_execute_sql_connection_error(self):
        """Test execute_sql handles connection errors."""
        from pg_airman_mcp.server import execute_sql

        with patch("pg_airman_mcp.server.get_sql_driver") as mock_driver:
            mock_sql_driver = AsyncMock()
            mock_sql_driver.execute_query = AsyncMock(
                side_effect=Exception("Connection timeout")
            )
            mock_driver.return_value = mock_sql_driver

            result = await execute_sql(sql="SELECT 1")

            # Should return error response
            assert len(result) == 1
            content = result[0]
            assert isinstance(content, types.TextContent)
            assert "Error:" in content.text
            assert "Connection timeout" in content.text

    @pytest.mark.asyncio
    async def test_get_object_details_not_found(self):
        """Test get_object_details handles non-existent objects."""
        from pg_airman_mcp.server import get_object_details

        with patch("pg_airman_mcp.server.get_sql_driver") as mock_driver:
            mock_sql_driver = AsyncMock()
            mock_sql_driver.execute_query = AsyncMock(
                side_effect=Exception("relation does not exist")
            )
            mock_driver.return_value = mock_sql_driver

            result = await get_object_details(
                schema_name="nonexistent", object_name="table", object_type="table"
            )

            # Should return error response
            assert len(result) == 1
            content = result[0]
            assert isinstance(content, types.TextContent)
            assert "Error:" in content.text

    @pytest.mark.asyncio
    async def test_analyze_db_health_extension_missing(self):
        """Test analyze_db_health handles missing extensions."""
        from pg_airman_mcp.server import analyze_db_health

        with patch("pg_airman_mcp.server.get_sql_driver") as mock_driver:
            mock_sql_driver = AsyncMock()
            mock_driver.return_value = mock_sql_driver

            # DatabaseHealthTool is instantiated directly in analyze_db_health
            # We need to patch it at construction time
            with patch("pg_airman_mcp.server.DatabaseHealthTool") as mock_tool_class:
                mock_health_instance = MagicMock()
                mock_health_instance.health = AsyncMock(
                    side_effect=Exception("pg_stat_statements not installed")
                )
                mock_tool_class.return_value = mock_health_instance

                # Must explicitly pass default value for Field() parameter
                # The function doesn't catch exceptions, so it will propagate
                with pytest.raises(Exception) as exc:
                    await analyze_db_health(health_type="all")

                # Should raise the extension error
                assert "pg_stat_statements not installed" in str(exc.value)

    @pytest.mark.asyncio
    async def test_list_objects_invalid_type(self):
        """Test list_objects handles invalid object_type."""
        from pg_airman_mcp.server import list_objects

        with patch("pg_airman_mcp.server.get_sql_driver"):
            result = await list_objects(
                schema_name="public", object_type="invalid_type"
            )

            # Should return error response for unsupported type
            assert len(result) == 1
            content = result[0]
            assert isinstance(content, types.TextContent)
            assert "Error:" in content.text
            assert "Unsupported object type" in content.text


class TestMainFunctionIntegration:
    """Test main() function argument handling and integration."""

    @pytest.mark.asyncio
    async def test_main_requires_database_url(self):
        """Test main() raises error when no database URL provided."""
        from pg_airman_mcp.server import main

        with (
            patch("pg_airman_mcp.server.ServerSettings") as mock_settings,
            patch("sys.argv", ["server.py"]),
        ):
            # Mock settings with no database_url
            mock_settings_instance = MagicMock()
            mock_settings_instance.database_url = None
            mock_settings_instance.access_mode = "unrestricted"
            mock_settings_instance.transport = "stdio"
            mock_settings_instance.auth_enabled = False
            mock_settings_instance.build_transport_security.return_value = None
            mock_settings.return_value = mock_settings_instance

            # Should raise ValueError
            with pytest.raises(ValueError) as exc:
                await main()
            assert "No database URL provided" in str(exc.value)

    @pytest.mark.asyncio
    async def test_main_auth_requires_server_url_for_stdio(self):
        """Test auth with stdio requires AIRMAN_MCP_SERVER_URL."""
        from pg_airman_mcp.server import main

        with (
            patch("pg_airman_mcp.server.ServerSettings") as mock_settings,
            patch("sys.argv", ["server.py", "postgresql://test"]),
        ):
            # Mock settings with auth enabled but no server_url
            mock_settings_instance = MagicMock()
            mock_settings_instance.database_url = "postgresql://test"
            mock_settings_instance.access_mode = "unrestricted"
            mock_settings_instance.transport = "stdio"
            mock_settings_instance.auth_enabled = True
            mock_settings_instance.determine_server_url = MagicMock(return_value=None)
            mock_settings_instance.get_required_scopes = MagicMock(return_value=[])
            mock_settings.return_value = mock_settings_instance

            # Should raise ValueError
            with pytest.raises(ValueError) as exc:
                await main()
            assert "AIRMAN_MCP_SERVER_URL" in str(exc.value)

    @pytest.mark.asyncio
    async def test_main_adds_execute_sql_tool_unrestricted(self):
        """Test execute_sql tool added with unrestricted description."""
        from pg_airman_mcp.server import main

        with (
            patch("pg_airman_mcp.server.ServerSettings") as mock_settings,
            patch("pg_airman_mcp.server.create_mcp_server") as mock_create,
            patch("pg_airman_mcp.server.db_connection") as mock_db,
            patch("sys.argv", ["server.py", "postgresql://test"]),
            patch("signal.signal"),
        ):
            # Mock settings
            mock_settings_instance = MagicMock()
            mock_settings_instance.database_url = "postgresql://test"
            mock_settings_instance.access_mode = "unrestricted"
            mock_settings_instance.transport = "stdio"
            mock_settings_instance.auth_enabled = False
            mock_settings_instance.determine_server_url = MagicMock(return_value=None)
            mock_settings_instance.get_required_scopes = MagicMock(return_value=[])
            mock_settings.return_value = mock_settings_instance

            # Mock MCP server
            mock_mcp = MagicMock()
            mock_mcp.add_tool = MagicMock()
            mock_mcp.run_stdio_async = AsyncMock()
            mock_create.return_value = mock_mcp

            # Mock db connection
            mock_db.pool_connect = AsyncMock()
            mock_db.close = AsyncMock()

            # Mock shutdown_event to exit loop immediately
            with patch("pg_airman_mcp.server.shutdown_event") as mock_event:
                mock_event.is_set = MagicMock(side_effect=[False, True])

                # main() calls sys.exit(0) at the end
                with pytest.raises(SystemExit) as exc:
                    await main()
                assert exc.value.code == 0

                # Check that add_tool was called with execute_sql
                add_tool_calls = [call for call in mock_mcp.add_tool.call_args_list]
                # Find the execute_sql call
                execute_sql_call = None
                for call in add_tool_calls:
                    desc = call[1].get("description", "")
                    if "Execute any SQL" in desc:
                        execute_sql_call = call
                        break

                assert execute_sql_call is not None, "execute_sql not added to tools"

    @pytest.mark.asyncio
    async def test_main_adds_execute_sql_tool_restricted(self):
        """Test execute_sql tool added with restricted description."""
        from pg_airman_mcp.server import main

        with (
            patch("pg_airman_mcp.server.ServerSettings") as mock_settings,
            patch("pg_airman_mcp.server.create_mcp_server") as mock_create,
            patch("pg_airman_mcp.server.db_connection") as mock_db,
            patch("sys.argv", ["server.py", "postgresql://test"]),
            patch("signal.signal"),
        ):
            # Mock settings with restricted mode
            mock_settings_instance = MagicMock()
            mock_settings_instance.database_url = "postgresql://test"
            mock_settings_instance.access_mode = "restricted"
            mock_settings_instance.transport = "stdio"
            mock_settings_instance.auth_enabled = False
            mock_settings_instance.determine_server_url = MagicMock(return_value=None)
            mock_settings_instance.get_required_scopes = MagicMock(return_value=[])
            mock_settings.return_value = mock_settings_instance

            mock_mcp = MagicMock()
            mock_mcp.add_tool = MagicMock()
            mock_mcp.run_stdio_async = AsyncMock()
            mock_create.return_value = mock_mcp

            mock_db.pool_connect = AsyncMock()
            mock_db.close = AsyncMock()

            with patch("pg_airman_mcp.server.shutdown_event") as mock_event:
                mock_event.is_set = MagicMock(side_effect=[False, True])

                # main() calls sys.exit(0) at the end
                with pytest.raises(SystemExit) as exc:
                    await main()
                assert exc.value.code == 0

                # Check for read-only description
                add_tool_calls = [call for call in mock_mcp.add_tool.call_args_list]
                execute_sql_call = None
                for call in add_tool_calls:
                    desc = call[1].get("description", "")
                    if "read-only SQL" in desc:
                        execute_sql_call = call
                        break

                assert execute_sql_call is not None, (
                    "execute_sql with restricted description not added"
                )


class TestToolRegistration:
    """Test tool registration validation."""

    def test_all_required_tools_registered(self):
        """Test that all expected tools are registered."""
        from pg_airman_mcp.server import create_mcp_server

        with patch("pg_airman_mcp.server.FastMCP") as mock_fastmcp:
            mock_instance = MagicMock()
            mock_fastmcp.return_value = mock_instance

            create_mcp_server(auth_context=None)

            # Get all registered tool names
            registered_tools = []
            for call in mock_instance.add_tool.call_args_list:
                tool_func = call[0][0]
                registered_tools.append(tool_func.__name__)

            # Check all expected tools (excluding execute_sql which is added in main)
            expected_tools = [
                "list_schemas",
                "list_objects",
                "get_object_details",
                "explain_query",
                "analyze_workload_indexes",
                "analyze_query_indexes",
                "analyze_db_health",
                "get_top_queries",
                "add_comment_to_object",
            ]

            assert len(registered_tools) == len(expected_tools)
            for tool in expected_tools:
                assert tool in registered_tools, f"Tool {tool} not registered"

    def test_tools_have_descriptions(self):
        """Test that all registered tools have descriptions."""
        from pg_airman_mcp.server import create_mcp_server

        with patch("pg_airman_mcp.server.FastMCP") as mock_fastmcp:
            mock_instance = MagicMock()
            mock_fastmcp.return_value = mock_instance

            create_mcp_server(auth_context=None)

            # Check all tools have descriptions
            for call in mock_instance.add_tool.call_args_list:
                description = call[1].get("description")
                assert description is not None, "Tool missing description"
                assert len(description) > 0, "Tool has empty description"
                assert isinstance(description, str), "Tool description must be string"

    def test_tool_functions_are_async(self):
        """Test that all registered tool functions are async."""
        import asyncio

        from pg_airman_mcp.server import create_mcp_server

        with patch("pg_airman_mcp.server.FastMCP") as mock_fastmcp:
            mock_instance = MagicMock()
            mock_fastmcp.return_value = mock_instance

            create_mcp_server(auth_context=None)

            # Check all tool functions are coroutines
            for call in mock_instance.add_tool.call_args_list:
                tool_func = call[0][0]
                assert asyncio.iscoroutinefunction(tool_func), (
                    f"Tool {tool_func.__name__} is not async"
                )

    def test_tool_functions_have_parameters(self):
        """Test that tool functions have properly defined parameters."""
        import inspect

        from pg_airman_mcp.server import create_mcp_server

        with patch("pg_airman_mcp.server.FastMCP") as mock_fastmcp:
            mock_instance = MagicMock()
            mock_fastmcp.return_value = mock_instance

            create_mcp_server(auth_context=None)

            # Check tool parameters
            for call in mock_instance.add_tool.call_args_list:
                tool_func = call[0][0]
                sig = inspect.signature(tool_func)

                # All parameters should have type annotations
                for param_name, param in sig.parameters.items():
                    assert param.annotation != inspect.Parameter.empty, (
                        f"Parameter {param_name} in "
                        f"{tool_func.__name__} lacks type annotation"
                    )

    def test_tool_registration_with_auth(self):
        """Test tools are registered correctly with authentication enabled."""
        from pg_airman_mcp.auth_config import AuthContext
        from pg_airman_mcp.server import create_mcp_server

        with patch("pg_airman_mcp.server.FastMCP") as mock_fastmcp:
            mock_instance = MagicMock()
            mock_fastmcp.return_value = mock_instance

            auth_ctx = AuthContext(
                token_verifier=MagicMock(),
                auth_settings=MagicMock(),
            )
            create_mcp_server(auth_context=auth_ctx)

            # Should register same number of tools with auth
            assert mock_instance.add_tool.call_count == 9

    def test_list_schemas_has_proper_signature(self):
        """Test list_schemas tool has expected parameters."""
        import inspect

        from pg_airman_mcp.server import list_schemas

        sig = inspect.signature(list_schemas)
        params = list(sig.parameters.keys())

        # list_schemas should have no required parameters
        assert len(params) == 0 or all(
            sig.parameters[p].default != inspect.Parameter.empty for p in params
        )

    def test_explain_query_has_proper_signature(self):
        """Test explain_query tool has expected parameters."""
        import inspect

        from pg_airman_mcp.server import explain_query

        sig = inspect.signature(explain_query)
        params = list(sig.parameters.keys())

        # explain_query should have sql, analyze, hypothetical_indexes
        assert "sql" in params
        assert "analyze" in params
        assert "hypothetical_indexes" in params

    def test_execute_sql_has_proper_signature(self):
        """Test execute_sql tool has expected parameters."""
        import inspect

        from pg_airman_mcp.server import execute_sql

        sig = inspect.signature(execute_sql)
        params = list(sig.parameters.keys())

        # execute_sql should have sql parameter
        assert "sql" in params
        assert len(params) == 1

    def test_analyze_workload_indexes_has_proper_signature(self):
        """Test analyze_workload_indexes has expected parameters."""
        import inspect

        from pg_airman_mcp.server import analyze_workload_indexes

        sig = inspect.signature(analyze_workload_indexes)
        params = list(sig.parameters.keys())

        # Should have max_index_size_mb and method parameters
        assert "max_index_size_mb" in params
        assert "method" in params

    def test_get_top_queries_has_proper_signature(self):
        """Test get_top_queries has expected parameters."""
        import inspect

        from pg_airman_mcp.server import get_top_queries

        sig = inspect.signature(get_top_queries)
        params = list(sig.parameters.keys())

        # Should have sort_by and limit parameters
        assert "sort_by" in params
        assert "limit" in params

    def test_tool_return_types_annotated(self):
        """Test that all tool functions have return type annotations."""
        import inspect

        from pg_airman_mcp.server import (
            add_comment_to_object,
            analyze_db_health,
            analyze_query_indexes,
            analyze_workload_indexes,
            execute_sql,
            explain_query,
            get_object_details,
            get_top_queries,
            list_objects,
            list_schemas,
        )

        tools = [
            list_schemas,
            list_objects,
            get_object_details,
            explain_query,
            analyze_workload_indexes,
            analyze_query_indexes,
            analyze_db_health,
            get_top_queries,
            add_comment_to_object,
            execute_sql,
        ]

        for tool in tools:
            sig = inspect.signature(tool)
            assert sig.return_annotation != inspect.Signature.empty, (
                f"Tool {tool.__name__} lacks return type annotation"
            )

    def test_tool_docstrings_present(self):
        """Test that all tool functions have docstrings."""
        from pg_airman_mcp.server import (
            add_comment_to_object,
            analyze_db_health,
            analyze_query_indexes,
            analyze_workload_indexes,
            execute_sql,
            explain_query,
            get_object_details,
            get_top_queries,
            list_objects,
            list_schemas,
        )

        tools = [
            list_schemas,
            list_objects,
            get_object_details,
            explain_query,
            analyze_workload_indexes,
            analyze_query_indexes,
            analyze_db_health,
            get_top_queries,
            add_comment_to_object,
            execute_sql,
        ]

        for tool in tools:
            assert tool.__doc__ is not None, f"Tool {tool.__name__} lacks docstring"
            assert len(tool.__doc__.strip()) > 0, (
                f"Tool {tool.__name__} has empty docstring"
            )


class TestAuthBooleanFlags:
    """Test BooleanOptionalAction for --auth-enabled and --auth-validate-resource.

    These flags use argparse.BooleanOptionalAction so that an env-derived
    ``True`` default can be overridden from the CLI with ``--no-<flag>``.
    """

    @pytest.fixture
    def make_parser(self):
        """Build a parser that mirrors main()'s auth flag definitions."""

        def _make(
            auth_enabled_default: bool = False,
            auth_validate_default: bool = False,
        ):
            parser = argparse.ArgumentParser()
            parser.add_argument("database_url", nargs="?", default=None)
            parser.add_argument(
                "--auth-enabled",
                action=argparse.BooleanOptionalAction,
                default=auth_enabled_default,
            )
            parser.add_argument(
                "--auth-validate-resource",
                action=argparse.BooleanOptionalAction,
                default=auth_validate_default,
            )
            return parser

        return _make

    # --auth-enabled -----------------------------------------------------------

    def test_auth_enabled_default_false(self, make_parser):
        """No flag provided, default False → False."""
        args = make_parser(auth_enabled_default=False).parse_args([])
        assert args.auth_enabled is False

    def test_auth_enabled_default_true(self, make_parser):
        """No flag provided, default True (env var) → True."""
        args = make_parser(auth_enabled_default=True).parse_args([])
        assert args.auth_enabled is True

    def test_auth_enabled_explicit_flag(self, make_parser):
        """--auth-enabled explicitly sets True."""
        args = make_parser(auth_enabled_default=False).parse_args(["--auth-enabled"])
        assert args.auth_enabled is True

    def test_no_auth_enabled_overrides_true_default(self, make_parser):
        """--no-auth-enabled overrides a True default (the key use case)."""
        args = make_parser(auth_enabled_default=True).parse_args(["--no-auth-enabled"])
        assert args.auth_enabled is False

    # --auth-validate-resource -------------------------------------------------

    def test_auth_validate_resource_default_false(self, make_parser):
        """No flag provided, default False → False."""
        args = make_parser(auth_validate_default=False).parse_args([])
        assert args.auth_validate_resource is False

    def test_auth_validate_resource_default_true(self, make_parser):
        """No flag provided, default True (env var) → True."""
        args = make_parser(auth_validate_default=True).parse_args([])
        assert args.auth_validate_resource is True

    def test_auth_validate_resource_explicit_flag(self, make_parser):
        """--auth-validate-resource explicitly sets True."""
        args = make_parser(auth_validate_default=False).parse_args(
            ["--auth-validate-resource"]
        )
        assert args.auth_validate_resource is True

    def test_no_auth_validate_resource_overrides_true_default(self, make_parser):
        """--no-auth-validate-resource overrides a True default."""
        args = make_parser(auth_validate_default=True).parse_args(
            ["--no-auth-validate-resource"]
        )
        assert args.auth_validate_resource is False

    # Integration through main() -----------------------------------------------

    @pytest.mark.asyncio
    async def test_main_no_auth_enabled_overrides_env(self):
        """--no-auth-enabled disables auth even when env default is True."""
        from pg_airman_mcp.server import main

        with (
            patch("pg_airman_mcp.server.ServerSettings") as mock_settings,
            patch("pg_airman_mcp.server.create_mcp_server") as mock_create,
            patch("pg_airman_mcp.server.db_connection") as mock_db,
            patch(
                "sys.argv",
                ["server.py", "postgresql://test", "--no-auth-enabled"],
            ),
            patch("signal.signal"),
        ):
            mock_settings_instance = MagicMock()
            mock_settings_instance.database_url = "postgresql://test"
            mock_settings_instance.access_mode = "unrestricted"
            mock_settings_instance.transport = "stdio"
            # Simulate env var setting auth_enabled = True
            mock_settings_instance.auth_enabled = True
            mock_settings_instance.auth_validate_resource = False
            mock_settings_instance.determine_server_url = MagicMock(return_value=None)
            mock_settings_instance.get_required_scopes = MagicMock(return_value=[])
            mock_settings.return_value = mock_settings_instance

            mock_mcp = MagicMock()
            mock_mcp.add_tool = MagicMock()
            mock_mcp.run_stdio_async = AsyncMock()
            mock_create.return_value = mock_mcp

            mock_db.pool_connect = AsyncMock()
            mock_db.close = AsyncMock()

            with patch("pg_airman_mcp.server.shutdown_event") as mock_event:
                mock_event.is_set = MagicMock(side_effect=[False, True])

                with pytest.raises(SystemExit) as exc:
                    await main()
                assert exc.value.code == 0

            # --no-auth-enabled should have overridden the env default
            assert mock_settings_instance.auth_enabled is False

    @pytest.mark.asyncio
    async def test_main_no_auth_validate_resource_overrides_env(self):
        """--no-auth-validate-resource disables validation even when
        env default is True."""
        from pg_airman_mcp.server import main

        with (
            patch("pg_airman_mcp.server.ServerSettings") as mock_settings,
            patch("pg_airman_mcp.server.create_mcp_server") as mock_create,
            patch("pg_airman_mcp.server.db_connection") as mock_db,
            patch(
                "sys.argv",
                ["server.py", "postgresql://test", "--no-auth-validate-resource"],
            ),
            patch("signal.signal"),
        ):
            mock_settings_instance = MagicMock()
            mock_settings_instance.database_url = "postgresql://test"
            mock_settings_instance.access_mode = "unrestricted"
            mock_settings_instance.transport = "stdio"
            mock_settings_instance.auth_enabled = False
            # Simulate env var setting auth_validate_resource = True
            mock_settings_instance.auth_validate_resource = True
            mock_settings_instance.determine_server_url = MagicMock(return_value=None)
            mock_settings_instance.get_required_scopes = MagicMock(return_value=[])
            mock_settings.return_value = mock_settings_instance

            mock_mcp = MagicMock()
            mock_mcp.add_tool = MagicMock()
            mock_mcp.run_stdio_async = AsyncMock()
            mock_create.return_value = mock_mcp

            mock_db.pool_connect = AsyncMock()
            mock_db.close = AsyncMock()

            with patch("pg_airman_mcp.server.shutdown_event") as mock_event:
                mock_event.is_set = MagicMock(side_effect=[False, True])

                with pytest.raises(SystemExit) as exc:
                    await main()
                assert exc.value.code == 0

            # --no-auth-validate-resource should have overridden the env default
            assert mock_settings_instance.auth_validate_resource is False
