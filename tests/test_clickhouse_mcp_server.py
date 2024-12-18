import os
import uuid

import pytest
from pydantic import AnyUrl
from testcontainers.clickhouse import ClickHouseContainer

from clickhouse_mcp_server.server import (
    app,
    call_tool,
    get_clickhouse_client,
    list_resources,
    list_tools,
    read_resource,
)


@pytest.fixture(scope="module")
def clickhouse_container():
    with ClickHouseContainer(
        image="clickhouse/clickhouse-server:21.8", username="default", password="test"
    ) as container:
        yield container


@pytest.fixture
def clickhouse_client(clickhouse_container):
    # clickhouse_container.get_connection_url()
    port = clickhouse_container.get_exposed_port(8123)
    os.environ["CLICKHOUSE_HOST"] = "localhost"
    os.environ["CLICKHOUSE_PORT"] = str(port)
    os.environ["CLICKHOUSE_USER"] = "default"
    os.environ["CLICKHOUSE_PASSWORD"] = "test"
    os.environ["CLICKHOUSE_DATABASE"] = "default"
    return get_clickhouse_client()


def test_server_initialization():
    """Test that the server initializes correctly."""
    assert app.name == "clickhouse_mcp_server"


@pytest.mark.asyncio
async def test_list_tools():
    """Test that list_tools returns expected tools."""
    tools = await list_tools()
    assert len(tools) == 1
    assert tools[0].name == "execute_select_query"
    assert "query" in tools[0].inputSchema["properties"]

@pytest.mark.asyncio
async def test_list_resources():
    """Test that list_resources returns some resources."""
    resources = await list_resources()
    assert len(resources) >= 1
    assert any(str(r.uri) == 'clickhouse://default/tables' for r in resources)

@pytest.mark.asyncio
async def test_clickhouse_integration(clickhouse_client):
    # Create a test table
    clickhouse_client.command("""
        CREATE TABLE test_table (
            id UUID,
            name String,
            value Int32
        ) ENGINE = Memory
    """)

    # Insert test data
    test_data = [
        (uuid.uuid4(), "Test 1", 10),
        (uuid.uuid4(), "Test 2", 20),
        (uuid.uuid4(), "Test 3", 30),
    ]

    for id, name, value in test_data:
        clickhouse_client.command(f"INSERT INTO test_table (id, name, value) VALUES ('{id}', '{name}', {value})")

    # Test list_resources
    resources = await list_resources()
    assert any(r.name == "Database: default" for r in resources)
    assert any(r.name == "Table: default.test_table" for r in resources)

    # Test read_resource for table schema
    schema = await read_resource(AnyUrl("clickhouse://default/test_table/schema"))
    assert "id - UUID" in schema
    assert "name - String" in schema
    assert "value - Int32" in schema

    # Test execute_select_query
    query_result = await call_tool("execute_select_query", {"query": "SELECT * FROM test_table ORDER BY value"})

    assert len(query_result) == 1
    result_text = query_result[0].text
    assert "Test 1\t10" in result_text
    assert "Test 2\t20" in result_text
    assert "Test 3\t30" in result_text


@pytest.mark.asyncio
async def test_invalid_query(clickhouse_client):
    # Test execute_select_query with an invalid query
    query_result = await call_tool(
        "execute_select_query",
        {"query": "INSERT INTO test_table (id, name, value) VALUES (1, 'Invalid', 100)"},
    )

    assert len(query_result) == 1
    result_text = query_result[0].text
    assert "Error: Only SELECT queries are allowed." in result_text


@pytest.mark.asyncio
async def test_invalid_resource_uri():
    with pytest.raises(ValueError, match="Invalid URI scheme"):
        await read_resource(AnyUrl("invalid://default/test_table/schema"))

    with pytest.raises(ValueError, match="Invalid resource URI"):
        await read_resource(AnyUrl("clickhouse://default/invalid"))


@pytest.mark.asyncio
async def test_read_resource_list_tables(clickhouse_client):
    # Create additional test tables
    clickhouse_client.command("CREATE TABLE test_table2 (id Int32) ENGINE = Memory")
    clickhouse_client.command("CREATE TABLE test_table3 (name String) ENGINE = Memory")

    # Test read_resource for listing tables
    tables = await read_resource(AnyUrl("clickhouse://default/tables"))
    assert "test_table" in tables
    assert "test_table2" in tables
    assert "test_table3" in tables


@pytest.mark.asyncio
async def test_read_resource_table_schema(clickhouse_client):
    # Test read_resource for table schema
    schema = await read_resource(AnyUrl("clickhouse://default/test_table2/schema"))
    assert "id - Int32" in schema

    schema = await read_resource(AnyUrl("clickhouse://default/test_table3/schema"))
    assert "name - String" in schema


@pytest.mark.asyncio
async def test_read_resource_non_existent_table(clickhouse_client):
    with pytest.raises(Exception, match="Table default.non_existent_table doesn't exist"):
        await read_resource(AnyUrl("clickhouse://default/non_existent_table/schema"))


@pytest.mark.asyncio
async def test_read_resource_non_existent_database():
    with pytest.raises(Exception, match="Database non_existent_db doesn't exist"):
        await read_resource(AnyUrl("clickhouse://non_existent_db/tables"))
