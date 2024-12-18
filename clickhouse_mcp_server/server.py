import asyncio
import logging
import os
from typing import Annotated

from clickhouse_connect import get_client
from clickhouse_connect.driver.client import Client
from mcp.server import Server
from mcp.types import Resource, TextContent, Tool
from pydantic import AnyUrl, UrlConstraints

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("clickhouse_mcp_server")


ClickhouseDsn = Annotated[
    AnyUrl,
    UrlConstraints(
        host_required=True,
        allowed_schemes=[
            "clickhouse",
        ],
    ),
]


def get_clickhouse_client(
    host: str | None = None,
    port: int | None = None,
    username: str | None = None,
    password: str | None = None,
    database: str | None = None,
) -> Client:
    """Create and return a ClickHouse client."""

    CLICKHOUSE_HOST = host or os.getenv("CLICKHOUSE_HOST", "localhost")
    CLICKHOUSE_PORT = port or int(os.getenv("CLICKHOUSE_PORT", "8123"))
    CLICKHOUSE_USER = username or os.getenv("CLICKHOUSE_USER", "default")
    CLICKHOUSE_PASSWORD = password or os.getenv("CLICKHOUSE_PASSWORD", "")
    CLICKHOUSE_DATABASE = database or os.getenv("CLICKHOUSE_DATABASE", "default")
    return get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )


app = Server("clickhouse_mcp_server")


@app.list_resources()
async def list_resources() -> list[Resource]:
    """List ClickHouse databases and tables as resources."""
    client = get_clickhouse_client()
    resources = []

    db_query = """
    SELECT
        name,
        engine
    FROM system.databases
    WHERE name NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
    """
    db_result = client.query(db_query)
    for db in db_result.result_rows:
        database = db[0]
        resources.append(
            Resource(
                uri=AnyUrl(f"clickhouse://{database}/tables"),
                name=f"Database: {database}",
                mimeType="text/plain",
                description=f"Tables in database: {database}",
            )
        )

        # List tables for each database
        table_result = client.query(f"SHOW TABLES FROM {database}")
        resources.extend(
            [
                Resource(
                    uri=AnyUrl(f"clickhouse://{database}/{table[0]}/schema"),
                    name=f"Table: {database}.{table[0]}",
                    mimeType="text/plain",
                    description=f"Schema of table: {database}.{table[0]}",
                )
                for table in table_result.result_rows
            ]
        )

    return resources


@app.read_resource()
async def read_resource(uri: AnyUrl) -> str:
    """Read resource contents."""
    client = get_clickhouse_client()
    uri_str = str(uri)
    logger.info(f"Reading resource: {uri_str}")

    if not uri_str.startswith("clickhouse://"):
        raise ValueError(f"Invalid URI scheme: {uri_str}")

    parts = uri_str[len("clickhouse://") :].split("/")
    if len(parts) == 2 and parts[1] == "tables":
        database = parts[0]
        result = client.query(f"SHOW TABLES FROM {database}")
        return "\n".join(row[0] for row in result.result_rows)

    elif len(parts) == 3 and parts[2] == "schema":
        database, table = parts[0], parts[1]
        result = client.query(f"DESCRIBE TABLE {database}.{table}")
        schema = [f"{row[0]} - {row[1]}" for row in result.result_rows]
        return "\n".join(schema)

    else:
        raise ValueError(f"Invalid resource URI: {uri_str}")


@app.list_tools()
async def list_tools() -> list[Tool]:
    """List available ClickHouse tools."""
    logger.info("Listing tools...")
    return [
        Tool(
            name="execute_select_query",
            description="Execute a SELECT query on the ClickHouse server",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The SELECT query to execute",
                    }
                },
                "required": ["query"],
            },
        )
    ]


@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    """Execute SELECT queries."""
    logger.info(f"Calling tool: {name} with arguments: {arguments}")

    if name != "execute_select_query":
        raise ValueError(f"Unknown tool: {name}")

    query = arguments.get("query")
    if not query:
        raise ValueError("Query is required")

    if not query.strip().upper().startswith("SELECT"):
        return [TextContent(type="text", text="Error: Only SELECT queries are allowed.")]

    try:
        client = get_clickhouse_client()
        result = client.query(query)

        output = []
        output.append("\t".join(result.column_names))
        for row in result.result_rows:
            output.append("\t".join(str(value) for value in row))

        return [TextContent(type="text", text="\n".join(output))]
    except Exception as e:
        logger.error(f"Error executing SQL '{query}': {e}")
        return [TextContent(type="text", text=f"Error executing query: {str(e)}")]


async def main():
    """Main entry point to run the MCP server."""
    from mcp.server.stdio import stdio_server

    logger.info("Starting ClickHouse MCP server...")

    async with stdio_server() as (read_stream, write_stream):
        try:
            await app.run(read_stream, write_stream, app.create_initialization_options())
        except Exception as e:
            logger.error(f"Server error: {str(e)}", exc_info=True)
            raise


if __name__ == "__main__":
    asyncio.run(main())
