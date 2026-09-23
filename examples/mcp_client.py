import asyncio
import sys
import os
from mcp import ClientSession,StdioServerParameters
from mcp.client.stdio import stdio_client

async def main():
    params=StdioServerParameters(command=sys.executable,args=['-m','transitionbench.mcp_server'],env={'TRANSITIONBENCH_API':os.environ.get('TRANSITIONBENCH_API','http://127.0.0.1:8765')})
    async with stdio_client(params) as (read,write):
        async with ClientSession(read,write) as session:
            response=await session.initialize()
            assert response.protocolVersion=='2025-11-25'
            result=await session.call_tool('list_capabilities',{})
            assert not result.isError
            print(result.model_dump_json())

asyncio.run(main())
