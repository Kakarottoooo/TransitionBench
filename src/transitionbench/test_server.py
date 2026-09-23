"""Real local HTTP test fixture. Not a model and not GPU evidence."""
import asyncio
import json
import re
import os
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse, JSONResponse

app = FastAPI(title="TransitionBench local protocol fixture")


@app.get("/healthz")
def health():
    return {"status": "ok", "model": False, "config_id": os.environ.get("TB_FIXTURE_CONFIG", "A"),
            "generation": int(os.environ.get("TB_FIXTURE_GENERATION", "0")), "process_id": os.getpid()}


@app.get("/v1/models")
def models():
    return {"data": [{"id": "local-arithmetic", "zdr_supported": True}]}


@app.post("/v1/chat/completions")
async def complete(request: Request):
    body = await request.json()
    prompt = body["messages"][-1]["content"]
    marker = re.search(r"Return exactly (TB:[\w-]+:4|TB-WARM-[\w-]+:4)", prompt)
    answer = marker.group(1) if marker else "4"
    if body.get("stream"):
        async def stream():
            chunks = [{"role": "assistant", "content": ""}, {"content": answer[:3]}, {"content": answer[3:]}]
            for delta in chunks:
                packet = "data: " + json.dumps({"id": "local-response", "model": "local-arithmetic", "choices": [{"index": 0, "delta": delta}]}) + "\n\n"
                # Split inside JSON to exercise transport fragmentation.
                yield packet[:17]
                await asyncio.sleep(.005)
                yield packet[17:]
            yield 'data: {"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}\n\n'
            yield "data: [DONE]\n\n"
        return StreamingResponse(stream(), media_type="text/event-stream", headers={"x-request-id": "local-fixture"})
    await asyncio.sleep(.015)
    return JSONResponse({"id": "local-response", "model": "local-arithmetic", "choices": [{"message": {"content": answer}, "finish_reason": "stop"}]}, headers={"x-request-id": "local-fixture"})
