from __future__ import annotations

import zlib
from collections.abc import Awaitable, Callable

from fastapi import HTTPException, Request, Response
from fastapi.routing import APIRoute

from packages.contracts.gateway.requests import MAX_EVIDENCE_PAYLOAD_BYTES
from packages.contracts.gateway.routes import AGENT_EVIDENCE_PATH

MAX_REQUEST_BYTES = MAX_EVIDENCE_PAYLOAD_BYTES + 65_536


class EvidenceRequest(Request):
    # cost: time O(n), heap O(n), stack O(1)
    # vars: n = bounded decompressed request bytes
    # basis: estimate
    async def body(self) -> bytes:
        if hasattr(self, "_body"):
            return self._body
        encoding = self.headers.get("content-encoding", "identity").lower()
        if encoding not in {"identity", "gzip"}:
            raise HTTPException(415, "unsupported evidence encoding")
        decoder = zlib.decompressobj(16 + zlib.MAX_WBITS) if encoding == "gzip" else None
        result = bytearray()
        received = 0
        try:
            async for chunk in self.stream():
                received += len(chunk)
                if received > MAX_REQUEST_BYTES:
                    raise HTTPException(413, "evidence request too large")
                result.extend(
                    decoder.decompress(chunk, MAX_REQUEST_BYTES + 1 - len(result))
                    if decoder
                    else chunk
                )
                if len(result) > MAX_REQUEST_BYTES:
                    raise HTTPException(413, "evidence request too large")
            if decoder and (not decoder.eof or decoder.unused_data):
                raise HTTPException(400, "invalid gzip evidence")
        except zlib.error as exc:
            raise HTTPException(400, "invalid gzip evidence") from exc
        self._body = bytes(result)
        return self._body


class EvidenceRoute(APIRoute):
    def get_route_handler(self) -> Callable[[Request], Awaitable[Response]]:
        handler = super().get_route_handler()

        async def receive(request: Request) -> Response:
            if self.path == AGENT_EVIDENCE_PATH:
                request = EvidenceRequest(request.scope, request.receive)
            return await handler(request)

        return receive
