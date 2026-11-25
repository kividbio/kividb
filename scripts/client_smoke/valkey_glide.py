"""valkey-glide runner (Python package): run.py's contract (host port protocol cases.json)."""

import asyncio
import json
import sys
from importlib.metadata import version

from glide import GlideClient, GlideClientConfiguration, NodeAddress, ProtocolVersion, RequestError


def norm(v):
    if isinstance(v, bytes):
        return v.decode("utf-8", "backslashreplace")
    if isinstance(v, dict):
        return [[norm(k), norm(x)] for k, x in v.items()]
    if isinstance(v, (list, tuple)):
        return [norm(x) for x in v]
    if isinstance(v, set):
        return sorted((norm(x) for x in v), key=json.dumps)
    return v


def code(message):
    """The error's code. Glide writes "WRONGTYPE: ..." for some, drops "ERR" from others
    ("An error was signalled by the server: - ResponseError: ..."), and says "Response
    couldn't be converted" when it cannot read a reply."""
    if message.startswith("Response couldn't be converted"):
        return "client could not convert the reply"
    if message.startswith("An error was signalled by the server"):
        return "ERR"
    return message.split(" ")[0].rstrip(":")


async def main(host, port, protocol, cases):
    client = await GlideClient.create(GlideClientConfiguration(
        [NodeAddress(host, int(port))],
        protocol=ProtocolVersion.RESP3 if protocol == "3" else ProtocolVersion.RESP2,
        request_timeout=10000,
    ))
    print(json.dumps({"client": "valkey-glide", "version": version("valkey-glide")}))
    for case in json.load(open(cases)):
        try:
            out = {"reply": norm(await client.custom_command(case["cmd"]))}
        except RequestError as e:
            out = {"error": code(str(e))}
        print(json.dumps({"case": case["name"], **out}, ensure_ascii=False))
    await client.close()


asyncio.run(main(*sys.argv[1:5]))
