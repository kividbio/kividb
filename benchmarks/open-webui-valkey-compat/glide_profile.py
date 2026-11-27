#!/usr/bin/env python3
"""FT.PROFILE through valkey-glide-sync: the conversion has to succeed.

usage: glide_profile.py PORT

valkey-glide converts an FT.PROFILE reply itself: an array of two, the search's reply and a map of
names to numbers. KiviDB answers a GLIDE connection that way (docs/CONFIGURATION.md). valkey-search
1.2 has no FT.PROFILE, so this is not part of the checks that run against valkey-bundle. Run it with
the venv ci.sh setup made:  DIR/venv/bin/python glide_profile.py PORT
"""
import struct
import sys
import time

from glide_sync import (
    FtCreateOptions,
    FtProfileOptions,
    FtSearchOptions,
    GlideClient,
    GlideClientConfiguration,
    NodeAddress,
    DataType,
    DistanceMetricType,
    TagField,
    VectorAlgorithm,
    VectorField,
    VectorFieldAttributesHnsw,
    VectorType,
    ft,
)

port = int(sys.argv[1])
client = GlideClient.create(GlideClientConfiguration([NodeAddress("127.0.0.1", port)]))
vec = lambda *f: struct.pack(f"<{len(f)}f", *f)  # noqa: E731
ft.create(
    client,
    "profile-idx",
    [
        VectorField(
            "v",
            VectorAlgorithm.HNSW,
            VectorFieldAttributesHnsw(2, DistanceMetricType.L2, VectorType.FLOAT32),
        ),
        TagField("t"),
    ],
    FtCreateOptions(DataType.HASH, ["p:"]),
)
client.hset("p:1", {"v": vec(0, 0), "t": "a"})
client.hset("p:2", {"v": vec(1, 0), "t": "b"})
time.sleep(1.5)
options = FtProfileOptions.from_query_options(
    "*=>[KNN 2 @v $q]", FtSearchOptions(params={"q": vec(0, 0)})
)
reply = ft.profile(client, "profile-idx", options)
print(reply)
assert isinstance(reply, list) and len(reply) == 2, reply
found, stats = reply
assert found[0] == 2, found
assert isinstance(stats, dict) and stats, stats
assert all(isinstance(v, (int, float)) for v in stats.values()), stats
print("FT.PROFILE converts:", sorted(stats))
ft.dropindex(client, "profile-idx")
