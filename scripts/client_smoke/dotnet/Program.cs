// StackExchange.Redis runner: run.py's contract (host port protocol cases.json).
using System.Text.Json;
using System.Text.Json.Nodes;
using StackExchange.Redis;

var options = new ConfigurationOptions
{
    EndPoints = { { args[0], int.Parse(args[1]) } },
    Protocol = args[2] == "3" ? RedisProtocol.Resp3 : RedisProtocol.Resp2,
    AllowAdmin = true, // FLUSHALL
    AbortOnConnectFail = true,
    SyncTimeout = 10000,
};
using var mux = ConnectionMultiplexer.Connect(options);
var db = mux.GetDatabase();
var cases = JsonNode.Parse(File.ReadAllText(args[3]))!.AsArray();
var version = typeof(ConnectionMultiplexer).Assembly
    .GetCustomAttributes(typeof(System.Reflection.AssemblyInformationalVersionAttribute), false)
    .OfType<System.Reflection.AssemblyInformationalVersionAttribute>().FirstOrDefault()?.InformationalVersion.Split('+')[0] ?? "?";
Console.WriteLine(new JsonObject { ["client"] = "StackExchange.Redis", ["version"] = version }.ToJsonString());

// A reply as JSON: a map as {"map": [key, value] pairs}, a set as {"set": sorted items}, and a simple
// string, a verbatim string and a big number each tagged with its kind; a bulk string is a string.
static JsonNode? Norm(RedisResult r)
{
    if (r.IsNull) return null;
    switch (r.Resp3Type)
    {
        case ResultType.Array:
        case ResultType.Push:
            return new JsonArray(((RedisResult[])r!).Select(Norm).ToArray());
        case ResultType.Set:
            return new JsonObject { ["set"] = new JsonArray(((RedisResult[])r!).Select(Norm).OrderBy(n => n?.ToJsonString(), StringComparer.Ordinal).ToArray()) };
        case ResultType.Map:
            var items = (RedisResult[])r!;
            var pairs = new JsonArray();
            for (var i = 0; i + 1 < items.Length; i += 2) pairs.Add(new JsonArray(Norm(items[i]), Norm(items[i + 1])));
            return new JsonObject { ["map"] = pairs };
        case ResultType.Integer:
            return JsonValue.Create((long)r);
        case ResultType.Double:
            var d = (double)r;
            return double.IsFinite(d) ? JsonValue.Create(d) : JsonValue.Create(d.ToString());
        case ResultType.Boolean:
            return JsonValue.Create((bool)r);
        case ResultType.SimpleString:
            return new JsonObject { ["simple"] = r.ToString() };
        case ResultType.VerbatimString:
            return new JsonObject { ["verbatim"] = r.ToString() };
        case ResultType.BigInteger:
            return new JsonObject { ["bignum"] = r.ToString() };
        default:
            return JsonValue.Create(r.ToString());
    }
}

foreach (var c in cases)
{
    var cmd = c!["cmd"]!.AsArray().Select(a => (object)a!.GetValue<string>()).ToArray();
    var line = new JsonObject { ["case"] = c["name"]!.GetValue<string>() };
    try
    {
        line["reply"] = Norm(db.Execute((string)cmd[0], cmd.Skip(1).ToArray()));
    }
    catch (RedisServerException e)
    {
        line["error"] = e.Message.Split(' ')[0];
    }
    catch (Exception e) when (e is ArgumentException or RedisCommandException)
    {
        line["error"] = "client refused the command";
    }
    Console.WriteLine(line.ToJsonString(new JsonSerializerOptions { Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping }));
}
