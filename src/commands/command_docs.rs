// `COMMAND DOCS`: summary, group, complexity and since, for redis-cli and
// clients that ask on connect. The command used to answer with an empty
// array so those clients would still start; they now get the fields they
// read. Argument trees are omitted: `COMMAND` / `COMMAND INFO` already
// carry arity and key positions.

use crate::resp::write::{resp_bulk_str, resp_map_for};

pub(crate) fn reply(resp: u8, requested: &[&[u8]], all: &[&str]) -> Vec<u8> {
    let names: Vec<String> = if requested.is_empty() {
        all.iter().map(|n| n.to_ascii_lowercase()).collect()
    } else {
        requested
            .iter()
            .filter_map(|raw| {
                let name = std::str::from_utf8(raw).ok()?.to_ascii_uppercase();
                all.iter()
                    .find(|n| n.eq_ignore_ascii_case(&name))
                    .map(|n| n.to_ascii_lowercase())
            })
            .collect()
    };
    let mut pairs = Vec::with_capacity(names.len() * 2);
    for name in names {
        let upper = name.to_ascii_uppercase();
        pairs.push(resp_bulk_str(&name));
        pairs.push(one(resp, &upper));
    }
    resp_map_for(resp, pairs)
}

fn one(resp: u8, name: &str) -> Vec<u8> {
    resp_map_for(
        resp,
        vec![
            resp_bulk_str("summary"),
            resp_bulk_str(summary(name)),
            resp_bulk_str("since"),
            resp_bulk_str(since(name)),
            resp_bulk_str("group"),
            resp_bulk_str(group(name)),
            resp_bulk_str("complexity"),
            resp_bulk_str(complexity(name)),
        ],
    )
}

fn group(name: &str) -> &'static str {
    if name.starts_with("FT.") {
        return "search";
    }
    if name.starts_with("JSON.") {
        return "json";
    }
    if name.starts_with("TS.") {
        return "timeseries";
    }
    if name.starts_with("AR") {
        return "array";
    }
    if name.starts_with("GEO") {
        return "geo";
    }
    if name.starts_with("PF") {
        return "hyperloglog";
    }
    match name {
        "VADD" | "VCARD" | "VDIM" | "VEMB" | "VGETATTR" | "VINFO" | "VISMEMBER" | "VLINKS"
        | "VRANDMEMBER" | "VRANGE" | "VREM" | "VSETATTR" | "VSIM" => "vectorset",
        "APPEND" | "DECR" | "DECRBY" | "DELEX" | "DIGEST" | "GET" | "GETDEL" | "GETEX"
        | "GETRANGE" | "GETSET" | "INCR" | "INCRBY" | "INCRBYFLOAT" | "INCREX" | "LCS" | "MGET"
        | "MSET" | "MSETNX" | "MSETEX" | "PSETEX" | "SET" | "SETEX" | "SETNX" | "SETRANGE"
        | "STRLEN" | "SUBSTR" => "string",
        "BLMOVE" | "BLMOVEM" | "BLMPOP" | "BLPOP" | "BRPOP" | "BRPOPLPUSH" | "LINDEX"
        | "LINSERT" | "LLEN" | "LMOVE" | "LMOVEM" | "LMPOP" | "LPOP" | "LPOS" | "LPUSH"
        | "LPUSHX" | "LRANGE" | "LREM" | "LSET" | "LTRIM" | "RPOP" | "RPOPLPUSH" | "RPUSH"
        | "RPUSHX" => "list",
        "BZMPOP" | "BZPOPMAX" | "BZPOPMIN" | "ZADD" | "ZCARD" | "ZCOUNT" | "ZDIFF"
        | "ZDIFFSTORE" | "ZINCRBY" | "ZINTER" | "ZINTERCARD" | "ZINTERSTORE" | "ZLEXCOUNT"
        | "ZMPOP" | "ZMSCORE" | "ZPOPMAX" | "ZPOPMIN" | "ZRANDMEMBER" | "ZRANGE"
        | "ZRANGEBYLEX" | "ZRANGEBYSCORE" | "ZRANGESTORE" | "ZRANK" | "ZREM" | "ZREMRANGEBYLEX"
        | "ZREMRANGEBYRANK" | "ZREMRANGEBYSCORE" | "ZREVRANGE" | "ZREVRANGEBYLEX"
        | "ZREVRANGEBYSCORE" | "ZREVRANK" | "ZSCAN" | "ZSCORE" | "ZUNION" | "ZUNIONSTORE" => {
            "sorted-set"
        }
        "HDEL" | "HEXISTS" | "HEXPIRE" | "HEXPIREAT" | "HEXPIRETIME" | "HGET" | "HGETALL"
        | "HGETDEL" | "HGETEX" | "HIMPORT" | "HINCRBY" | "HINCRBYFLOAT" | "HKEYS" | "HLEN"
        | "HMGET" | "HMSET" | "HPERSIST" | "HPEXPIRE" | "HPEXPIREAT" | "HPEXPIRETIME" | "HPTTL"
        | "HRANDFIELD" | "HSCAN" | "HSET" | "HSETEX" | "HSETNX" | "HSTRLEN" | "HTTL" | "HVALS" => {
            "hash"
        }
        "SADD" | "SCARD" | "SDIFF" | "SDIFFCARD" | "SDIFFSTORE" | "SINTER" | "SINTERCARD"
        | "SINTERSTORE" | "SISMEMBER" | "SMEMBERS" | "SMISMEMBER" | "SMOVE" | "SPOP"
        | "SRANDMEMBER" | "SREM" | "SSCAN" | "SUNION" | "SUNIONCARD" | "SUNIONSTORE" => "set",
        "XACK" | "XACKDEL" | "XADD" | "XAUTOCLAIM" | "XCFGSET" | "XCLAIM" | "XDEL" | "XDELEX"
        | "XGROUP" | "XIDMPRECORD" | "XINFO" | "XLEN" | "XNACK" | "XPENDING" | "XRANGE"
        | "XREAD" | "XREADGROUP" | "XREVRANGE" | "XSETID" | "XTRIM" => "stream",
        "BITCOUNT" | "BITFIELD" | "BITFIELD_RO" | "BITOP" | "BITPOS" | "GETBIT" | "SETBIT" => {
            "bitmap"
        }
        "PSUBSCRIBE" | "PUBLISH" | "PUBSUB" | "PUNSUBSCRIBE" | "SPUBLISH" | "SSUBSCRIBE"
        | "SUBSCRIBE" | "SUNSUBSCRIBE" | "UNSUBSCRIBE" => "pubsub",
        "EVAL" | "EVALSHA" | "EVALSHA_RO" | "EVAL_RO" | "FCALL" | "FCALL_RO" | "FUNCTION"
        | "SCRIPT" => "scripting",
        "DISCARD" | "EXEC" | "MULTI" | "UNWATCH" | "WATCH" => "transactions",
        "ASKING" | "CLUSTER" | "READONLY" | "READWRITE" => "cluster",
        "AUTH" | "CLIENT" | "ECHO" | "HELLO" | "PING" | "QUIT" | "RESET" | "SELECT" => "connection",
        "COPY" | "DEL" | "DUMP" | "EXISTS" | "EXPIRE" | "EXPIREAT" | "EXPIRETIME" | "KEYS"
        | "MOVE" | "OBJECT" | "PERSIST" | "PEXPIRE" | "PEXPIREAT" | "PEXPIRETIME" | "PTTL"
        | "RANDOMKEY" | "RENAME" | "RENAMENX" | "RESTORE" | "SCAN" | "SORT" | "SORT_RO"
        | "TOUCH" | "TTL" | "TYPE" | "UNLINK" | "WAIT" | "WAITAOF" => "generic",
        _ => "server",
    }
}

fn complexity(name: &str) -> &'static str {
    match name {
        "AUTH" | "CLIENT" | "DBSIZE" | "ECHO" | "EXISTS" | "GET" | "GETBIT" | "GETDEL"
        | "HELLO" | "HEXISTS" | "HGET" | "HLEN" | "HSTRLEN" | "INCR" | "INCRBY" | "LLEN"
        | "PING" | "PTTL" | "QUIT" | "SCARD" | "SELECT" | "SET" | "SETBIT" | "SETNX"
        | "SISMEMBER" | "STRLEN" | "TIME" | "TTL" | "TYPE" | "ZCARD" | "ZSCORE" => "O(1)",
        "ZADD" | "ZINCRBY" | "ZRANK" | "ZREM" | "ZREVRANK" => "O(log(N))",
        _ => "O(N)",
    }
}

fn since(name: &str) -> &'static str {
    if name.starts_with("AR") {
        return "8.8.0";
    }
    match name {
        "DIGEST" | "DELEX" | "MSETEX" | "INCREX" => "8.4.0",
        "HOTKEYS" => "8.6.0",
        "LMOVEM" | "BLMOVEM" | "SUNIONCARD" | "SDIFFCARD" | "BACKUP" | "HIMPORT"
        | "FT.ALIASLIST" | "FT.HYBRID" => "8.10.0",
        _ => "1.0.0",
    }
}

fn summary(name: &str) -> &'static str {
    match name {
        "ACL" => "Manage Access Control Lists",
        "APPEND" => "Append a value to a key",
        "ARCOUNT" => "Number of non-empty array elements",
        "ARDEL" => "Delete array elements at the given indices",
        "ARDELRANGE" => "Delete array elements in inclusive index ranges",
        "ARGET" => "Get the array value at an index",
        "ARGETRANGE" => "Get every array slot in a range, including empty ones",
        "ARGREP" => "Search array elements by a textual predicate",
        "ARINFO" => "Array metadata",
        "ARINSERT" => "Insert values at the array cursor",
        "ARLASTITEMS" => "The most recently inserted array elements",
        "ARLEN" => "Highest occupied array index plus one",
        "ARMGET" => "Get array values at several indices",
        "ARMSET" => "Set several array index-value pairs",
        "ARNEXT" => "The index the next ARINSERT would use",
        "AROP" => "Aggregate non-empty array elements in a range",
        "ARRING" => "Insert into a fixed-size array ring buffer",
        "ARSCAN" => "Iterate existing array elements in a range",
        "ARSEEK" => "Move the array insert cursor",
        "ARSET" => "Set contiguous array values starting at an index",
        "ASKING" => "Allow a command on a slot being imported",
        "AUTH" => "Authenticate to the server",
        "BACKUP" => "Managed on-disk backups",
        "BGREWRITEAOF" => "Asynchronously rewrite the append-only file",
        "BGSAVE" => "Asynchronously save the dataset to disk",
        "BITCOUNT" => "Count set bits in a string",
        "BITFIELD" => "Perform arbitrary bitfield integer operations on strings",
        "BITFIELD_RO" => "Read-only BITFIELD",
        "BITOP" => "Perform bitwise operations between strings",
        "BITPOS" => "Find first bit set or clear in a string",
        "BLMOVE" => "Pop an element from a list, push it to another, and block",
        "BLMOVEM" => "Blocking LMOVEM",
        "BLMPOP" => "Pop elements from the first non-empty list, and block",
        "BLPOP" => "Remove and get the first element in a list, or block",
        "BRPOP" => "Remove and get the last element in a list, or block",
        "BRPOPLPUSH" => "Pop from one list, push to another, and block",
        "BZMPOP" => "Remove and return members with scores from sorted sets, and block",
        "BZPOPMAX" => "Remove and return the member with the highest score, and block",
        "BZPOPMIN" => "Remove and return the member with the lowest score, and block",
        "CLIENT" => "Manage client connections",
        "CLUSTER" => "Cluster management",
        "COMMAND" => "Get command details",
        "CONFIG" => "Read or rewrite the server configuration",
        "COPY" => "Copy a key",
        "DBSIZE" => "Return the number of keys in the selected database",
        "DEBUG" => "Debugging commands",
        "DECR" => "Decrement the integer value of a key by one",
        "DECRBY" => "Decrement the integer value of a key by the given number",
        "DEL" => "Delete one or more keys",
        "DELEX" => "Delete a key, optionally only if its value matches",
        "DIGEST" => "Return the digest of a string value",
        "DISCARD" => "Discard a transaction",
        "DUMP" => "Return a serialized value",
        "ECHO" => "Echo the given string",
        "EVAL" => "Execute a Lua script",
        "EVALSHA" => "Execute a Lua script by its SHA1",
        "EVALSHA_RO" => "Read-only EVALSHA",
        "EVAL_RO" => "Read-only EVAL",
        "EXEC" => "Execute all commands issued after MULTI",
        "EXISTS" => "Determine if a key exists",
        "EXPIRE" => "Set a key's time to live in seconds",
        "EXPIREAT" => "Set the expiration for a key as a Unix timestamp",
        "EXPIRETIME" => "Unix timestamp at which a key will expire",
        "FAILOVER" => "Not supported: Sentinel failover",
        "FCALL" => "Call a function",
        "FCALL_RO" => "Read-only FCALL",
        "FLUSHALL" => "Remove all keys from all databases",
        "FLUSHDB" => "Remove all keys from the current database",
        "FT.AGGREGATE" => "Run an aggregation on the documents a query matches",
        "FT.ALIASADD" => "Add an alias for a search index",
        "FT.ALIASDEL" => "Remove a search index alias",
        "FT.ALIASLIST" => "List aliases that point to a search index",
        "FT.ALIASUPDATE" => "Update a search index alias",
        "FT.ALTER" => "Add fields to a search index",
        "FT.CONFIG" => "Read or set query-engine settings",
        "FT.CREATE" => "Create a search index",
        "FT.CURSOR" => "Read or close an aggregation cursor",
        "FT.DEBUG" => "Search-engine debug commands",
        "FT.DICTADD" => "Add terms to a named dictionary",
        "FT.DICTDEL" => "Remove terms from a named dictionary",
        "FT.DICTDUMP" => "List terms in a named dictionary",
        "FT.DROPINDEX" => "Delete a search index",
        "FT.EXPLAIN" => "Return the plan of a search query",
        "FT.EXPLAINCLI" => "Return the plan of a search query as lines",
        "FT.HYBRID" => "Combined text and vector search with score fusion",
        "FT.INFO" => "Search index metadata",
        "FT.PROFILE" => "Run a search query and return it with timings",
        "FT.SEARCH" => "Search an index",
        "FT.SPELLCHECK" => "Suggest spellings for unrecognised query terms",
        "FT.SYNDUMP" => "Dump synonym groups of a search index",
        "FT.SYNUPDATE" => "Create or update a synonym group",
        "FT.TAGVALS" => "Distinct values of a TAG field",
        "FT._LIST" => "List search index names",
        "FUNCTION" => "Manage function libraries",
        "GEOADD" => "Add one or more geospatial items",
        "GEODELBYMEMBER" => "Remove geospatial members",
        "GEODIST" => "Distance between two geospatial members",
        "GEOHASH" => "Geohash strings of geospatial members",
        "GEOPOS" => "Positions of geospatial members",
        "GEORADIUS" => "Query a geospatial index by radius",
        "GEORADIUSBYMEMBER" => "Query a geospatial index by radius from a member",
        "GEORADIUSBYMEMBER_RO" => "Read-only GEORADIUSBYMEMBER",
        "GEORADIUS_RO" => "Read-only GEORADIUS",
        "GEOSEARCH" => "Query a geospatial index",
        "GEOSEARCHSTORE" => "Query a geospatial index and store the result",
        "GET" => "Get the value of a key",
        "GETBIT" => "Return the bit value at offset in a string",
        "GETDEL" => "Get the value of a key and delete it",
        "GETEX" => "Get the value of a key and optionally set its expiry",
        "GETRANGE" => "Get a substring of the string stored at a key",
        "GETSET" => "Set a key and return its old value",
        "HDEL" => "Delete one or more hash fields",
        "HELLO" => "Handshake with the server",
        "HEXISTS" => "Determine if a hash field exists",
        "HEXPIRE" => "Set a timeout on hash fields, in seconds",
        "HEXPIREAT" => "Set a timeout on hash fields as a Unix timestamp",
        "HEXPIRETIME" => "Unix timestamp at which hash fields expire",
        "HGET" => "Get the value of a hash field",
        "HGETALL" => "Get all fields and values in a hash",
        "HGETDEL" => "Get hash fields and delete them",
        "HGETEX" => "Get hash fields and optionally set their expiry",
        "HIMPORT" => "Import a hash from a prepared dump",
        "HINCRBY" => "Increment the integer value of a hash field",
        "HINCRBYFLOAT" => "Increment the float value of a hash field",
        "HKEYS" => "Get all field names in a hash",
        "HLEN" => "Number of fields in a hash",
        "HMGET" => "Get the values of all the given hash fields",
        "HMSET" => "Set multiple hash fields",
        "HPERSIST" => "Remove the expiry from hash fields",
        "HPEXPIRE" => "Set a timeout on hash fields, in milliseconds",
        "HPEXPIREAT" => "Set a timeout on hash fields as a Unix timestamp in milliseconds",
        "HPEXPIRETIME" => "Unix timestamp in milliseconds at which hash fields expire",
        "HPTTL" => "Millisecond TTL of hash fields",
        "HRANDFIELD" => "Get one or more random fields from a hash",
        "HSCAN" => "Incrementally iterate hash fields and values",
        "HSET" => "Set the string value of a hash field",
        "HSETEX" => "Set hash fields with an expiry",
        "HSETNX" => "Set a hash field only if it does not exist",
        "HSTRLEN" => "Length of a hash field's value",
        "HTTL" => "TTL of hash fields, in seconds",
        "HVALS" => "Get all values in a hash",
        "HOTKEYS" => "Track the keys that cost the most",
        "INCR" => "Increment the integer value of a key by one",
        "INCRBY" => "Increment the integer value of a key by the given amount",
        "INCRBYFLOAT" => "Increment the float value of a key by the given amount",
        "INCREX" => "Atomic increment with bounds and expiry",
        "INFO" => "Get information and statistics about the server",
        "JSON.ARRAPPEND" => "Append values to JSON arrays",
        "JSON.ARRINDEX" => "Index of a value in JSON arrays",
        "JSON.ARRINSERT" => "Insert values into JSON arrays",
        "JSON.ARRLEN" => "Length of JSON arrays",
        "JSON.ARRPOP" => "Remove and return an element from JSON arrays",
        "JSON.ARRTRIM" => "Trim JSON arrays to a range",
        "JSON.CLEAR" => "Clear JSON arrays and objects, and zero numbers",
        "JSON.DEBUG" => "JSON debug commands",
        "JSON.DEL" => "Delete JSON values",
        "JSON.FORGET" => "Alias for JSON.DEL",
        "JSON.GET" => "Get JSON values at a path",
        "JSON.MERGE" => "Merge a JSON value at a path",
        "JSON.MGET" => "Get a JSON path from several keys",
        "JSON.MSET" => "Set several JSON key-path-value triples",
        "JSON.NUMINCRBY" => "Add to JSON numbers",
        "JSON.NUMMULTBY" => "Multiply JSON numbers",
        "JSON.OBJKEYS" => "Object keys at a JSON path",
        "JSON.OBJLEN" => "Number of members of JSON objects",
        "JSON.RESP" => "JSON values as RESP",
        "JSON.SET" => "Set JSON values at a path",
        "JSON.STRAPPEND" => "Append to JSON strings",
        "JSON.STRLEN" => "Length of JSON strings",
        "JSON.TOGGLE" => "Flip JSON booleans",
        "JSON.TYPE" => "Type of JSON values at a path",
        "KEYS" => "Find all keys matching a pattern",
        "LASTSAVE" => "Unix timestamp of the last successful save",
        "LATENCY" => "Latency monitoring",
        "LCS" => "Longest common subsequence of two strings",
        "LINDEX" => "Get an element from a list by its index",
        "LINSERT" => "Insert an element before or after another in a list",
        "LLEN" => "Get the length of a list",
        "LMOVE" => "Pop an element from a list and push it to another",
        "LMOVEM" => "Move several elements between lists",
        "LMPOP" => "Pop elements from the first non-empty list",
        "LOLWUT" => "Server version as ASCII art",
        "LPOP" => "Remove and get the first elements in a list",
        "LPOS" => "Return the index of matching elements in a list",
        "LPUSH" => "Prepend one or more values to a list",
        "LPUSHX" => "Prepend values to a list, only if the list exists",
        "LRANGE" => "Get a range of elements from a list",
        "LREM" => "Remove elements from a list",
        "LSET" => "Set the value of an element in a list by its index",
        "LTRIM" => "Trim a list to the specified range",
        "MEMORY" => "Memory introspection",
        "MGET" => "Get the values of all the given keys",
        "MIGRATE" => "Atomically transfer a key to another instance",
        "MODULE" => "List built-in command families",
        "MONITOR" => "Listen for every command processed by the server",
        "MOVE" => "Move a key to another database",
        "MSET" => "Set multiple keys to multiple values",
        "MSETNX" => "Set multiple keys to multiple values, only if none of the keys exist",
        "MSETEX" => "Set multiple keys with a shared expiry",
        "MULTI" => "Mark the start of a transaction block",
        "OBJECT" => "Inspect the internals of Redis objects",
        "PERSIST" => "Remove the expiration from a key",
        "PEXPIRE" => "Set a key's time to live in milliseconds",
        "PEXPIREAT" => "Set the expiration for a key as a Unix timestamp in milliseconds",
        "PEXPIRETIME" => "Unix timestamp in milliseconds at which a key will expire",
        "PFADD" => "Add elements to a HyperLogLog",
        "PFCOUNT" => "Approximate cardinality of HyperLogLog keys",
        "PFDEBUG" => "HyperLogLog debug commands",
        "PFMERGE" => "Merge HyperLogLog values",
        "PFSELFTEST" => "HyperLogLog self-test",
        "PING" => "Ping the server",
        "PSETEX" => "Set the value and expiration in milliseconds of a key",
        "PSUBSCRIBE" => "Listen for messages published to channels matching patterns",
        "PSYNC" => "Internal command used for replication",
        "PTTL" => "Get the time to live for a key in milliseconds",
        "PUBLISH" => "Post a message to a channel",
        "PUBSUB" => "Inspect the Pub/Sub system",
        "PUNSUBSCRIBE" => "Stop listening for messages posted to channels matching patterns",
        "QUIT" => "Close the connection",
        "RANDOMKEY" => "Return a random key from the keyspace",
        "READONLY" => "Enable read-only queries for a connection to a replica node",
        "READWRITE" => "Enable read/write queries for a connection to a replica node",
        "RENAME" => "Rename a key",
        "RENAMENX" => "Rename a key, only if the new key does not exist",
        "REPLCONF" => "Internal command used for replication",
        "REPLICAOF" => "Make the server a replica of another instance, or promote it",
        "RESET" => "Reset the connection",
        "RESTORE" => "Create a key using a serialized value from DUMP",
        "ROLE" => "Role of the instance in replication",
        "RPOP" => "Remove and get the last elements in a list",
        "RPOPLPUSH" => "Remove the last element in a list, prepend it to another",
        "RPUSH" => "Append one or more values to a list",
        "RPUSHX" => "Append values to a list, only if the list exists",
        "SADD" => "Add one or more members to a set",
        "SAVE" => "Synchronously save the dataset to disk",
        "SCAN" => "Incrementally iterate the keyspace",
        "SCARD" => "Get the number of members in a set",
        "SCRIPT" => "Manage the script cache",
        "SDIFF" => "Subtract multiple sets",
        "SDIFFCARD" => "Cardinality of the difference of multiple sets",
        "SDIFFSTORE" => "Subtract multiple sets and store the resulting set",
        "SELECT" => "Change the selected database",
        "SET" => "Set the string value of a key",
        "SETBIT" => "Sets or clears the bit at offset in the string value",
        "SETEX" => "Set the value and expiration of a key",
        "SETNX" => "Set the value of a key, only if the key does not exist",
        "SETRANGE" => "Overwrite part of a string at key starting at the specified offset",
        "SHUTDOWN" => "Synchronously save the dataset and shut down the server",
        "SINTER" => "Intersect multiple sets",
        "SINTERCARD" => "Cardinality of the intersection of multiple sets",
        "SINTERSTORE" => "Intersect multiple sets and store the resulting set",
        "SISMEMBER" => "Determine if a given value is a member of a set",
        "SLAVEOF" => "Make the server a replica of another instance, or promote it",
        "SLOWLOG" => "Manage the slow log",
        "SMEMBERS" => "Get all the members in a set",
        "SMISMEMBER" => "Determine whether multiple values are members of a set",
        "SMOVE" => "Move a member from one set to another",
        "SORT" => "Sort the elements in a list, set or sorted set",
        "SORT_RO" => "Read-only SORT",
        "SPOP" => "Remove and return one or more random members from a set",
        "SPUBLISH" => "Post a message to a shard channel",
        "SRANDMEMBER" => "Get one or multiple random members from a set",
        "SREM" => "Remove one or more members from a set",
        "SSCAN" => "Incrementally iterate set elements",
        "SSUBSCRIBE" => "Listen for messages published to shard channels",
        "STRLEN" => "Get the length of the value stored in a key",
        "SUBSCRIBE" => "Listen for messages published to the given channels",
        "SUBSTR" => "Get a substring of the string stored at a key",
        "SUNION" => "Add multiple sets",
        "SUNIONCARD" => "Cardinality of the union of multiple sets",
        "SUNIONSTORE" => "Add multiple sets and store the resulting set",
        "SUNSUBSCRIBE" => "Stop listening for messages posted to shard channels",
        "SWAPDB" => "Swap the values of two databases",
        "SYNC" => "Internal command used for replication",
        "TIME" => "Return the current server time",
        "TOUCH" => "Alter the last access time of a key",
        "TS.ADD" => "Add a sample to a time series",
        "TS.ALTER" => "Update a time series",
        "TS.CREATE" => "Create a time series",
        "TS.CREATERULE" => "Create a compaction rule",
        "TS.DECRBY" => "Decrease a time series by a value",
        "TS.DEL" => "Delete samples from a time series",
        "TS.DELETERULE" => "Delete a compaction rule",
        "TS.GET" => "Get the last sample of a time series",
        "TS.INCRBY" => "Increase a time series by a value",
        "TS.INFO" => "Time series metadata",
        "TS.MADD" => "Add samples to several time series",
        "TS.MGET" => "Get the last samples of several time series",
        "TS.MRANGE" => "Query several time series in a range",
        "TS.MREVRANGE" => "Query several time series in a range, reverse",
        "TS.NRANGE" => "Query a time series in a range with named options",
        "TS.NREVRANGE" => "Query a time series in a range with named options, reverse",
        "TS.QUERYINDEX" => "Find time series by labels",
        "TS.QUERYLABELS" => "Labels of matching time series",
        "TS.RANGE" => "Query a time series in a range",
        "TS.READ" => "Read samples from a time series",
        "TS.REVRANGE" => "Query a time series in a range, reverse",
        "TTL" => "Get the time to live for a key in seconds",
        "TYPE" => "Determine the type stored at key",
        "UNLINK" => "Delete one or more keys",
        "UNSUBSCRIBE" => "Stop listening for messages posted to the given channels",
        "UNWATCH" => "Forget about all watched keys",
        "VADD" => "Add an element and its vector to a vector set",
        "VCARD" => "Number of elements in a vector set",
        "VDIM" => "Dimension of a vector set",
        "VEMB" => "Vector of an element in a vector set",
        "VGETATTR" => "Attributes of an element in a vector set",
        "VINFO" => "Vector set metadata",
        "VISMEMBER" => "Whether an element is in a vector set",
        "VLINKS" => "Graph neighbours of a vector-set element",
        "VRANDMEMBER" => "Random elements of a vector set",
        "VRANGE" => "Elements of a vector set in a range",
        "VREM" => "Remove elements from a vector set",
        "VSETATTR" => "Set attributes of a vector-set element",
        "VSIM" => "Nearest elements in a vector set",
        "WAIT" => "Wait for writes to reach replicas",
        "WAITAOF" => "Wait for writes to be synced to the AOF",
        "WATCH" => "Watch the given keys to determine execution of a MULTI/EXEC",
        "XACK" => "Mark messages as acknowledged",
        "XACKDEL" => "Acknowledge and delete stream entries",
        "XADD" => "Append a new entry to a stream",
        "XAUTOCLAIM" => "Claim pending stream entries",
        "XCFGSET" => "Set stream configuration",
        "XCLAIM" => "Change ownership of pending stream entries",
        "XDEL" => "Remove entries from a stream",
        "XDELEX" => "Delete stream entries with reference options",
        "XGROUP" => "Manage consumer groups",
        "XIDMPRECORD" => "Set at-most-once metadata on a stream entry",
        "XINFO" => "Stream, group and consumer information",
        "XLEN" => "Number of entries in a stream",
        "XNACK" => "Negatively acknowledge stream entries",
        "XPENDING" => "Pending entries of a consumer group",
        "XRANGE" => "Entries of a stream in a range",
        "XREAD" => "Read entries from one or more streams",
        "XREADGROUP" => "Read entries from streams as a consumer group",
        "XREVRANGE" => "Entries of a stream in a range, reverse",
        "XSETID" => "Set the last ID of a stream",
        "XTRIM" => "Trim a stream",
        "ZADD" => "Add one or more members to a sorted set",
        "ZCARD" => "Get the number of members in a sorted set",
        "ZCOUNT" => "Count the members in a sorted set with scores within the given values",
        "ZDIFF" => "Subtract multiple sorted sets",
        "ZDIFFSTORE" => "Subtract multiple sorted sets and store the resulting sorted set",
        "ZINCRBY" => "Increment the score of a member in a sorted set",
        "ZINTER" => "Intersect multiple sorted sets",
        "ZINTERCARD" => "Cardinality of the intersection of multiple sorted sets",
        "ZINTERSTORE" => "Intersect multiple sorted sets and store the resulting sorted set",
        "ZLEXCOUNT" => "Count the members in a sorted set between a given lexicographical range",
        "ZMPOP" => "Remove and return members with scores in a sorted set",
        "ZMSCORE" => "Get the score associated with the given members in a sorted set",
        "ZPOPMAX" => "Remove and return members with the highest scores",
        "ZPOPMIN" => "Remove and return members with the lowest scores",
        "ZRANDMEMBER" => "Get one or multiple random members from a sorted set",
        "ZRANGE" => "Return a range of members in a sorted set",
        "ZRANGEBYLEX" => "Return a range of members in a sorted set, by lexicographical range",
        "ZRANGEBYSCORE" => "Return a range of members in a sorted set, by score",
        "ZRANGESTORE" => "Store a range of members from a sorted set",
        "ZRANK" => "Determine the index of a member in a sorted set",
        "ZREM" => "Remove one or more members from a sorted set",
        "ZREMRANGEBYLEX" => "Remove all members in a given lexicographical range",
        "ZREMRANGEBYRANK" => "Remove all members within the given indexes",
        "ZREMRANGEBYSCORE" => "Remove all members in a given score range",
        "ZREVRANGE" => {
            "Return a range of members in a sorted set, by index, with scores ordered from high to low"
        }
        "ZREVRANGEBYLEX" => {
            "Return a range of members in a sorted set, by lexicographical range, ordered from high to low"
        }
        "ZREVRANGEBYSCORE" => {
            "Return a range of members in a sorted set, by score, with scores ordered from high to low"
        }
        "ZREVRANK" => {
            "Determine the index of a member in a sorted set, with scores ordered from high to low"
        }
        "ZSCAN" => "Incrementally iterate sorted set elements and scores",
        "ZSCORE" => "Get the score associated with the given member in a sorted set",
        "ZUNION" => "Add multiple sorted sets",
        "ZUNIONSTORE" => "Add multiple sorted sets and store the resulting sorted set",
        _ => "See COMMANDS.md",
    }
}

#[cfg(test)]
mod tests {
    use super::{complexity, group, reply, since, summary};

    #[test]
    fn get_is_a_string_command() {
        assert_eq!(group("GET"), "string");
        assert_eq!(summary("GET"), "Get the value of a key");
        assert_eq!(complexity("GET"), "O(1)");
        assert_eq!(since("GET"), "1.0.0");
    }

    #[test]
    fn search_and_json_go_by_prefix() {
        assert_eq!(group("FT.SEARCH"), "search");
        assert_eq!(group("JSON.SET"), "json");
        assert_eq!(group("TS.ADD"), "timeseries");
        assert_eq!(group("ARSET"), "array");
        assert_eq!(group("VADD"), "vectorset");
        assert_eq!(since("ARSET"), "8.8.0");
        assert_eq!(since("DIGEST"), "8.4.0");
    }

    #[test]
    fn unknown_names_are_omitted() {
        let out = reply(2, &[b"nosuch"], &["GET"]);
        assert_eq!(out, crate::resp::write::resp_array(vec![]));
    }
}
