// Generated from Redis 8.0.0 `src/commands/*.json` (the `key_specs` of each
// command) by `gen_keyspecs.py`. Not edited by hand.

use super::key_specs::{Begin, Find, Spec};

/// `(command, whether it may have no keys, its key specs)`, by command.
#[rustfmt::skip]
pub(super) const TABLE: &[(&str, bool, &[Spec])] = &[
    ("APPEND", false, &[
        Spec { flags: "RW insert", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("BITCOUNT", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("BITFIELD", false, &[
        Spec { flags: "RW access update variable_flags", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("BITFIELD_RO", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("BITOP", false, &[
        Spec { flags: "OW update", begin: Begin::Index(2), find: Find::Range(0, 1, 0) },
        Spec { flags: "RO access", begin: Begin::Index(3), find: Find::Range(-1, 1, 0) },
    ]),
    ("BITPOS", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("BLMOVE", false, &[
        Spec { flags: "RW access delete", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
        Spec { flags: "RW insert", begin: Begin::Index(2), find: Find::Range(0, 1, 0) },
    ]),
    ("BLMPOP", false, &[
        Spec { flags: "RW access delete", begin: Begin::Index(2), find: Find::KeyNum(0, 1, 1) },
    ]),
    ("BLPOP", false, &[
        Spec { flags: "RW access delete", begin: Begin::Index(1), find: Find::Range(-2, 1, 0) },
    ]),
    ("BRPOP", false, &[
        Spec { flags: "RW access delete", begin: Begin::Index(1), find: Find::Range(-2, 1, 0) },
    ]),
    ("BRPOPLPUSH", false, &[
        Spec { flags: "RW access delete", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
        Spec { flags: "RW insert", begin: Begin::Index(2), find: Find::Range(0, 1, 0) },
    ]),
    ("BZMPOP", false, &[
        Spec { flags: "RW access delete", begin: Begin::Index(2), find: Find::KeyNum(0, 1, 1) },
    ]),
    ("BZPOPMAX", false, &[
        Spec { flags: "RW access delete", begin: Begin::Index(1), find: Find::Range(-2, 1, 0) },
    ]),
    ("BZPOPMIN", false, &[
        Spec { flags: "RW access delete", begin: Begin::Index(1), find: Find::Range(-2, 1, 0) },
    ]),
    ("COPY", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
        Spec { flags: "OW update", begin: Begin::Index(2), find: Find::Range(0, 1, 0) },
    ]),
    ("DECR", false, &[
        Spec { flags: "RW access update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("DECRBY", false, &[
        Spec { flags: "RW access update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("DEL", false, &[
        Spec { flags: "RM delete", begin: Begin::Index(1), find: Find::Range(-1, 1, 0) },
    ]),
    ("DUMP", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("EVAL", true, &[
        Spec { flags: "RW access update", begin: Begin::Index(2), find: Find::KeyNum(0, 1, 1) },
    ]),
    ("EVALSHA", true, &[
        Spec { flags: "RW access update", begin: Begin::Index(2), find: Find::KeyNum(0, 1, 1) },
    ]),
    ("EVALSHA_RO", true, &[
        Spec { flags: "RO access", begin: Begin::Index(2), find: Find::KeyNum(0, 1, 1) },
    ]),
    ("EVAL_RO", true, &[
        Spec { flags: "RO access", begin: Begin::Index(2), find: Find::KeyNum(0, 1, 1) },
    ]),
    ("EXISTS", false, &[
        Spec { flags: "RO", begin: Begin::Index(1), find: Find::Range(-1, 1, 0) },
    ]),
    ("EXPIRE", false, &[
        Spec { flags: "RW update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("EXPIREAT", false, &[
        Spec { flags: "RW update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("EXPIRETIME", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("FCALL", true, &[
        Spec { flags: "RW access update", begin: Begin::Index(2), find: Find::KeyNum(0, 1, 1) },
    ]),
    ("FCALL_RO", true, &[
        Spec { flags: "RO access", begin: Begin::Index(2), find: Find::KeyNum(0, 1, 1) },
    ]),
    ("GEOADD", false, &[
        Spec { flags: "RW update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("GEODIST", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("GEOHASH", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("GEOPOS", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("GEORADIUS", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
        Spec { flags: "OW update", begin: Begin::Keyword("STORE", 6), find: Find::Range(0, 1, 0) },
        Spec { flags: "OW update", begin: Begin::Keyword("STOREDIST", 6), find: Find::Range(0, 1, 0) },
    ]),
    ("GEORADIUSBYMEMBER", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
        Spec { flags: "OW update", begin: Begin::Keyword("STORE", 5), find: Find::Range(0, 1, 0) },
        Spec { flags: "OW update", begin: Begin::Keyword("STOREDIST", 5), find: Find::Range(0, 1, 0) },
    ]),
    ("GEORADIUSBYMEMBER_RO", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("GEORADIUS_RO", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("GEOSEARCH", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("GEOSEARCHSTORE", false, &[
        Spec { flags: "OW update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
        Spec { flags: "RO access", begin: Begin::Index(2), find: Find::Range(0, 1, 0) },
    ]),
    ("GET", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("GETBIT", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("GETDEL", false, &[
        Spec { flags: "RW access delete", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("GETEX", false, &[
        Spec { flags: "RW access update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("GETRANGE", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("GETSET", false, &[
        Spec { flags: "RW access update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("HDEL", false, &[
        Spec { flags: "RW delete", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("HEXISTS", false, &[
        Spec { flags: "RO", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("HEXPIRE", false, &[
        Spec { flags: "RW update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("HEXPIREAT", false, &[
        Spec { flags: "RW update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("HEXPIRETIME", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("HGET", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("HGETALL", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("HGETDEL", false, &[
        Spec { flags: "RW access delete", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("HGETEX", false, &[
        Spec { flags: "RW access update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("HINCRBY", false, &[
        Spec { flags: "RW access update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("HINCRBYFLOAT", false, &[
        Spec { flags: "RW access update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("HKEYS", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("HLEN", false, &[
        Spec { flags: "RO", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("HMGET", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("HMSET", false, &[
        Spec { flags: "RW update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("HPERSIST", false, &[
        Spec { flags: "RW update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("HPEXPIRE", false, &[
        Spec { flags: "RW update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("HPEXPIREAT", false, &[
        Spec { flags: "RW update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("HPEXPIRETIME", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("HPTTL", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("HRANDFIELD", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("HSCAN", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("HSET", false, &[
        Spec { flags: "RW update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("HSETEX", false, &[
        Spec { flags: "RW update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("HSETNX", false, &[
        Spec { flags: "RW insert", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("HSTRLEN", false, &[
        Spec { flags: "RO", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("HTTL", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("HVALS", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("INCR", false, &[
        Spec { flags: "RW access update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("INCRBY", false, &[
        Spec { flags: "RW access update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("INCRBYFLOAT", false, &[
        Spec { flags: "RW access update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("LCS", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(1, 1, 0) },
    ]),
    ("LINDEX", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("LINSERT", false, &[
        Spec { flags: "RW insert", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("LLEN", false, &[
        Spec { flags: "RO", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("LMOVE", false, &[
        Spec { flags: "RW access delete", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
        Spec { flags: "RW insert", begin: Begin::Index(2), find: Find::Range(0, 1, 0) },
    ]),
    ("LMPOP", false, &[
        Spec { flags: "RW access delete", begin: Begin::Index(1), find: Find::KeyNum(0, 1, 1) },
    ]),
    ("LPOP", false, &[
        Spec { flags: "RW access delete", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("LPOS", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("LPUSH", false, &[
        Spec { flags: "RW insert", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("LPUSHX", false, &[
        Spec { flags: "RW insert", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("LRANGE", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("LREM", false, &[
        Spec { flags: "RW delete", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("LSET", false, &[
        Spec { flags: "RW update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("LTRIM", false, &[
        Spec { flags: "RW delete", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("MEMORY|USAGE", false, &[
        Spec { flags: "RO", begin: Begin::Index(2), find: Find::Range(0, 1, 0) },
    ]),
    ("MGET", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(-1, 1, 0) },
    ]),
    ("MIGRATE", false, &[
        Spec { flags: "RW access delete", begin: Begin::Index(3), find: Find::Range(0, 1, 0) },
        Spec { flags: "RW access delete incomplete", begin: Begin::Keyword("KEYS", -2), find: Find::Range(-1, 1, 0) },
    ]),
    ("MOVE", false, &[
        Spec { flags: "RW access update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("MSET", false, &[
        Spec { flags: "OW update", begin: Begin::Index(1), find: Find::Range(-1, 2, 0) },
    ]),
    ("MSETNX", false, &[
        Spec { flags: "OW insert", begin: Begin::Index(1), find: Find::Range(-1, 2, 0) },
    ]),
    ("OBJECT|ENCODING", false, &[
        Spec { flags: "RO", begin: Begin::Index(2), find: Find::Range(0, 1, 0) },
    ]),
    ("OBJECT|FREQ", false, &[
        Spec { flags: "RO", begin: Begin::Index(2), find: Find::Range(0, 1, 0) },
    ]),
    ("OBJECT|IDLETIME", false, &[
        Spec { flags: "RO", begin: Begin::Index(2), find: Find::Range(0, 1, 0) },
    ]),
    ("OBJECT|REFCOUNT", false, &[
        Spec { flags: "RO", begin: Begin::Index(2), find: Find::Range(0, 1, 0) },
    ]),
    ("PERSIST", false, &[
        Spec { flags: "RW update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("PEXPIRE", false, &[
        Spec { flags: "RW update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("PEXPIREAT", false, &[
        Spec { flags: "RW update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("PEXPIRETIME", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("PFADD", false, &[
        Spec { flags: "RW insert", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("PFCOUNT", false, &[
        Spec { flags: "RW access", begin: Begin::Index(1), find: Find::Range(-1, 1, 0) },
    ]),
    ("PFDEBUG", false, &[
        Spec { flags: "RW access", begin: Begin::Index(2), find: Find::Range(0, 1, 0) },
    ]),
    ("PFMERGE", false, &[
        Spec { flags: "RW access insert", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
        Spec { flags: "RO access", begin: Begin::Index(2), find: Find::Range(-1, 1, 0) },
    ]),
    ("PSETEX", false, &[
        Spec { flags: "OW update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("PTTL", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("RENAME", false, &[
        Spec { flags: "RW access delete", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
        Spec { flags: "OW update", begin: Begin::Index(2), find: Find::Range(0, 1, 0) },
    ]),
    ("RENAMENX", false, &[
        Spec { flags: "RW access delete", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
        Spec { flags: "OW insert", begin: Begin::Index(2), find: Find::Range(0, 1, 0) },
    ]),
    ("RESTORE", false, &[
        Spec { flags: "OW update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("RESTORE-ASKING", false, &[
        Spec { flags: "OW update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("RPOP", false, &[
        Spec { flags: "RW access delete", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("RPOPLPUSH", false, &[
        Spec { flags: "RW access delete", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
        Spec { flags: "RW insert", begin: Begin::Index(2), find: Find::Range(0, 1, 0) },
    ]),
    ("RPUSH", false, &[
        Spec { flags: "RW insert", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("RPUSHX", false, &[
        Spec { flags: "RW insert", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("SADD", false, &[
        Spec { flags: "RW insert", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("SCARD", false, &[
        Spec { flags: "RO", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("SDIFF", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(-1, 1, 0) },
    ]),
    ("SDIFFSTORE", false, &[
        Spec { flags: "OW update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
        Spec { flags: "RO access", begin: Begin::Index(2), find: Find::Range(-1, 1, 0) },
    ]),
    ("MSETEX", false, &[
        Spec { flags: "OW update", begin: Begin::Index(1), find: Find::KeyNum(0, 1, 2) },
    ]),
    ("SET", false, &[
        Spec { flags: "RW access update variable_flags", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("SETBIT", false, &[
        Spec { flags: "RW access update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("SETEX", false, &[
        Spec { flags: "OW update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("SETNX", false, &[
        Spec { flags: "OW insert", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("SETRANGE", false, &[
        Spec { flags: "RW update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("SINTER", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(-1, 1, 0) },
    ]),
    ("SINTERCARD", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::KeyNum(0, 1, 1) },
    ]),
    ("SINTERSTORE", false, &[
        Spec { flags: "OW update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
        Spec { flags: "RO access", begin: Begin::Index(2), find: Find::Range(-1, 1, 0) },
    ]),
    ("SISMEMBER", false, &[
        Spec { flags: "RO", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("SMEMBERS", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("SMISMEMBER", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("SMOVE", false, &[
        Spec { flags: "RW access delete", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
        Spec { flags: "RW insert", begin: Begin::Index(2), find: Find::Range(0, 1, 0) },
    ]),
    ("SORT", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
        Spec { flags: "RO access", begin: Begin::Unknown, find: Find::Unknown },
        Spec { flags: "OW update", begin: Begin::Unknown, find: Find::Unknown },
    ]),
    ("SORT_RO", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
        Spec { flags: "RO access", begin: Begin::Unknown, find: Find::Unknown },
    ]),
    ("SPOP", false, &[
        Spec { flags: "RW access delete", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("SPUBLISH", false, &[
        Spec { flags: "not_key", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("SRANDMEMBER", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("SREM", false, &[
        Spec { flags: "RW delete", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("SSCAN", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("SSUBSCRIBE", false, &[
        Spec { flags: "not_key", begin: Begin::Index(1), find: Find::Range(-1, 1, 0) },
    ]),
    ("STRLEN", false, &[
        Spec { flags: "RO", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("SUBSTR", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("SUNION", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(-1, 1, 0) },
    ]),
    ("SUNIONSTORE", false, &[
        Spec { flags: "OW update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
        Spec { flags: "RO access", begin: Begin::Index(2), find: Find::Range(-1, 1, 0) },
    ]),
    ("SUNSUBSCRIBE", false, &[
        Spec { flags: "not_key", begin: Begin::Index(1), find: Find::Range(-1, 1, 0) },
    ]),
    ("TOUCH", false, &[
        Spec { flags: "RO", begin: Begin::Index(1), find: Find::Range(-1, 1, 0) },
    ]),
    ("TTL", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("TYPE", false, &[
        Spec { flags: "RO", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("UNLINK", false, &[
        Spec { flags: "RM delete", begin: Begin::Index(1), find: Find::Range(-1, 1, 0) },
    ]),
    ("WATCH", false, &[
        Spec { flags: "RO", begin: Begin::Index(1), find: Find::Range(-1, 1, 0) },
    ]),
    ("XACK", false, &[
        Spec { flags: "RW update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("XADD", false, &[
        Spec { flags: "RW update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("XAUTOCLAIM", false, &[
        Spec { flags: "RW delete", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("XCLAIM", false, &[
        Spec { flags: "RW update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("XDEL", false, &[
        Spec { flags: "RW delete", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("XGROUP|CREATE", false, &[
        Spec { flags: "RW insert", begin: Begin::Index(2), find: Find::Range(0, 1, 0) },
    ]),
    ("XGROUP|CREATECONSUMER", false, &[
        Spec { flags: "RW insert", begin: Begin::Index(2), find: Find::Range(0, 1, 0) },
    ]),
    ("XGROUP|DELCONSUMER", false, &[
        Spec { flags: "RW delete", begin: Begin::Index(2), find: Find::Range(0, 1, 0) },
    ]),
    ("XGROUP|DESTROY", false, &[
        Spec { flags: "RW delete", begin: Begin::Index(2), find: Find::Range(0, 1, 0) },
    ]),
    ("XGROUP|SETID", false, &[
        Spec { flags: "RW update", begin: Begin::Index(2), find: Find::Range(0, 1, 0) },
    ]),
    ("XINFO|CONSUMERS", false, &[
        Spec { flags: "RO access", begin: Begin::Index(2), find: Find::Range(0, 1, 0) },
    ]),
    ("XINFO|GROUPS", false, &[
        Spec { flags: "RO access", begin: Begin::Index(2), find: Find::Range(0, 1, 0) },
    ]),
    ("XINFO|STREAM", false, &[
        Spec { flags: "RO access", begin: Begin::Index(2), find: Find::Range(0, 1, 0) },
    ]),
    ("XLEN", false, &[
        Spec { flags: "RO", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("XPENDING", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("XRANGE", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("XREAD", false, &[
        Spec { flags: "RO access", begin: Begin::Keyword("STREAMS", 1), find: Find::Range(-1, 1, 2) },
    ]),
    ("XREADGROUP", false, &[
        Spec { flags: "RO access", begin: Begin::Keyword("STREAMS", 4), find: Find::Range(-1, 1, 2) },
    ]),
    ("XREVRANGE", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("XSETID", false, &[
        Spec { flags: "RW update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("XIDMPRECORD", false, &[
        Spec { flags: "RW update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("XTRIM", false, &[
        Spec { flags: "RW delete", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("ZADD", false, &[
        Spec { flags: "RW update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("ZCARD", false, &[
        Spec { flags: "RO", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("ZCOUNT", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("ZDIFF", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::KeyNum(0, 1, 1) },
    ]),
    ("ZDIFFSTORE", false, &[
        Spec { flags: "OW update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
        Spec { flags: "RO access", begin: Begin::Index(2), find: Find::KeyNum(0, 1, 1) },
    ]),
    ("ZINCRBY", false, &[
        Spec { flags: "RW access update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("ZINTER", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::KeyNum(0, 1, 1) },
    ]),
    ("ZINTERCARD", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::KeyNum(0, 1, 1) },
    ]),
    ("ZINTERSTORE", false, &[
        Spec { flags: "OW update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
        Spec { flags: "RO access", begin: Begin::Index(2), find: Find::KeyNum(0, 1, 1) },
    ]),
    ("ZLEXCOUNT", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("ZMPOP", false, &[
        Spec { flags: "RW access delete", begin: Begin::Index(1), find: Find::KeyNum(0, 1, 1) },
    ]),
    ("ZMSCORE", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("ZPOPMAX", false, &[
        Spec { flags: "RW access delete", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("ZPOPMIN", false, &[
        Spec { flags: "RW access delete", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("ZRANDMEMBER", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("ZRANGE", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("ZRANGEBYLEX", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("ZRANGEBYSCORE", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("ZRANGESTORE", false, &[
        Spec { flags: "OW update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
        Spec { flags: "RO access", begin: Begin::Index(2), find: Find::Range(0, 1, 0) },
    ]),
    ("ZRANK", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("ZREM", false, &[
        Spec { flags: "RW delete", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("ZREMRANGEBYLEX", false, &[
        Spec { flags: "RW delete", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("ZREMRANGEBYRANK", false, &[
        Spec { flags: "RW delete", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("ZREMRANGEBYSCORE", false, &[
        Spec { flags: "RW delete", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("ZREVRANGE", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("ZREVRANGEBYLEX", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("ZREVRANGEBYSCORE", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("ZREVRANK", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("ZSCAN", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("ZSCORE", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
    ]),
    ("ZUNION", false, &[
        Spec { flags: "RO access", begin: Begin::Index(1), find: Find::KeyNum(0, 1, 1) },
    ]),
    ("ZUNIONSTORE", false, &[
        Spec { flags: "OW update", begin: Begin::Index(1), find: Find::Range(0, 1, 0) },
        Spec { flags: "RO access", begin: Begin::Index(2), find: Find::KeyNum(0, 1, 1) },
    ]),
];
