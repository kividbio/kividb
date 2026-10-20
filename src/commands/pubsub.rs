use crate::commands::ctx::Ctx;
use crate::resp::write::{resp_array, resp_bulk_str, resp_error, resp_integer};
use std::sync::Arc;

pub fn handle(ctx: &mut Ctx, name: &str) -> Option<Vec<u8>> {
    match name {
        "PUBLISH" => Some(publish(ctx, false)),
        "SPUBLISH" => Some(publish(ctx, true)),
        "PUBSUB" => Some(pubsub_cmd(ctx)),
        _ => None,
    }
}

fn publish(ctx: &mut Ctx, shard: bool) -> Vec<u8> {
    if ctx.cmd.len() != 3 {
        return resp_error("ERR wrong number of arguments for 'publish' command");
    }
    let channel = match std::str::from_utf8(ctx.cmd[1]) {
        Ok(s) => s,
        Err(_) => return resp_error("ERR channel must be valid UTF-8"),
    };
    let payload: Arc<[u8]> = Arc::from(ctx.cmd[2]);
    // By this connection: whether it is a client's command, one in a
    // transaction, or a script's redis.call.
    let from = Some(ctx.client_id);
    let count = if shard {
        ctx.pubsub.spublish_from(channel, payload, from)
    } else {
        ctx.pubsub.publish_from(channel, payload, from)
    };
    // In a cluster a subscriber may be connected to any node, so the message
    // goes to all of them (to the slot's shard only, for a shard channel).
    // Without this a message only reached subscribers that happened to be on
    // the node it was published to. The reply counts local receivers only, as
    // in Redis.
    if ctx.cluster.enabled && !ctx.apply_replication {
        crate::cluster::bus::heartbeat::forward_publish(ctx.cluster, ctx.cmd[1], ctx.cmd[2], shard);
    }
    resp_integer(count as i64)
}

fn pubsub_cmd(ctx: &mut Ctx) -> Vec<u8> {
    if ctx.cmd.len() < 2 {
        return resp_error("ERR wrong number of arguments for 'pubsub' command");
    }
    let sub = match std::str::from_utf8(ctx.cmd[1]) {
        Ok(s) => s.to_ascii_uppercase(),
        Err(_) => return resp_error("ERR invalid subcommand"),
    };
    match sub.as_str() {
        "HELP" => crate::commands::server::help(
            "PUBSUB",
            &[
                (
                    "CHANNELS [<pattern>]",
                    "Return the currently active channels matching a <pattern> (default: '*').",
                ),
                ("NUMPAT", "Return number of subscriptions to patterns."),
                (
                    "NUMSUB [<channel> ...]",
                    "Return the number of subscribers for the specified channels, excluding pattern subscriptions(default: no channels).",
                ),
                (
                    "SHARDCHANNELS [<pattern>]",
                    "Return the currently active shard level channels matching a <pattern> (default: '*').",
                ),
                (
                    "SHARDNUMSUB [<shardchannel> ...]",
                    "Return the number of subscribers for the specified shard level channel(s)",
                ),
            ],
        ),
        "CHANNELS" => {
            let pat = ctx.cmd.get(2).and_then(|b| std::str::from_utf8(b).ok());
            let items: Vec<Vec<u8>> = ctx
                .pubsub
                .channels(pat)
                .iter()
                .map(|c| resp_bulk_str(c))
                .collect();
            resp_array(items)
        }
        "NUMSUB" => {
            let names: Vec<&str> = ctx.cmd[2..]
                .iter()
                .filter_map(|b| std::str::from_utf8(b).ok())
                .collect();
            let counts = ctx.pubsub.numsub(&names);
            let mut out = Vec::with_capacity(counts.len() * 2);
            for (ch, n) in counts {
                out.push(resp_bulk_str(&ch));
                out.push(resp_integer(n as i64));
            }
            resp_array(out)
        }
        "NUMPAT" => resp_integer(ctx.pubsub.numpat() as i64),
        "SHARDCHANNELS" => {
            let pat = ctx.cmd.get(2).and_then(|b| std::str::from_utf8(b).ok());
            let items: Vec<Vec<u8>> = ctx
                .pubsub
                .shard_channels(pat)
                .iter()
                .map(|c| resp_bulk_str(c))
                .collect();
            resp_array(items)
        }
        "SHARDNUMSUB" => {
            let names: Vec<&str> = ctx.cmd[2..]
                .iter()
                .filter_map(|b| std::str::from_utf8(b).ok())
                .collect();
            let counts = ctx.pubsub.shardnumsub(&names);
            let mut out = Vec::with_capacity(counts.len() * 2);
            for (ch, n) in counts {
                out.push(resp_bulk_str(&ch));
                out.push(resp_integer(n as i64));
            }
            resp_array(out)
        }
        _ => resp_error(&format!("ERR unknown PUBSUB subcommand '{}'", sub)),
    }
}
