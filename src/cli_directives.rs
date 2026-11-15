// src/cli_directives.rs
//
// Redis-style configuration on the command line (#390): `kividb --appendonly
// yes --save 900 1 --maxmemory 256mb`, the form `redis-server` takes, and the
// one a Compose file, a Helm chart or a platform template carries over from
// Redis unchanged.
//
// Clap knows only KiviDB's own flags, and has no "parse what you know and hand
// back the rest". So the command line is split first: what names one of the
// flags goes to clap as before, and every other `--<directive> <values...>`
// goes through `Config::apply_directive`, the function a config file's lines
// and `CONFIG SET` already use, so that a directive means the same wherever it
// is written and nothing is checked twice.
//
// The rules, in order of strength:
//   native flag  >  directive on the command line  >  config file  >  default
//
// The form is Redis's (`main` in `server.c`):
//   - the first argument, when it does not start with `-`, is a config file;
//   - `-`, first or last, says the config text follows on standard input, so
//     a `-` that is the last argument is never a value (`--dir -` is
//     `--dir` with no value, an error, as in Redis);
//   - `--name` starts an option, and every following argument up to the next
//     one that starts with `--` is its values, so a value may start with one
//     dash (`--maxmemory -1`) but not with two, except the first, which is
//     always a value (`--requirepass --x`);
//   - `--name "a b"` in one argument is the name and one value, as `--port
//     6380` in one argument is `--port` and `6380`;
//   - `--save` followed by another option, or by nothing, is `--save ""`.
// Beyond Redis, `--name=value` is taken too, as clap takes it for the flags.
// Directive names are not case-sensitive, as in a config file.

use crate::config::{Config, split_config_line};
use std::ffi::OsString;

/// One `--<directive> <values...>` from the command line.
#[derive(Debug, PartialEq, Eq)]
pub struct Directive {
    /// The name as written after the dashes, in lower case.
    pub name: String,
    pub args: Vec<String>,
}

/// A command line, split.
#[derive(Debug, Default)]
pub struct Split {
    /// What clap is given: the program name, then the arguments of the flags
    /// it knows.
    pub native: Vec<OsString>,
    /// The other directives, in the order written.
    pub directives: Vec<Directive>,
    /// `-` was given: more config text is on standard input.
    pub stdin: bool,
}

/// How many values the flag called `name` takes, if `cmd` has one so named
/// (by its long name or an alias).
fn native_arity(cmd: &clap::Command, name: &str) -> Option<(String, usize)> {
    let arg = cmd.get_arguments().find(|a| {
        a.get_long() == Some(name) || a.get_all_aliases().is_some_and(|l| l.contains(&name))
    })?;
    let arity = match arg.get_num_args() {
        Some(range) => range.max_values(),
        None => usize::from(arg.get_action().takes_values()),
    };
    Some((arg.get_id().to_string(), arity))
}

/// Split `args` (the whole command line, program name first) into what clap
/// reads and the directives it does not know. `Err` is a command line that
/// cannot be read, as a message to print.
pub fn split(cmd: &clap::Command, args: Vec<OsString>) -> Result<Split, String> {
    let mut cmd = cmd.clone();
    cmd.build();
    let mut out = Split::default();
    let mut segments: Vec<(String, Vec<OsString>)> = Vec::new();
    let n = args.len();
    let text = |a: &OsString| a.to_str().map(str::to_string);
    let starts_option = |a: &OsString| a.to_str().is_some_and(|s| s.starts_with("--"));
    let mut j = 0;
    if let Some(program) = args.first() {
        segments.push((String::new(), vec![program.clone()]));
        j = 1;
    }
    while j < n {
        let token = &args[j];
        let Some(s) = text(token) else {
            segments.push((String::new(), vec![token.clone()]));
            j += 1;
            continue;
        };
        if s == "-" && (j == 1 || j == n - 1) {
            out.stdin = true;
            j += 1;
            continue;
        }
        if j == 1 && !s.starts_with('-') {
            segments.retain(|(key, _)| key != "configfile");
            segments.push((
                "configfile".to_string(),
                vec!["--configfile".into(), token.clone()],
            ));
            j += 1;
            continue;
        }
        // A single dash (`-h`, `-V`), a lone `--`, or a value no option asked
        // for: clap's to read, and to refuse.
        if !s.starts_with("--") || s.len() == 2 {
            segments.push((String::new(), vec![token.clone()]));
            j += 1;
            continue;
        }
        let body = &s[2..];
        // Nothing below echoes `s`: it may hold a password.
        // `--name=value` first: the value may hold spaces (`--dir=/my dir`).
        // Only a name with whitespace before any `=` is `--name value` in one
        // argument.
        let (name, mut values, equals, complete) = match body.split_once('=') {
            Some((name, value)) if !name.contains(char::is_whitespace) => {
                (name.to_string(), vec![value.to_string()], true, true)
            }
            _ if body.contains(char::is_whitespace) => {
                let mut words = split_config_line(body)
                    .map_err(|e| format!("an argument with a name and a value: {e}"))?;
                if words.is_empty() {
                    return Err("an argument with no directive named".into());
                }
                let name = words.remove(0);
                (name, words, false, true)
            }
            _ => (body.to_string(), Vec::new(), false, false),
        };
        j += 1;
        if name.is_empty() {
            return Err("an argument with no directive named".into());
        }
        let name = name.to_ascii_lowercase();
        // `bind` lists addresses, which a flag cannot: it goes the way of the
        // config file's, with its warning when KiviDB uses only one.
        let native = if name == "bind" {
            None
        } else {
            native_arity(&cmd, &name)
        };
        if !complete && native.as_ref().is_none_or(|(_, arity)| *arity > 0) {
            // As Redis reads them, the first argument after a bare name is
            // its value even if it starts with two dashes (`--requirepass
            // --x`), except that `--save` then means `--save ""`.
            let mut first = name != "save";
            while j < n && (first || !starts_option(&args[j])) && !(j == n - 1 && args[j] == "-") {
                first = false;
                let Some(value) = text(&args[j]) else {
                    return Err(format!("'--{name}': a value that is not valid UTF-8"));
                };
                values.push(value);
                j += 1;
            }
        }
        if let Some((id, arity)) = native {
            // Given twice, the last one counts, as for a directive in Redis.
            segments.retain(|(key, _)| *key != id);
            if equals {
                segments.push((id, vec![format!("--{name}={}", values[0]).into()]));
                continue;
            }
            let extra = values.split_off(values.len().min(arity));
            // One value goes as `--name=value`, so that clap takes a value
            // that starts with a dash (`--requirepass -x`) as Redis does.
            let tokens: Vec<OsString> = match values.as_slice() {
                [value] if arity == 1 => vec![format!("--{name}={value}").into()],
                _ => std::iter::once(format!("--{name}"))
                    .chain(values)
                    .map(OsString::from)
                    .collect(),
            };
            segments.push((id, tokens));
            if !extra.is_empty() {
                // Counts only: a value may be a password.
                return Err(format!(
                    "'--{name}' takes {arity} value{}, and {} more follow{}",
                    if arity == 1 { "" } else { "s" },
                    extra.len(),
                    if extra.len() == 1 { "s" } else { "" }
                ));
            }
            continue;
        }
        // `--bind "a b"` lists two addresses, as `--bind a b` does.
        if name == "bind" && values.len() == 1 {
            values = values[0].split_whitespace().map(String::from).collect();
        }
        // Redis: `--save` with nothing after it is `--save ""`.
        if values.is_empty() && name == "save" {
            values.push(String::new());
        }
        out.directives.push(Directive { name, args: values });
    }
    out.native = segments
        .into_iter()
        .flat_map(|(_, tokens)| tokens)
        .collect();
    Ok(out)
}

/// The known name closest to `name`, if one is close.
fn closest<'a>(name: &str, known: impl Iterator<Item = &'a str>) -> Option<&'a str> {
    let limit = (name.len() / 3).max(2);
    known
        .map(|k| (edit_distance(name, k), k))
        .filter(|(d, _)| *d <= limit)
        .min_by_key(|(d, _)| *d)
        .map(|(_, k)| k)
}

/// Levenshtein distance.
fn edit_distance(a: &str, b: &str) -> usize {
    let b: Vec<char> = b.chars().collect();
    let mut row: Vec<usize> = (0..=b.len()).collect();
    for (i, ca) in a.chars().enumerate() {
        let mut diagonal = row[0];
        row[0] = i + 1;
        for (j, cb) in b.iter().enumerate() {
            let above = row[j + 1];
            row[j + 1] = (diagonal + usize::from(ca != *cb))
                .min(row[j] + 1)
                .min(above + 1);
            diagonal = above;
        }
    }
    row[b.len()]
}

/// Apply the directives over `config`, which has already had the config file
/// applied: a directive replaces the file's setting, and leaves alone one the
/// command line's own flags set (`from_cli`). `Ok` carries the warnings to
/// log; `Err` is the message to print before exiting.
///
/// A directive a native flag overrides is still checked, but its warning is
/// not logged: it says something about a setting that is not in effect.
pub fn apply(
    config: &mut Config,
    cmd: &clap::Command,
    directives: &[Directive],
    from_cli: &dyn Fn(&str) -> bool,
) -> Result<Vec<String>, String> {
    let mut warnings = Vec::new();
    for Directive { name, args } in directives {
        let overridden = std::cell::Cell::new(false);
        let asked = |option: &str| {
            let given = from_cli(option);
            overridden.set(overridden.get() || given);
            given
        };
        match config.apply_directive(name, args, &asked) {
            Ok(Some(warning)) if !overridden.get() => {
                warnings.push(format!("command line: --{name}: {warning}"));
            }
            Ok(_) => {}
            Err(e) if e.starts_with("unknown directive") => {
                let longs = cmd.get_arguments().filter_map(|a| a.get_long());
                let known = crate::redis_settings::REDIS_SETTINGS
                    .iter()
                    .copied()
                    .chain(longs);
                return Err(match closest(name, known) {
                    Some(near) => format!(
                        "unknown directive '--{name}': did you mean '--{near}'? \
                         (kividb --help lists the options)"
                    ),
                    None => format!(
                        "unknown directive '--{name}' (kividb --help lists the options; \
                         CONFIG GET * lists the directives)"
                    ),
                });
            }
            Err(e) => {
                return Err(format!(
                    "--{name}: {}",
                    crate::config::redact(name, args, e)
                ));
            }
        }
    }
    Ok(warnings)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    fn argv(args: &[&str]) -> Vec<OsString> {
        std::iter::once("kividb")
            .chain(args.iter().copied())
            .map(OsString::from)
            .collect()
    }

    fn split_of(args: &[&str]) -> Split {
        split(&Config::command(), argv(args)).unwrap()
    }

    fn native_of(s: &Split) -> Vec<&str> {
        s.native
            .iter()
            .skip(1)
            .map(|a| a.to_str().unwrap())
            .collect()
    }

    fn d(name: &str, args: &[&str]) -> Directive {
        Directive {
            name: name.into(),
            args: args.iter().map(|s| s.to_string()).collect(),
        }
    }

    /// The configuration `args` ends up as, with no config file.
    fn config_of(args: &[&str]) -> Result<(Config, Vec<String>), String> {
        let cmd = Config::command();
        let s = split(&cmd, argv(args))?;
        let matches = cmd
            .clone()
            .try_get_matches_from(s.native.clone())
            .map_err(|e| e.to_string())?;
        let mut config = <Config as clap::FromArgMatches>::from_arg_matches(&matches).unwrap();
        let from_cli =
            |o: &str| matches.value_source(o) == Some(clap::parser::ValueSource::CommandLine);
        let warnings = apply(&mut config, &cmd, &s.directives, &from_cli)?;
        Ok((config, warnings))
    }

    #[test]
    fn a_directive_and_its_value() {
        let s = split_of(&["--appendonly", "yes", "--dbfilename", "x.rdb"]);
        assert!(native_of(&s).is_empty());
        assert_eq!(
            s.directives,
            vec![d("appendonly", &["yes"]), d("dbfilename", &["x.rdb"])]
        );
    }

    #[test]
    fn a_native_flag_goes_to_clap() {
        let s = split_of(&["--port", "7000", "--aof", "--appendonly", "no"]);
        assert_eq!(native_of(&s), ["--port=7000", "--aof"]);
        assert_eq!(s.directives, vec![d("appendonly", &["no"])]);
    }

    #[test]
    fn a_native_alias_and_upper_case() {
        let s = split_of(&["--PORT", "7000", "--slave-priority", "5"]);
        assert_eq!(native_of(&s), ["--port=7000", "--slave-priority=5"]);
        assert!(s.directives.is_empty());
    }

    #[test]
    fn several_values() {
        let s = split_of(&[
            "--save", "900", "1", "--save", "300", "10", "--bind", "a", "b",
        ]);
        assert_eq!(
            s.directives[..2],
            [d("save", &["900", "1"]), d("save", &["300", "10"])]
        );
        assert_eq!(s.directives[2], d("bind", &["a", "b"]));
        assert!(native_of(&s).is_empty());
    }

    #[test]
    fn a_save_with_one_quoted_value() {
        let s = split_of(&["--save", "900 1"]);
        assert_eq!(s.directives, vec![d("save", &["900 1"])]);
    }

    #[test]
    fn an_empty_value() {
        let s = split_of(&["--save", "", "--dbfilename", ""]);
        assert_eq!(s.directives, vec![d("save", &[""]), d("dbfilename", &[""])]);
    }

    #[test]
    fn save_with_no_value_is_save_empty() {
        let s = split_of(&["--save", "--appendonly", "yes"]);
        assert_eq!(
            s.directives,
            vec![d("save", &[""]), d("appendonly", &["yes"])]
        );
        let s = split_of(&["--appendonly", "yes", "--save"]);
        assert_eq!(
            s.directives,
            vec![d("appendonly", &["yes"]), d("save", &[""])]
        );
    }

    #[test]
    fn a_value_may_start_with_one_dash_but_not_two() {
        let s = split_of(&["--hz", "-5", "--dbfilename", "-x-"]);
        assert_eq!(
            s.directives,
            vec![d("hz", &["-5"]), d("dbfilename", &["-x-"])]
        );
        // The first argument after a name is its value, whatever it starts with.
        let s = split_of(&["--dbfilename", "--x", "--appendonly", "yes"]);
        assert_eq!(
            s.directives,
            vec![d("dbfilename", &["--x"]), d("appendonly", &["yes"])]
        );
    }

    #[test]
    fn name_and_value_in_one_argument() {
        let s = split_of(&["--hz 5", "--save 900 1", "--dbfilename \"a b\""]);
        assert_eq!(
            s.directives,
            vec![
                d("hz", &["5"]),
                d("save", &["900", "1"]),
                d("dbfilename", &["a b"])
            ]
        );
    }

    #[test]
    fn name_equals_value() {
        let s = split_of(&["--appendonly=yes", "--port=7000"]);
        assert_eq!(s.directives, vec![d("appendonly", &["yes"])]);
        assert_eq!(native_of(&s), ["--port=7000"]);
    }

    #[test]
    fn a_config_file_first() {
        let s = split_of(&["/etc/redis.conf", "--appendonly", "yes"]);
        assert_eq!(native_of(&s), ["--configfile", "/etc/redis.conf"]);
        assert_eq!(s.directives, vec![d("appendonly", &["yes"])]);
        // Only first: elsewhere a bare word is clap's to refuse.
        let s = split_of(&["--appendonly", "yes", "no"]);
        assert_eq!(s.directives, vec![d("appendonly", &["yes", "no"])]);
    }

    #[test]
    fn a_dash_for_standard_input_first_or_last() {
        let s = split_of(&["-", "--port", "7000"]);
        assert!(s.stdin);
        assert_eq!(native_of(&s), ["--port=7000"]);
        let s = split_of(&["--appendonly", "yes", "-"]);
        assert!(s.stdin);
        assert_eq!(s.directives, vec![d("appendonly", &["yes"])]);
    }

    #[test]
    fn single_dash_options_are_claps() {
        let s = split_of(&["-h"]);
        assert_eq!(native_of(&s), ["-h"]);
        let s = split_of(&["--help"]);
        assert_eq!(native_of(&s), ["--help"]);
        let s = split_of(&["--version"]);
        assert_eq!(native_of(&s), ["--version"]);
    }

    #[test]
    fn a_native_flag_given_too_many_values_is_refused() {
        let e = split(&Config::command(), argv(&["--port", "1", "2"])).unwrap_err();
        assert!(
            e.contains("--port") && e.contains("1 more follows") && !e.contains('2'),
            "{e}"
        );
        // A flag that takes no value leaves what follows to clap, which refuses it.
        let s = split_of(&["--aof", "yes"]);
        assert_eq!(native_of(&s), ["--aof", "yes"]);
    }

    #[test]
    fn an_empty_name_is_refused() {
        assert!(split(&Config::command(), argv(&["--=x"])).is_err());
        assert!(split(&Config::command(), argv(&["-- "])).is_err());
    }

    #[test]
    fn the_docker_compose_command_line() {
        let (c, w) = config_of(&[
            "--appendonly",
            "yes",
            "--requirepass",
            "s3cret",
            "--maxmemory",
            "256mb",
            "--maxmemory-policy",
            "allkeys-lru",
        ])
        .unwrap();
        assert!(c.aof);
        assert_eq!(c.requirepass, "s3cret");
        assert_eq!(c.maxmemory, 256 * 1024 * 1024);
        assert_eq!(c.maxmemory_policy, "allkeys-lru");
        assert!(w.is_empty(), "{w:?}");
    }

    #[test]
    fn the_directives_the_issue_lists() {
        let (c, w) = config_of(&[
            "--appendonly",
            "no",
            "--save",
            "900",
            "1",
            "--save",
            "300",
            "10",
            "--save",
            "",
            "--appendfsync",
            "everysec",
            "--maxmemory",
            "1gb",
            "--bind",
            "127.0.0.1",
            "--protected-mode",
            "no",
            "--dir",
            "/data",
            "--dbfilename",
            "dump.rdb",
            "--loglevel",
            "warning",
            "--databases",
            "4",
            "--tcp-keepalive",
            "300",
            "--timeout",
            "0",
            "--io-threads",
            "4",
        ])
        .unwrap();
        assert!(!c.aof);
        assert_eq!(c.bind, "127.0.0.1");
        assert!(!c.protected_mode);
        assert_eq!(c.dir, "/data");
        assert_eq!(c.databases, 4);
        assert_eq!(c.maxmemory, 1 << 30);
        assert_eq!(c.loglevel, "warning");
        // `save 900 1` is warned about; `save ""` is not.
        let ignored: Vec<_> = w.iter().filter(|m| m.contains("--save")).collect();
        assert_eq!(ignored.len(), 2, "{w:?}");
        for name in ["--tcp-keepalive", "--timeout", "--io-threads"] {
            assert_eq!(w.iter().filter(|m| m.contains(name)).count(), 1, "{w:?}");
        }
    }

    #[test]
    fn a_directive_is_checked_as_a_config_line_is() {
        let e = config_of(&["--auto-aof-rewrite-percentage", "lots"]).unwrap_err();
        assert!(
            e.starts_with("--auto-aof-rewrite-percentage: ") && e.contains("lots"),
            "{e}"
        );
        let e = config_of(&["--appendonly", "maybe"]).unwrap_err();
        assert!(e.contains("yes' or 'no"), "{e}");
        let e = config_of(&["--dbfilename"]).unwrap_err();
        assert!(e.contains("needs a value"), "{e}");
        let e = config_of(&["--dbfilename", "a", "b"]).unwrap_err();
        assert!(e.contains("takes one value"), "{e}");
    }

    #[test]
    fn an_unknown_directive_is_named_and_the_nearest_suggested() {
        let e = config_of(&["--frobnicate", "1"]).unwrap_err();
        assert!(e.contains("'--frobnicate'"), "{e}");
        let e = config_of(&["--dbfilenam", "x"]).unwrap_err();
        assert!(
            e.contains("'--dbfilenam'") && e.contains("did you mean '--dbfilename'"),
            "{e}"
        );
        // A mistyped native flag is suggested too.
        let e = config_of(&["--prot", "6380"]).unwrap_err();
        assert!(e.contains("did you mean '--port'"), "{e}");
    }

    #[test]
    fn a_native_flag_wins_over_a_directive_in_either_order() {
        let (c, _) = config_of(&["--aof", "--appendonly", "no"]).unwrap();
        assert!(c.aof);
        let (c, _) = config_of(&["--appendonly", "no", "--aof"]).unwrap();
        assert!(c.aof);
    }

    #[test]
    fn a_flag_given_twice_is_the_last_one() {
        let (c, _) = config_of(&["--appendfsync", "always", "--APPENDFSYNC", "everysec"]).unwrap();
        assert_eq!(c.appendfsync, "everysec");
        let s = split_of(&[
            "/a.conf",
            "--port",
            "1",
            "--configfile",
            "/b.conf",
            "--port",
            "2",
        ]);
        assert_eq!(native_of(&s), ["--configfile=/b.conf", "--port=2"]);
    }

    #[test]
    fn a_directive_a_flag_overrides_is_checked_but_not_warned_about() {
        let cmd = Config::command();
        let mut config = <Config as clap::FromArgMatches>::from_arg_matches(
            &cmd.clone().try_get_matches_from(["kividb"]).unwrap(),
        )
        .unwrap();
        let said_so = |_: &str| true;
        let w = apply(&mut config, &cmd, &[d("appendfsync", &["no"])], &said_so).unwrap();
        assert!(w.is_empty(), "{w:?}");
        assert_eq!(config.appendfsync, "everysec");
        assert!(
            apply(
                &mut config,
                &cmd,
                &[d("appendfsync", &["sometimes"])],
                &said_so
            )
            .is_err()
        );
    }

    #[test]
    fn directives_win_over_the_config_file() {
        let cmd = Config::command();
        let s = split(&cmd, argv(&["--auto-aof-rewrite-percentage", "150"])).unwrap();
        let matches = cmd.clone().try_get_matches_from(s.native.clone()).unwrap();
        let mut config = <Config as clap::FromArgMatches>::from_arg_matches(&matches).unwrap();
        let from_cli =
            |o: &str| matches.value_source(o) == Some(clap::parser::ValueSource::CommandLine);
        config
            .apply_config_text("auto-aof-rewrite-percentage 120\ndatabases 3\n", &from_cli)
            .unwrap();
        assert_eq!(config.aof_rewrite_percentage, 120);
        apply(&mut config, &cmd, &s.directives, &from_cli).unwrap();
        assert_eq!(config.aof_rewrite_percentage, 150);
        assert_eq!(config.databases, 3);
    }

    #[test]
    fn a_value_with_spaces_after_an_equals_sign() {
        let s = split_of(&["--requirepass=correct horse", "--dir=/data/my dir"]);
        assert!(native_of(&s).iter().all(|a| !a.contains("--requirepass ")));
        assert_eq!(
            native_of(&s),
            ["--requirepass=correct horse", "--dir=/data/my dir"]
        );
        let s = split_of(&["--logfile=/var/log/my app.log", "--dbfilename=a b"]);
        assert_eq!(native_of(&s), ["--logfile=/var/log/my app.log"]);
        assert_eq!(s.directives, vec![d("dbfilename", &["a b"])]);
        let (c, _) = config_of(&["--requirepass=correct horse", "--dir=/data/my dir"]).unwrap();
        assert_eq!(c.requirepass, "correct horse");
        assert_eq!(c.dir, "/data/my dir");
        let (c, _) = config_of(&["--ping", "--requirepass=a b"]).unwrap();
        assert!(c.ping);
        assert_eq!(c.requirepass, "a b");
        // A name with a space before the `=` is a name and a value in one.
        let s = split_of(&["--save 900 1=x"]);
        assert_eq!(s.directives, vec![d("save", &["900", "1=x"])]);
    }

    #[test]
    fn no_message_prints_a_secret() {
        let cmd = Config::command();
        let refused = |args: &[&str]| split(&cmd, argv(args)).unwrap_err();
        // Too many values for a flag, a bad quote, a name with no directive.
        for e in [
            refused(&["--requirepass", "hunter2", "more-hunter2"]),
            refused(&["--requirepass 'hunter2"]),
            refused(&["--=hunter2"]),
        ] {
            assert!(!e.contains("hunter2"), "{e}");
        }
        for args in [
            &["--frobnicate=hunter2"][..],
            &["--frobnicate", "hunter2"][..],
            &["--masterauth", "hunter2", "x"][..],
        ] {
            let e = config_of(args).unwrap_err();
            assert!(!e.contains("hunter2"), "{e}");
        }
        // The refusal of a secret's value leaves the value out.
        let args = vec!["hunter2".to_string()];
        let e = crate::config::redact(
            "requirepass",
            &args,
            "invalid value 'hunter2' for 'requirepass'".into(),
        );
        assert!(!e.contains("hunter2"), "{e}");
        let e = crate::config::redact("maxmemory", &args, "invalid value 'hunter2'".into());
        assert!(e.contains("hunter2"));
        for name in [
            "requirepass",
            "masterauth",
            "masteruser",
            "tls-key-file-pass",
            "client-secret",
        ] {
            assert!(crate::config::is_secret(name), "{name}");
        }
        assert!(!crate::config::is_secret("maxmemory"));
        // A config text refused: the line is not echoed for a secret.
        let mut config = Config::default();
        let e = config
            .apply_config_text("requirepass hunter2 hunter3\n", &|_| false)
            .unwrap_err();
        assert!(!e.contains("hunter"), "{e}");
    }

    #[test]
    fn bind_lists_addresses_as_the_config_file_does() {
        for args in [
            &["--bind", "127.0.0.1", "::1"][..],
            &["--bind", "127.0.0.1 ::1"][..],
        ] {
            let (c, w) = config_of(args).unwrap();
            assert_eq!(c.bind, "127.0.0.1");
            assert_eq!(w.len(), 1, "{w:?}");
            assert!(
                w[0].contains(
                    "'bind' lists 2 addresses; KiviDB listens on one, and uses '127.0.0.1'"
                ),
                "{w:?}"
            );
        }
        // The optional-address prefix, as in the config file.
        let (c, w) = config_of(&["--bind", "-::1"]).unwrap();
        assert_eq!(c.bind, "::1");
        assert!(w.is_empty(), "{w:?}");
        let (c, w) = config_of(&["--bind=10.0.0.1"]).unwrap();
        assert_eq!(c.bind, "10.0.0.1");
        assert!(c.bind_explicit && w.is_empty());
    }

    #[test]
    fn a_trailing_dash_is_standard_input_not_a_value() {
        let s = split_of(&["--dir", "-"]);
        assert!(s.stdin);
        assert_eq!(native_of(&s), ["--dir"]);
        assert!(config_of(&["--dir", "-"]).is_err());
        let e = config_of(&["--dbfilename", "-"]).unwrap_err();
        assert!(e.contains("needs a value"), "{e}");
    }

    #[test]
    fn the_command_lines_of_the_repo_are_read_as_they_were() {
        let table: &[&[&str]] = &[
            // docker/refresh/Dockerfile
            &["--protected-mode", "no"],
            // packaging/kividb.service
            &["--configfile", "/etc/kividb/kividb.conf"],
            // docker/verify/docker-compose*.yml
            &[
                "--port",
                "6380",
                "--requirepass",
                "adminpass",
                "--aclfile",
                "/etc/kividb/users.acl",
            ],
            &[
                "--port",
                "6380",
                "--tls-port",
                "6390",
                "--tls-cert-file",
                "/certs/server.crt",
                "--tls-key-file",
                "/certs/server.key",
                "--requirepass",
                "adminpass",
                "--aclfile",
                "/etc/kividb/users.acl",
            ],
            // docs/INSTALL.md, docs/integrations/*.md
            &["--aof", "--requirepass", "pw"],
            &[
                "--port",
                "6379",
                "--protected-mode",
                "no",
                "--aof",
                "--maxmemory-policy",
                "noeviction",
            ],
            &[
                "--port",
                "6379",
                "--requirepass",
                "difyai123456",
                "--dir",
                "/data",
            ],
            &[
                "--port",
                "6379",
                "--requirepass",
                "pw",
                "--aof",
                "--dir",
                "/home/kividb",
            ],
            &[
                "--port",
                "6379",
                "--requirepass",
                "pw",
                "--maxmemory",
                "1gb",
            ],
            &["--protected-mode", "no", "--aof"],
            &[
                "--maxmemory-policy",
                "volatile-lru",
                "--dir",
                "/home/kividb",
            ],
            // README.md, docs/CONFIGURATION.md
            &[
                "--port",
                "6380",
                "--tls-port",
                "6381",
                "--tls-cert-file",
                "cert.pem",
                "--tls-key-file",
                "key.pem",
            ],
            &["--port", "6501", "--io-model", "tokio"],
            &["--preload-file", "aof:/backups/n1/appendonly.aof.manifest"],
            &["--preload-file", "rdb:/backups/n1/dump.kdb"],
            &["--cluster-enabled", "yes"],
            &["--replicaof", "host", "6379"],
            &["--threads", "8"],
            &["--loglevel", "warning"],
            &["--tls-auth-clients", "no"],
            &["--tls-auth-clients-allowed-names", "*.db1.example"],
            &["--tls-cluster", "yes"],
            &["--ping"],
            &["--ping", "--ping-port", "6390"],
        ];
        for args in table {
            let s = split_of(args);
            assert!(
                s.directives.is_empty(),
                "{args:?} became {:?}",
                s.directives
            );
            config_of(args).unwrap_or_else(|e| panic!("{args:?}: {e}"));
        }
        let (c, _) = config_of(&["--replicaof", "host", "6379"]).unwrap();
        assert_eq!(
            c.replicaof,
            Some(vec!["host".to_string(), "6379".to_string()])
        );
        let (c, _) = config_of(&["--protected-mode", "no"]).unwrap();
        assert!(!c.protected_mode);
        // `--version` and `--help` stay clap's.
        assert_eq!(native_of(&split_of(&["--version"])), ["--version"]);
    }

    #[test]
    fn edit_distance_and_closest() {
        assert_eq!(edit_distance("port", "prot"), 2);
        assert_eq!(edit_distance("", "abc"), 3);
        assert_eq!(closest("xyzxyzxyz", ["port", "bind"].into_iter()), None);
    }
}
