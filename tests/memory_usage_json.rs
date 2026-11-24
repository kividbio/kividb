mod memory_usage;

use memory_usage::*;

fn json_set(key: &[u8], path: &str, text: String) -> Cmd {
    vec![b("JSON.SET"), key.to_vec(), b(path), b(text)]
}

fn object(size: usize, member: fn(usize) -> String) -> String {
    let members: Vec<String> = (0..size)
        .map(|i| format!("\"field:{i}\":{}", member(i)))
        .collect();
    format!("{{{}}}", members.join(","))
}

fn array(size: usize, element: fn(usize) -> String) -> String {
    let elements: Vec<String> = (0..size).map(element).collect();
    format!("[{}]", elements.join(","))
}

fn float(i: usize) -> String {
    let x = (((i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 40) as f32 / (1u32 << 24) as f32)
        * 2.0
        - 1.0;
    format!("{:?}", x as f64)
}

fn case(name: &str, size: usize, len_cmd: &'static [u8], make: Make) -> Case {
    Case {
        name: format!("json {name}"),
        size,
        len_cmd,
        make,
        settle: None,
        tolerance: Tolerance::EXACT,
    }
}

fn cases() -> Vec<Case> {
    let mut cases = Vec::new();
    for size in [1, 3, 4, 7, 8, 14, 15, 64, 500] {
        cases.push(case(
            "object of integers",
            size,
            b"JSON.OBJLEN",
            Box::new(move |k| vec![json_set(k, "$", object(size, |i| i.to_string()))]),
        ));
        cases.push(case(
            "object of 20-byte strings",
            size,
            b"JSON.OBJLEN",
            Box::new(move |k| {
                vec![json_set(
                    k,
                    "$",
                    object(size, |i| format!("\"value-{i:014}\"")),
                )]
            }),
        ));
    }
    for size in [1, 8, 100, 1536] {
        cases.push(case(
            "array of floats",
            size,
            b"JSON.ARRLEN",
            Box::new(move |k| vec![json_set(k, "$", array(size, float))]),
        ));
        cases.push(case(
            "array of objects",
            size,
            b"JSON.ARRLEN",
            Box::new(move |k| {
                vec![json_set(
                    k,
                    "$",
                    array(size, |i| format!("{{\"id\":{i},\"tags\":[\"a\",\"b\"]}}")),
                )]
            }),
        ));
    }
    cases.push(case(
        "document with a 1536-number embedding",
        6,
        b"JSON.OBJLEN",
        Box::new(|k| {
            let text = format!(
                "{{\"prefix\":\"memories\",\"key\":\"{}\",\"namespace\":\"user:42\",\
                 \"created_at\":1732000000,\"content\":\"note about topic 7\",\"embedding\":{}}}",
                String::from_utf8_lossy(k),
                array(1536, float)
            );
            vec![json_set(k, "$", text)]
        }),
    ));
    cases.push(case(
        "array grown by ARRAPPEND",
        100,
        b"JSON.ARRLEN",
        Box::new(|k| {
            let mut cmds = vec![json_set(k, "$", "[]".into())];
            cmds.extend((0..100).map(|i| vec![b("JSON.ARRAPPEND"), k.to_vec(), b("$"), b(i)]));
            cmds
        }),
    ));
    cases.push(case(
        "object grown by JSON.SET of members",
        40,
        b"JSON.OBJLEN",
        Box::new(|k| {
            let mut cmds = vec![json_set(k, "$", "{}".into())];
            cmds.extend((0..40).map(|i| json_set(k, &format!("$.m{i}"), format!("\"v{i}\""))));
            cmds
        }),
    ));
    cases
}

#[test]
fn memory_usage_of_json_documents_is_what_their_keys_take() {
    check(cases());
}
