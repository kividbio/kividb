// tests/expr.rs
//
// Unit tests for src/commands/expr.rs — the expression evaluator used by
// FT.AGGREGATE FILTER and APPLY clauses.
//
// These tests run entirely in-process: no server, no TCP, no RESP parsing.
// They are instantaneous and can catch regressions in the evaluator without
// any infrastructure.
//
// Run with: cargo test --test expr

use kividb::commands::expr::{Value, eval, parse};

// ── Helper ────────────────────────────────────────────────────────────────────

/// Build a row (AHashMap<String, String>) from alternating key/value pairs.
/// Usage: row!("price" => "100", "category" => "electronics")
macro_rules! row {
    ($($k:expr => $v:expr),* $(,)?) => {{
        #[allow(unused_mut)]
        let mut m = ahash::AHashMap::new();
        $( m.insert($k.to_string(), $v.to_string()); )*
        m
    }};
}

/// Parse and evaluate an expression against a row, returning the Value.
fn run(expr: &str, row: &ahash::AHashMap<String, String>) -> Value {
    let ast = parse(expr).expect("parse failed");
    eval(&ast, row)
}

/// Shorthand: evaluate and assert the result is a specific f64.
fn assert_num(expr: &str, row: &ahash::AHashMap<String, String>, expected: f64) {
    let v = run(expr, row);
    let got = v.as_num();
    assert!(
        (got - expected).abs() < 1e-9,
        "expr={expr:?} expected={expected} got={got}"
    );
}

/// Shorthand: evaluate and assert the result is a specific string.
fn assert_str(expr: &str, row: &ahash::AHashMap<String, String>, expected: &str) {
    let v = run(expr, row);
    assert_eq!(v.as_str(), expected, "expr={expr:?}");
}

/// Shorthand: evaluate and assert the result is a specific bool.
fn assert_bool(expr: &str, row: &ahash::AHashMap<String, String>, expected: bool) {
    let v = run(expr, row);
    assert_eq!(v.is_truthy(), expected, "expr={expr:?}");
}

// ── Literals ──────────────────────────────────────────────────────────────────

#[test]
fn test_numeric_literal() {
    let r = row!();
    assert_num("42", &r, 42.0);
    assert_num("3.15", &r, 3.15);
    assert_num("0", &r, 0.0);
    assert_num("-7", &r, -7.0);
}

#[test]
fn test_string_literal() {
    let r = row!();
    assert_str(r#""hello""#, &r, "hello");
    assert_str(r#""""#, &r, "");
}

// ── Field access ──────────────────────────────────────────────────────────────

#[test]
fn test_field_numeric() {
    let r = row!("price" => "150");
    assert_num("@price", &r, 150.0);
}

#[test]
fn test_field_string() {
    let r = row!("category" => "electronics");
    assert_str("@category", &r, "electronics");
}

#[test]
fn test_field_missing_is_nil() {
    let r = row!();
    let v = run("@nosuch", &r);
    assert!(matches!(v, Value::Nil));
}

#[test]
fn test_field_missing_as_num_is_zero() {
    // Missing fields coerce to 0 in arithmetic — Redis semantics
    let r = row!();
    assert_num("@missing + 5", &r, 5.0);
}

// ── Arithmetic ────────────────────────────────────────────────────────────────

#[test]
fn test_add() {
    let r = row!("a" => "10", "b" => "3");
    assert_num("@a + @b", &r, 13.0);
    assert_num("@a + 5", &r, 15.0);
    assert_num("1 + 2 + 3", &r, 6.0);
}

#[test]
fn test_sub() {
    let r = row!("a" => "10", "b" => "3");
    assert_num("@a - @b", &r, 7.0);
    assert_num("@a - 15", &r, -5.0);
}

#[test]
fn test_mul() {
    let r = row!("price" => "100");
    assert_num("@price * 1.1", &r, 110.0);
    assert_num("3 * 4", &r, 12.0);
}

#[test]
fn test_div() {
    let r = row!("total" => "100", "qty" => "4");
    assert_num("@total / @qty", &r, 25.0);
    assert_num("10 / 4", &r, 2.5);
}

#[test]
fn test_div_by_zero_returns_nil() {
    let r = row!();
    let v = run("10 / 0", &r);
    assert!(matches!(v, Value::Nil));
}

#[test]
fn test_mod() {
    let r = row!();
    assert_num("10 % 3", &r, 1.0);
    assert_num("9 % 3", &r, 0.0);
}

#[test]
fn test_mod_by_zero_returns_nil() {
    let r = row!();
    let v = run("10 % 0", &r);
    assert!(matches!(v, Value::Nil));
}

#[test]
fn test_operator_precedence() {
    let r = row!();
    // 2 + 3 * 4 = 14, not 20
    assert_num("2 + 3 * 4", &r, 14.0);
    // (2 + 3) * 4 = 20
    assert_num("(2 + 3) * 4", &r, 20.0);
    // -2 * 3 = -6
    assert_num("-2 * 3", &r, -6.0);
}

#[test]
fn test_unary_minus() {
    let r = row!("n" => "5");
    assert_num("-@n", &r, -5.0);
    assert_num("- -@n", &r, 5.0);
}

// ── Comparisons ───────────────────────────────────────────────────────────────

#[test]
fn test_eq() {
    let r = row!("cat" => "electronics");
    assert_bool(r#"@cat == "electronics""#, &r, true);
    assert_bool(r#"@cat == "furniture""#, &r, false);
    assert_bool("5 == 5", &r, true);
    assert_bool("5 == 6", &r, false);
}

#[test]
fn test_ne() {
    let r = row!("n" => "10");
    assert_bool("@n != 5", &r, true);
    assert_bool("@n != 10", &r, false);
}

#[test]
fn test_lt_le_gt_ge() {
    let r = row!("price" => "100");
    assert_bool("@price < 200", &r, true);
    assert_bool("@price < 100", &r, false);
    assert_bool("@price <= 100", &r, true);
    assert_bool("@price <= 99", &r, false);
    assert_bool("@price > 50", &r, true);
    assert_bool("@price > 100", &r, false);
    assert_bool("@price >= 100", &r, true);
    assert_bool("@price >= 101", &r, false);
}

// ── Logic ─────────────────────────────────────────────────────────────────────

#[test]
fn test_and() {
    let r = row!("a" => "5", "b" => "10");
    assert_bool("@a > 1 && @b > 1", &r, true);
    assert_bool("@a > 10 && @b > 1", &r, false);
    assert_bool("@a > 1 && @b > 100", &r, false);
}

#[test]
fn test_or() {
    let r = row!("a" => "5", "b" => "10");
    assert_bool("@a > 100 || @b > 1", &r, true);
    assert_bool("@a > 100 || @b > 100", &r, false);
}

#[test]
fn test_not() {
    let r = row!("flag" => "0");
    assert_bool("!@flag", &r, true);
    assert_bool("!1", &r, false);
}

#[test]
fn test_and_short_circuits() {
    // If left is false, right side (division by zero) must not execute
    let r = row!();
    let v = run("0 && 10 / 0", &r);
    // Should be Bool(false), not Nil from the division
    assert!(matches!(v, Value::Bool(false)));
}

#[test]
fn test_or_short_circuits() {
    // If left is true, right side must not execute
    let r = row!();
    let v = run("1 || 10 / 0", &r);
    assert!(matches!(v, Value::Bool(true)));
}

#[test]
fn test_combined_filter_expression() {
    // Real-world FILTER expression pattern
    let r = row!("cnt" => "3", "category" => "electronics");
    assert_bool("@cnt > 2", &r, true);
    assert_bool("@cnt > 2 && @cnt < 10", &r, true);
    assert_bool("@cnt > 2 && @cnt < 3", &r, false);
}

// ── String functions ──────────────────────────────────────────────────────────

#[test]
fn test_upper_lower() {
    let r = row!("cat" => "Electronics");
    assert_str("upper(@cat)", &r, "ELECTRONICS");
    assert_str("lower(@cat)", &r, "electronics");
}

#[test]
fn test_strlen() {
    let r = row!("s" => "hello");
    assert_num("strlen(@s)", &r, 5.0);
    assert_num("length(@s)", &r, 5.0);
    assert_num("strlen(\"\")", &r, 0.0);
}

#[test]
fn test_startswith() {
    let r = row!("email" => "alice@example.com");
    assert_bool(r#"startswith(@email, "alice")"#, &r, true);
    assert_bool(r#"startswith(@email, "bob")"#, &r, false);
}

#[test]
fn test_contains() {
    let r = row!("name" => "MacBook Pro");
    assert_bool(r#"contains(@name, "Book")"#, &r, true);
    assert_bool(r#"contains(@name, "iPad")"#, &r, false);
}

#[test]
fn test_split_part() {
    let r = row!("email" => "alice@example.com");
    // split_part(str, sep, n) — 1-indexed
    assert_str(r#"split_part(@email, "@", 1)"#, &r, "alice");
    assert_str(r#"split_part(@email, "@", 2)"#, &r, "example.com");
}

#[test]
fn test_substr() {
    let r = row!("s" => "hello world");
    assert_str("substr(@s, 0, 5)", &r, "hello");
    assert_str("substr(@s, 6, 5)", &r, "world");
}

#[test]
fn test_trim_variants() {
    let r = row!("s" => "  hello  ");
    assert_str("trim(@s)", &r, "hello");
    assert_str("ltrim(@s)", &r, "hello  ");
    assert_str("rtrim(@s)", &r, "  hello");
}

// ── Math functions ────────────────────────────────────────────────────────────

#[test]
fn test_abs() {
    let r = row!("n" => "-5");
    assert_num("abs(@n)", &r, 5.0);
    assert_num("abs(3)", &r, 3.0);
}

#[test]
fn test_ceil_floor_round() {
    let r = row!();
    assert_num("ceil(3.1)", &r, 4.0);
    assert_num("floor(3.9)", &r, 3.0);
    assert_num("round(3.5)", &r, 4.0);
    assert_num("round(3.4)", &r, 3.0);
}

#[test]
fn test_log2_sqrt() {
    let r = row!();
    assert_num("log2(8)", &r, 3.0);
    assert_num("sqrt(9)", &r, 3.0);
}

#[test]
fn test_pow() {
    let r = row!();
    assert_num("pow(2, 10)", &r, 1024.0);
    assert_num("pow(3, 0)", &r, 1.0);
}

// ── Type and existence functions ──────────────────────────────────────────────

#[test]
fn test_exists() {
    let r = row!("present" => "yes");
    assert_bool("exists(@present)", &r, true);
    assert_bool("exists(@missing)", &r, false);
}

#[test]
fn test_isnum() {
    let r = row!("n" => "42", "s" => "hello");
    assert_bool("isnum(@n)", &r, true);
    assert_bool("isnum(@s)", &r, false);
    assert_bool("isnumeric(@n)", &r, true);
}

// ── Conditional ───────────────────────────────────────────────────────────────

#[test]
fn test_if() {
    let r = row!("score" => "90");
    assert_str(r#"if(@score >= 90, "pass", "fail")"#, &r, "pass");
    assert_str(r#"if(@score < 50, "fail", "pass")"#, &r, "pass");
}

#[test]
fn test_if_with_numeric_result() {
    let r = row!("price" => "200");
    // Apply 20% discount if price > 100, else no discount
    assert_num("if(@price > 100, @price * 0.8, @price)", &r, 160.0);
}

// ── Type conversion ───────────────────────────────────────────────────────────

#[test]
fn test_tostring_tonumber() {
    let r = row!("n" => "42");
    assert_str("tostring(@n)", &r, "42");
    assert_num("tonumber(\"3.15\")", &r, 3.15);
    assert_num("tointeger(\"7.9\")", &r, 7.0); // truncates
}

// ── Format ────────────────────────────────────────────────────────────────────

#[test]
fn test_format_string() {
    let r = row!("cat" => "electronics", "price" => "99");
    assert_str(r#"format("%s: %d", @cat, @price)"#, &r, "electronics: 99");
}

#[test]
fn test_format_float() {
    let r = row!("val" => "3.14159");
    let result = run(r#"format("%f", @val)"#, &r);
    // %f gives 6 decimal places
    assert!(
        result.as_str().starts_with("3.14159"),
        "got: {}",
        result.as_str()
    );
}

#[test]
fn test_format_escaped_percent() {
    let r = row!();
    assert_str(r#"format("100%%")"#, &r, "100%");
}

// ── Real-world compound expressions ──────────────────────────────────────────

#[test]
fn test_price_with_tax_apply() {
    // APPLY "@price * 1.1" AS price_with_tax
    let r = row!("price" => "499");
    assert_num("@price * 1.1", &r, 548.9);
}

#[test]
fn test_filter_after_groupby() {
    // FILTER "@cnt > 2" — cnt comes from a REDUCE COUNT
    let r = row!("cnt" => "3");
    assert_bool("@cnt > 2", &r, true);

    let r2 = row!("cnt" => "1");
    assert_bool("@cnt > 2", &r2, false);
}

#[test]
fn test_label_formatting() {
    // APPLY "format(\"%s: %d\", @category, @price)" AS label
    let r = row!("category" => "furniture", "price" => "200");
    assert_str(
        r#"format("%s: %d", @category, @price)"#,
        &r,
        "furniture: 200",
    );
}

#[test]
fn test_category_upper_apply() {
    // APPLY "upper(@category)" AS cat_upper
    let r = row!("category" => "electronics");
    assert_str("upper(@category)", &r, "ELECTRONICS");
}

#[test]
fn test_email_domain_extraction() {
    // APPLY "split_part(@email, \"@\", 2)" AS domain
    let r = row!("email" => "user@example.com");
    assert_str(r#"split_part(@email, "@", 2)"#, &r, "example.com");
}

#[test]
fn test_conditional_discount() {
    // APPLY "if(@qty > 10, @price * 0.9, @price)" AS final_price
    let r_bulk = row!("qty" => "15", "price" => "100");
    assert_num("if(@qty > 10, @price * 0.9, @price)", &r_bulk, 90.0);

    let r_small = row!("qty" => "5", "price" => "100");
    assert_num("if(@qty > 10, @price * 0.9, @price)", &r_small, 100.0);
}

// ── Parse edge cases ──────────────────────────────────────────────────────────

#[test]
fn test_empty_expr_returns_none() {
    assert!(parse("").is_none());
    assert!(parse("   ").is_none());
}

#[test]
fn test_whitespace_around_operators() {
    let r = row!("a" => "5");
    assert_num("@a + 3", &r, 8.0);
    assert_num("@a+3", &r, 8.0);
    assert_num("@a  +  3", &r, 8.0);
}

#[test]
fn test_nested_function_calls() {
    let r = row!("s" => "  HELLO world  ");
    // lower(trim(...))
    assert_str("lower(trim(@s))", &r, "hello world");
    // upper(lower(@s))
    assert_str("upper(lower(@s))", &r, "  HELLO WORLD  ");
}
