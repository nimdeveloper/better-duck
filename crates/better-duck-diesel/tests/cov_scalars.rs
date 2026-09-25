#![allow(missing_docs)]

//! Coverage: call every scalar DSL function at least once so its generated
//! `QueryFragment` code runs. Values are executed (not read), so return types
//! don't need to round-trip here.

use better_duck_diesel::dsl::*;
use better_duck_diesel::DuckDbConnection;
use diesel::prelude::*;
use diesel::sql_types::{Double, Integer, Text};

fn conn() -> DuckDbConnection {
    DuckDbConnection::establish(":memory:").unwrap()
}

#[test]
fn numeric_functions() {
    let mut c = conn();
    diesel::select(abs::<Double, _>(1.0_f64)).execute(&mut c).unwrap();
    diesel::select(sqrt(2.0_f64)).execute(&mut c).unwrap();
    diesel::select(pow(2.0_f64, 3.0_f64)).execute(&mut c).unwrap();
    diesel::select(ceil(1.5_f64)).execute(&mut c).unwrap();
    diesel::select(floor(1.5_f64)).execute(&mut c).unwrap();
    diesel::select(round(1.5_f64)).execute(&mut c).unwrap();
    diesel::select(cbrt(8.0_f64)).execute(&mut c).unwrap();
    diesel::select(exp(1.0_f64)).execute(&mut c).unwrap();
    diesel::select(ln(2.0_f64)).execute(&mut c).unwrap();
    diesel::select(log2(2.0_f64)).execute(&mut c).unwrap();
    diesel::select(log10(10.0_f64)).execute(&mut c).unwrap();
    diesel::select(sign(-2.0_f64)).execute(&mut c).unwrap();
    diesel::select(degrees(1.0_f64)).execute(&mut c).unwrap();
    diesel::select(radians(1.0_f64)).execute(&mut c).unwrap();
    diesel::select(sin(1.0_f64)).execute(&mut c).unwrap();
    diesel::select(cos(1.0_f64)).execute(&mut c).unwrap();
    diesel::select(tan(1.0_f64)).execute(&mut c).unwrap();
    diesel::select(asin(0.5_f64)).execute(&mut c).unwrap();
    diesel::select(acos(0.5_f64)).execute(&mut c).unwrap();
    diesel::select(atan(1.0_f64)).execute(&mut c).unwrap();
    diesel::select(atan2(1.0_f64, 1.0_f64)).execute(&mut c).unwrap();
    diesel::select(gcd(12_i64, 18_i64)).execute(&mut c).unwrap();
    diesel::select(lcm(3_i64, 4_i64)).execute(&mut c).unwrap();
    diesel::select(mod_(7_i64, 3_i64)).execute(&mut c).unwrap();
    diesel::select(xor(6_i64, 3_i64)).execute(&mut c).unwrap();
    diesel::select(bit_count(7_i64)).execute(&mut c).unwrap();
    diesel::select(greatest::<Integer, _, _>(1_i32, 2_i32)).execute(&mut c).unwrap();
    diesel::select(least::<Integer, _, _>(1_i32, 2_i32)).execute(&mut c).unwrap();
}
// <APPEND-MARKER>

#[test]
fn string_functions() {
    let mut c = conn();
    diesel::select(lower("A")).execute(&mut c).unwrap();
    diesel::select(upper("a")).execute(&mut c).unwrap();
    diesel::select(length("a")).execute(&mut c).unwrap();
    diesel::select(trim(" a ")).execute(&mut c).unwrap();
    diesel::select(ltrim(" a")).execute(&mut c).unwrap();
    diesel::select(rtrim("a ")).execute(&mut c).unwrap();
    diesel::select(reverse("ab")).execute(&mut c).unwrap();
    diesel::select(repeat("a", 2_i32)).execute(&mut c).unwrap();
    diesel::select(replace("a", "a", "b")).execute(&mut c).unwrap();
    diesel::select(md5("a")).execute(&mut c).unwrap();
    diesel::select(contains("ab", "a")).execute(&mut c).unwrap();
    diesel::select(starts_with("ab", "a")).execute(&mut c).unwrap();
    diesel::select(ends_with("ab", "b")).execute(&mut c).unwrap();
}

#[test]
fn string_search_and_similarity() {
    let mut c = conn();
    diesel::select(strpos("ab", "b")).execute(&mut c).unwrap();
    diesel::select(instr("ab", "b")).execute(&mut c).unwrap();
    diesel::select(substr("abc", 1_i32, 2_i32)).execute(&mut c).unwrap();
    diesel::select(split_part("a,b", ",", 1_i32)).execute(&mut c).unwrap();
    diesel::select(concat_ws(",", "a", "b")).execute(&mut c).unwrap();
    diesel::select(left("ab", 1_i32)).execute(&mut c).unwrap();
    diesel::select(right("ab", 1_i32)).execute(&mut c).unwrap();
    diesel::select(lpad("a", 3_i32, "0")).execute(&mut c).unwrap();
    diesel::select(rpad("a", 3_i32, "0")).execute(&mut c).unwrap();
    diesel::select(ascii("a")).execute(&mut c).unwrap();
    diesel::select(chr(65_i32)).execute(&mut c).unwrap();
    diesel::select(levenshtein("a", "b")).execute(&mut c).unwrap();
    diesel::select(jaccard("a", "b")).execute(&mut c).unwrap();
    diesel::select(hamming("ab", "ac")).execute(&mut c).unwrap();
    diesel::select(mismatches("ab", "ac")).execute(&mut c).unwrap();
    diesel::select(damerau_levenshtein("a", "b")).execute(&mut c).unwrap();
    diesel::select(jaro_similarity("a", "b")).execute(&mut c).unwrap();
    diesel::select(jaro_winkler_similarity("a", "b")).execute(&mut c).unwrap();
}

#[test]
fn conditional_and_regex() {
    let mut c = conn();
    diesel::select(ifnull(nullif::<Text, _, _>("a", "b"), "x")).execute(&mut c).unwrap();
    diesel::select(regexp_matches("a", "a")).execute(&mut c).unwrap();
    diesel::select(regexp_full_match("a", "a")).execute(&mut c).unwrap();
    diesel::select(regexp_replace("a", "a", "b")).execute(&mut c).unwrap();
    diesel::select(regexp_extract("aXb", "[A-Z]")).execute(&mut c).unwrap();
}
// <APPEND-MARKER-2>

#[test]
fn datetime_functions() {
    let mut c = conn();
    diesel::select(year(make_timestamp(0_i64))).execute(&mut c).unwrap();
    diesel::select(month(make_timestamp(0_i64))).execute(&mut c).unwrap();
    diesel::select(day(make_timestamp(0_i64))).execute(&mut c).unwrap();
    diesel::select(hour(make_timestamp(0_i64))).execute(&mut c).unwrap();
    diesel::select(minute(make_timestamp(0_i64))).execute(&mut c).unwrap();
    diesel::select(second(make_timestamp(0_i64))).execute(&mut c).unwrap();
    diesel::select(date_trunc("day", make_timestamp(0_i64))).execute(&mut c).unwrap();
    diesel::select(strftime(make_timestamp(0_i64), "%Y")).execute(&mut c).unwrap();
    diesel::select(epoch(make_timestamp(0_i64))).execute(&mut c).unwrap();
    diesel::select(make_date(2020_i32, 1_i32, 1_i32)).execute(&mut c).unwrap();
    diesel::select(make_time(1_i32, 2_i32, 3.0_f64)).execute(&mut c).unwrap();
    diesel::select(dayname(make_timestamp(0_i64))).execute(&mut c).unwrap();
    diesel::select(monthname(make_timestamp(0_i64))).execute(&mut c).unwrap();
    diesel::select(last_day(make_date(2020_i32, 1_i32, 1_i32))).execute(&mut c).unwrap();
    diesel::select(dayofweek(make_timestamp(0_i64))).execute(&mut c).unwrap();
    diesel::select(isodow(make_timestamp(0_i64))).execute(&mut c).unwrap();
    diesel::select(dayofyear(make_timestamp(0_i64))).execute(&mut c).unwrap();
    diesel::select(weekofyear(make_timestamp(0_i64))).execute(&mut c).unwrap();
    diesel::select(quarter(make_timestamp(0_i64))).execute(&mut c).unwrap();
    diesel::select(datediff("day", make_timestamp(0_i64), make_timestamp(0_i64)))
        .execute(&mut c)
        .unwrap();
}

#[test]
fn format_hash_blob_uuid_utility() {
    let mut c = conn();
    diesel::select(concat("a", "b")).execute(&mut c).unwrap();
    diesel::select(printf("%s", "a")).execute(&mut c).unwrap();
    diesel::select(format_("{}", "a")).execute(&mut c).unwrap();
    diesel::select(sha256("a")).execute(&mut c).unwrap();
    diesel::select(sha1("a")).execute(&mut c).unwrap();
    diesel::select(encode("a")).execute(&mut c).unwrap();
    diesel::select(decode(encode("a"))).execute(&mut c).unwrap();
    diesel::select(base64(encode("a"))).execute(&mut c).unwrap();
    diesel::select(from_base64("aGk=")).execute(&mut c).unwrap();
    diesel::select(hex(encode("a"))).execute(&mut c).unwrap();
    diesel::select(unhex("48")).execute(&mut c).unwrap();
    diesel::select(gen_random_uuid()).execute(&mut c).unwrap();
    diesel::select(uuidv4()).execute(&mut c).unwrap();
    diesel::select(uuidv7()).execute(&mut c).unwrap();
    diesel::select(version()).execute(&mut c).unwrap();
    diesel::select(type_of::<Integer, _>(1_i32)).execute(&mut c).unwrap();
}

#[test]
fn json_functions_when_available() {
    let mut c = conn();
    if diesel::select(json_valid("{}")).execute(&mut c).is_err() {
        eprintln!("skipping json coverage: `json` extension unavailable");
        return;
    }
    diesel::select(json_type("{}")).execute(&mut c).unwrap();
    diesel::select(json_extract_string("{\"a\":1}", "$.a")).execute(&mut c).unwrap();
    diesel::select(json_array_length("[1,2]")).execute(&mut c).unwrap();
}

diesel::table! {
    lst (id) { id -> Integer, l -> better_duck_diesel::sql_types::DuckList, }
}
diesel::table! {
    mp (id) { id -> Integer, m -> better_duck_diesel::sql_types::DuckMap, }
}

#[test]
fn list_and_map_functions() {
    let mut c = conn();
    diesel::sql_query("CREATE TABLE lst (id INTEGER, l INTEGER[])").execute(&mut c).unwrap();
    diesel::sql_query("INSERT INTO lst VALUES (1, [3, 1, 2, 2])").execute(&mut c).unwrap();
    lst::table.select(len(lst::l)).execute(&mut c).unwrap();
    lst::table.select(array_length(lst::l)).execute(&mut c).unwrap();
    lst::table.select(list_reverse(lst::l)).execute(&mut c).unwrap();
    lst::table.select(list_sort(lst::l)).execute(&mut c).unwrap();
    lst::table.select(list_distinct(lst::l)).execute(&mut c).unwrap();
    lst::table.select(list_concat(lst::l, lst::l)).execute(&mut c).unwrap();
    lst::table.select(array_to_string(lst::l, ",")).execute(&mut c).unwrap();

    diesel::sql_query("CREATE TABLE mp (id INTEGER, m MAP(VARCHAR, INTEGER))")
        .execute(&mut c)
        .unwrap();
    diesel::sql_query("INSERT INTO mp VALUES (1, MAP {'a': 1, 'b': 2})").execute(&mut c).unwrap();
    mp::table.select(cardinality(mp::m)).execute(&mut c).unwrap();
    mp::table.select(map_keys(mp::m)).execute(&mut c).unwrap();
    mp::table.select(map_values(mp::m)).execute(&mut c).unwrap();
}


