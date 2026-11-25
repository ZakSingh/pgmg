//! Which of a routine's references PostgreSQL resolves when the routine is
//! created, and which only when it runs.
//!
//! A function or procedure whose body is a string (`AS $$ ... $$`) stores that
//! body as text. PostgreSQL parses it at call time, so the tables and views it
//! queries are looked up by name on every (re)plan and nothing in the catalog
//! ties the routine to them. The rest of the definition, its *header*, is
//! resolved once at creation: parameter and result types, `%TYPE` references,
//! default expressions. A relation named there is bound for good.
//!
//! pgmg uses this to tell dependents that must be dropped and recreated along
//! with a relation from those that merely need re-checking.

use std::collections::HashSet;
use pg_query::NodeEnum;
use pg_query::protobuf::{KeywordKind, Token};

/// The identifiers a string-bodied routine's header mentions, folded the way
/// PostgreSQL folds them (unquoted names in lower case, quoted names exact).
/// The routine's own name is not included.
///
/// Returns `None` when `ddl` is not a single CREATE FUNCTION / PROCEDURE with a
/// string body (SQL-standard `BEGIN ATOMIC` / `RETURN expr` bodies are parsed
/// at creation, so every reference in them is bound), or when the header
/// cannot be read reliably. Callers treat `None` as "everything is bound".
pub fn routine_header_identifiers(ddl: &str) -> Option<HashSet<String>> {
    let parsed = pg_query::parse(ddl).ok()?;
    let mut stmts = parsed.protobuf.stmts.iter().filter_map(|s| s.stmt.as_ref()?.node.as_ref());
    match (stmts.next(), stmts.next()) {
        (Some(NodeEnum::CreateFunctionStmt(func)), None) if func.sql_body.is_none() => {}
        _ => return None,
    }

    let scanned = pg_query::scan(ddl).ok()?;
    let mut identifiers = HashSet::new();
    // The routine's own (possibly schema-qualified) name runs from the
    // FUNCTION / PROCEDURE keyword to the opening parenthesis.
    let mut in_own_name = false;
    let mut seen_own_name = false;

    for tok in &scanned.tokens {
        match tok.token() {
            Token::SqlComment | Token::CComment => continue,
            Token::Function | Token::Procedure if !seen_own_name => {
                in_own_name = true;
                seen_own_name = true;
                continue;
            }
            Token::Ascii40 if in_own_name => {
                in_own_name = false;
                continue;
            }
            // The body, and any other literal: never a bound reference.
            Token::Sconst | Token::Usconst | Token::Bconst | Token::Xconst
            | Token::Iconst | Token::Fconst | Token::Param => continue,
            // U&"..." would need unicode-unescaping to compare.
            Token::Uident => return None,
            _ => {}
        }
        if in_own_name {
            continue;
        }
        if let Some(name) = identifier_text(ddl, tok) {
            identifiers.insert(name);
        }
    }

    Some(identifiers)
}

/// True when `ddl` is a routine whose body PostgreSQL parses at call time.
pub fn has_late_bound_body(ddl: &str) -> bool {
    routine_header_identifiers(ddl).is_some()
}

/// True when the routine defined by `ddl` only reaches the relation called
/// `relation_name` from inside its string body, so nothing binds it to that
/// relation. False for anything that is not a string-bodied routine.
pub fn references_only_from_body(ddl: &str, relation_name: &str) -> bool {
    routine_header_identifiers(ddl).is_some_and(|header| !header.contains(relation_name))
}

/// The name a token denotes if it can be an identifier: a bare or quoted
/// identifier, or a keyword PostgreSQL also accepts as a name. `None` for
/// punctuation, operators and reserved words.
pub(crate) fn identifier_text(sql: &str, tok: &pg_query::protobuf::ScanToken) -> Option<String> {
    let start = usize::try_from(tok.start).ok()?;
    let end = usize::try_from(tok.end).ok()?;
    let text = sql.get(start..end)?;

    if let Some(quoted) = text.strip_prefix('"').and_then(|t| t.strip_suffix('"')) {
        Some(quoted.replace("\"\"", "\""))
    } else if tok.token() == Token::Ident
        || !matches!(tok.keyword_kind(), KeywordKind::NoKeyword | KeywordKind::ReservedKeyword)
    {
        Some(text.to_lowercase())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header(ddl: &str) -> HashSet<String> {
        routine_header_identifiers(ddl).expect("a string-bodied routine")
    }

    #[test]
    fn header_holds_signature_names_but_not_the_body() {
        let h = header(
            "create function api.list_items(p_seller bigint, p_status item_status default 'live') \
             returns setof core.item_card language plpgsql stable as $$ \
             begin return query select * from core.item_card c join listing l on true; end $$",
        );
        for name in ["p_seller", "bigint", "p_status", "item_status", "core", "item_card"] {
            assert!(h.contains(name), "{name} missing from {h:?}");
        }
        // Only in the body
        assert!(!h.contains("listing"));
        assert!(!h.contains("c"));
        // The routine's own name
        assert!(!h.contains("list_items"));
        assert!(!h.contains("api"));
    }

    #[test]
    fn own_name_is_excluded_but_a_same_named_type_in_the_signature_is_not() {
        let ddl = "create function item(p_id bigint) returns int language sql as $$ select count(*)::int from item $$";
        assert!(!header(ddl).contains("item"));
        assert!(references_only_from_body(ddl, "item"));

        let ddl = "create function item(p item) returns int language sql as $$ select 1 $$";
        assert!(header(ddl).contains("item"));
        assert!(!references_only_from_body(ddl, "item"));
    }

    #[test]
    fn signature_bindings_are_seen_wherever_they_appear() {
        for ddl in [
            "create function f(p listing) returns int language sql as $$ select 1 $$",
            "create function f(p public.listing) returns int language sql as $$ select 1 $$",
            "create function f(p listing.id%type) returns int language sql as $$ select 1 $$",
            "create function f() returns listing language sql as $$ select l from listing l $$",
            "create function f() returns setof listing language sql as $$ select l from listing l $$",
            "create function f() returns table (l listing) language sql as $$ select l from listing l $$",
            "create function f(out o_listing listing) language sql as $$ select l from listing l $$",
            "create or replace function f(p \"listing\") returns int language sql as 'select 1'",
            "create procedure p(p LISTING) language plpgsql as $$ begin null; end $$",
            "create function f(p int default (null::listing).id) returns int language sql as $$ select 1 $$",
        ] {
            assert!(!references_only_from_body(ddl, "listing"), "{ddl}");
        }
    }

    #[test]
    fn body_only_references_are_late_bound() {
        for ddl in [
            "create function f() returns int language sql as $$ select count(*)::int from listing $$",
            "create function f(p_id bigint) returns int language plpgsql as $$ declare r listing%rowtype; begin return 1; end $$",
            "create function f() returns int language sql as 'select count(*)::int from listing'",
            "create function f() returns int language sql stable -- reads listing\n as $$ select 1 $$",
            "create function f() returns int /* listing */ language sql as $$ select 1 $$",
            "create function f(p text default 'listing') returns int language sql as $$ select 1 $$",
            "create function f(p_listing_id bigint) returns int language sql as $$ select 1 $$",
            "create procedure p() language plpgsql as $$ begin delete from listing; end $$",
        ] {
            assert!(references_only_from_body(ddl, "listing"), "{ddl}");
        }
    }

    #[test]
    fn names_fold_the_way_postgres_folds_them() {
        let h = header("create function f(p \"Listing\", q LISTING, r \"a\"\"b\") returns int language sql as $$ select 1 $$");
        assert!(h.contains("Listing"));
        assert!(h.contains("listing"));
        assert!(h.contains("a\"b"));
    }

    #[test]
    fn anything_else_is_fully_bound() {
        for ddl in [
            // SQL-standard bodies are parsed at creation
            "create function f() returns int language sql begin atomic select count(*)::int from listing; end",
            "create function f() returns int language sql return (select count(*)::int from listing)",
            // Not routines
            "create view v as select * from listing",
            "create trigger t before update on listing for each row execute function f()",
            // Not one statement, not parseable, not safely comparable
            "create function f() returns int language sql as $$ select 1 $$; create function g() returns int language sql as $$ select 1 $$",
            "create function f( returns int",
            "create function f(p U&\"l\\0069sting\") returns int language sql as $$ select 1 $$",
            "",
        ] {
            assert!(routine_header_identifiers(ddl).is_none(), "{ddl}");
            assert!(!references_only_from_body(ddl, "listing"), "{ddl}");
            assert!(!has_late_bound_body(ddl), "{ddl}");
        }
    }
}
