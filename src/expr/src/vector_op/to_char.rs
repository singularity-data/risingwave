use aho_corasick::AhoCorasickBuilder;
use risingwave_common::array::{BytesGuard, BytesWriter};
use risingwave_common::error::Result;
use risingwave_common::types::NaiveDateTimeWrapper;

/// Compile the pg pattern to chrono pattern.
// TODO: Chrono can not fully support the pg format, so consider using other implementations later.
fn compile_pattern_to_chrono(tmpl: &str) -> String {
    static PG_PATTERNS: &[&str] = &[
        "HH24", "HH12", "HH", "MI", "SS", "YYYY", "YY", "IYYY", "IY", "MM", "DD",
    ];
    static CHRONO_PATTERNS: &[&str] = &[
        "%H", "%I", "%I", "%M", "%S", "%Y", "%Y", "%G", "%g", "%m", "%d",
    ];

    let ac = AhoCorasickBuilder::new()
        .ascii_case_insensitive(false)
        .match_kind(aho_corasick::MatchKind::LeftmostLongest)
        .build(PG_PATTERNS);

    let mut chrono_tmpl = String::new();
    ac.replace_all_with(tmpl, &mut chrono_tmpl, |mat, _, dst| {
        dst.push_str(CHRONO_PATTERNS[mat.pattern()]);
        true
    });

    chrono_tmpl
}

pub fn to_char_timestamp(
    data: NaiveDateTimeWrapper,
    tmpl: &str,
    dst: BytesWriter,
) -> Result<BytesGuard> {
    let chrono_tmpl = compile_pattern_to_chrono(tmpl);
    let res = data.0.format(&chrono_tmpl).to_string();
    dst.write_ref(&res)
}
