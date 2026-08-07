//! LogQL parser subset for the Loki-compatible API (#11): stream
//! selectors with label matchers, line filters, and the metric wrappers
//! Grafana's Loki datasource and Logs Drilldown actually send —
//! `count_over_time`, `rate`, optionally wrapped in `sum` / `sum by (…)`.

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MatchOp {
    Eq,
    Neq,
    Re,
    NotRe,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabelMatcher {
    pub label: String,
    pub op: MatchOp,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterOp {
    Contains,    // |=
    NotContains, // !=
    Regex,       // |~
    NotRegex,    // !~
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineFilter {
    pub op: FilterOp,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LogSelector {
    pub matchers: Vec<LabelMatcher>,
    pub filters: Vec<LineFilter>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricOp {
    CountOverTime,
    Rate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricQuery {
    pub selector: LogSelector,
    pub range_millis: i64,
    pub op: MetricOp,
    /// Labels from `sum by (…)`; empty for plain `sum(…)` or no sum.
    pub group_by: Vec<String>,
    /// Whether a `sum` wrapper was present: `sum(rate(…))` collapses all
    /// series into one, while bare `rate(…)` keeps one series per stream.
    pub summed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogQlQuery {
    Log(LogSelector),
    Metric(MetricQuery),
}

impl LogSelector {
    /// The value of an `=`-matcher for `label`, if present.
    pub fn eq_value(&self, label: &str) -> Option<&str> {
        self.matchers
            .iter()
            .find(|m| m.label == label && m.op == MatchOp::Eq)
            .map(|m| m.value.as_str())
    }
}

struct Parser<'a> {
    input: &'a [u8],
    pos: usize,
}

pub fn parse(input: &str) -> Result<LogQlQuery, String> {
    let mut parser = Parser {
        input: input.as_bytes(),
        pos: 0,
    };
    let query = parser.query()?;
    parser.skip_ws();
    if parser.pos != parser.input.len() {
        return Err(format!(
            "unexpected trailing input at byte {}: {:?}",
            parser.pos,
            &input[parser.pos..]
        ));
    }
    Ok(query)
}

impl<'a> Parser<'a> {
    fn skip_ws(&mut self) {
        while self.pos < self.input.len() && self.input[self.pos].is_ascii_whitespace() {
            self.pos += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.input.get(self.pos).copied()
    }

    fn eat(&mut self, token: &str) -> bool {
        self.skip_ws();
        if self.input[self.pos..].starts_with(token.as_bytes()) {
            self.pos += token.len();
            true
        } else {
            false
        }
    }

    fn expect(&mut self, token: &str) -> Result<(), String> {
        if self.eat(token) {
            Ok(())
        } else {
            Err(format!("expected '{token}' at byte {}", self.pos))
        }
    }

    fn ident(&mut self) -> Result<String, String> {
        self.skip_ws();
        let start = self.pos;
        while self
            .peek()
            .map(|b| b.is_ascii_alphanumeric() || b == b'_')
            .unwrap_or(false)
        {
            self.pos += 1;
        }
        if self.pos == start {
            return Err(format!("expected identifier at byte {}", self.pos));
        }
        Ok(String::from_utf8_lossy(&self.input[start..self.pos]).into_owned())
    }

    /// Double-quoted string with escapes, or a backtick raw string.
    fn string(&mut self) -> Result<String, String> {
        self.skip_ws();
        match self.peek() {
            Some(b'"') => {
                self.pos += 1;
                let mut out = String::new();
                loop {
                    match self.peek() {
                        None => return Err("unterminated string".into()),
                        Some(b'"') => {
                            self.pos += 1;
                            return Ok(out);
                        }
                        Some(b'\\') => {
                            self.pos += 1;
                            match self.peek() {
                                Some(b'n') => {
                                    out.push('\n');
                                    self.pos += 1;
                                }
                                Some(b't') => {
                                    out.push('\t');
                                    self.pos += 1;
                                }
                                Some(b'r') => {
                                    out.push('\r');
                                    self.pos += 1;
                                }
                                // Go-style hex/unicode escapes (LogQL strings
                                // follow Go string literal syntax).
                                Some(b'x') => {
                                    self.pos += 1;
                                    out.push(self.hex_escape(2)?);
                                }
                                Some(b'u') => {
                                    self.pos += 1;
                                    out.push(self.hex_escape(4)?);
                                }
                                Some(b'U') => {
                                    self.pos += 1;
                                    out.push(self.hex_escape(8)?);
                                }
                                Some(_) => {
                                    // Any other escaped char passes through
                                    // verbatim — decoded as a full UTF-8
                                    // scalar, not a single byte.
                                    let rest = std::str::from_utf8(&self.input[self.pos..])
                                        .map_err(|_| "invalid UTF-8 in string".to_string())?;
                                    let ch = rest.chars().next().unwrap();
                                    out.push(ch);
                                    self.pos += ch.len_utf8();
                                }
                                None => return Err("unterminated escape".into()),
                            }
                        }
                        Some(_) => {
                            // Consume one UTF-8 scalar, not one byte.
                            let rest = std::str::from_utf8(&self.input[self.pos..])
                                .map_err(|_| "invalid UTF-8 in string".to_string())?;
                            let ch = rest.chars().next().unwrap();
                            out.push(ch);
                            self.pos += ch.len_utf8();
                        }
                    }
                }
            }
            Some(b'`') => {
                self.pos += 1;
                let start = self.pos;
                while self.peek().map(|b| b != b'`').unwrap_or(false) {
                    self.pos += 1;
                }
                if self.peek().is_none() {
                    return Err("unterminated raw string".into());
                }
                let out = String::from_utf8_lossy(&self.input[start..self.pos]).into_owned();
                self.pos += 1;
                Ok(out)
            }
            _ => Err(format!("expected string at byte {}", self.pos)),
        }
    }

    /// `\xNN` / `\uNNNN` / `\UNNNNNNNN` escape body: `digits` hex chars.
    fn hex_escape(&mut self, digits: usize) -> Result<char, String> {
        if self.pos + digits > self.input.len() {
            return Err("truncated hex escape".into());
        }
        let text = std::str::from_utf8(&self.input[self.pos..self.pos + digits])
            .map_err(|_| "invalid hex escape".to_string())?;
        let code = u32::from_str_radix(text, 16).map_err(|_| "invalid hex escape".to_string())?;
        self.pos += digits;
        char::from_u32(code).ok_or_else(|| "escape is not a valid scalar".to_string())
    }

    /// `[5m]`-style range: sequence of number+unit components.
    fn duration_millis(&mut self) -> Result<i64, String> {
        self.skip_ws();
        let mut total: i64 = 0;
        let mut any = false;
        loop {
            let start = self.pos;
            while self.peek().map(|b| b.is_ascii_digit()).unwrap_or(false) {
                self.pos += 1;
            }
            if self.pos == start {
                break;
            }
            let n: i64 = std::str::from_utf8(&self.input[start..self.pos])
                .unwrap()
                .parse()
                .map_err(|_| "duration number too large".to_string())?;
            let unit_millis = if self.eat("ms") {
                1
            } else if self.eat("s") {
                1000
            } else if self.eat("m") {
                60_000
            } else if self.eat("h") {
                3_600_000
            } else if self.eat("d") {
                86_400_000
            } else if self.eat("w") {
                7 * 86_400_000
            } else {
                return Err(format!("expected duration unit at byte {}", self.pos));
            };
            total = total.saturating_add(n.saturating_mul(unit_millis));
            any = true;
        }
        if !any {
            return Err(format!("expected duration at byte {}", self.pos));
        }
        Ok(total)
    }

    fn query(&mut self) -> Result<LogQlQuery, String> {
        self.skip_ws();
        // sum [by (l1, l2)] ( <range-fn> ) — or a bare range-fn/selector.
        if self.eat("sum") {
            let mut group_by = Vec::new();
            if self.eat("by") {
                self.expect("(")?;
                loop {
                    group_by.push(self.ident()?);
                    if !self.eat(",") {
                        break;
                    }
                }
                self.expect(")")?;
            }
            self.expect("(")?;
            let mut metric = self.range_fn()?;
            self.expect(")")?;
            metric.group_by = group_by;
            metric.summed = true;
            return Ok(LogQlQuery::Metric(metric));
        }
        if self.looking_at_range_fn() {
            return Ok(LogQlQuery::Metric(self.range_fn()?));
        }
        Ok(LogQlQuery::Log(self.selector_with_filters()?))
    }

    fn looking_at_range_fn(&self) -> bool {
        let rest = &self.input[self.pos..];
        rest.starts_with(b"count_over_time") || rest.starts_with(b"rate")
    }

    fn range_fn(&mut self) -> Result<MetricQuery, String> {
        self.skip_ws();
        let op = if self.eat("count_over_time") {
            MetricOp::CountOverTime
        } else if self.eat("rate") {
            MetricOp::Rate
        } else {
            return Err(format!(
                "expected count_over_time or rate at byte {} (supported metric functions)",
                self.pos
            ));
        };
        self.expect("(")?;
        let selector = self.selector_with_filters()?;
        self.expect("[")?;
        let range_millis = self.duration_millis()?;
        self.expect("]")?;
        self.expect(")")?;
        Ok(MetricQuery {
            selector,
            range_millis,
            op,
            group_by: Vec::new(),
            summed: false,
        })
    }

    fn selector_with_filters(&mut self) -> Result<LogSelector, String> {
        self.expect("{")?;
        let mut matchers = Vec::new();
        self.skip_ws();
        if self.peek() != Some(b'}') {
            loop {
                let label = self.ident()?;
                self.skip_ws();
                let op = if self.eat("=~") {
                    MatchOp::Re
                } else if self.eat("!~") {
                    MatchOp::NotRe
                } else if self.eat("!=") {
                    MatchOp::Neq
                } else if self.eat("=") {
                    MatchOp::Eq
                } else {
                    return Err(format!("expected label operator at byte {}", self.pos));
                };
                let value = self.string()?;
                matchers.push(LabelMatcher { label, op, value });
                if !self.eat(",") {
                    break;
                }
            }
        }
        self.expect("}")?;

        let mut filters = Vec::new();
        loop {
            self.skip_ws();
            let op = if self.eat("|=") {
                FilterOp::Contains
            } else if self.eat("!=") {
                FilterOp::NotContains
            } else if self.eat("|~") {
                FilterOp::Regex
            } else if self.eat("!~") {
                FilterOp::NotRegex
            } else {
                break;
            };
            let text = self.string()?;
            filters.push(LineFilter { op, text });
        }
        Ok(LogSelector { matchers, filters })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_selector_and_filters() {
        let q = parse(r#"{service_name="api", level!="debug"} |= "timeout" != `retry`"#).unwrap();
        let LogQlQuery::Log(sel) = q else { panic!() };
        assert_eq!(sel.matchers.len(), 2);
        assert_eq!(sel.eq_value("service_name"), Some("api"));
        assert_eq!(sel.matchers[1].op, MatchOp::Neq);
        assert_eq!(sel.filters.len(), 2);
        assert_eq!(sel.filters[0].op, FilterOp::Contains);
        assert_eq!(sel.filters[1].op, FilterOp::NotContains);
        assert_eq!(sel.filters[1].text, "retry");
    }

    #[test]
    fn parses_metric_wrappers() {
        let q = parse(r#"sum by (level) (count_over_time({service_name="api"} |= "err" [5m]))"#)
            .unwrap();
        let LogQlQuery::Metric(m) = q else { panic!() };
        assert_eq!(m.op, MetricOp::CountOverTime);
        assert_eq!(m.range_millis, 300_000);
        assert_eq!(m.group_by, vec!["level"]);
        assert_eq!(m.selector.filters.len(), 1);

        let q = parse(r#"rate({service_name="api"}[1m30s])"#).unwrap();
        let LogQlQuery::Metric(m) = q else { panic!() };
        assert_eq!(m.op, MetricOp::Rate);
        assert_eq!(m.range_millis, 90_000);
        assert!(m.group_by.is_empty());

        let q = parse(r#"sum(count_over_time({app="x"}[1h]))"#).unwrap();
        let LogQlQuery::Metric(m) = q else { panic!() };
        assert!(m.group_by.is_empty());
        assert!(m.summed);
        assert_eq!(m.range_millis, 3_600_000);
    }

    #[test]
    fn parses_regex_matchers_and_empty_selector() {
        let q = parse(r#"{service_name=~"app-.+"}"#).unwrap();
        let LogQlQuery::Log(sel) = q else { panic!() };
        assert_eq!(sel.matchers[0].op, MatchOp::Re);

        let q = parse("{}").unwrap();
        let LogQlQuery::Log(sel) = q else { panic!() };
        assert!(sel.matchers.is_empty());
    }

    #[test]
    fn string_escapes() {
        let q = parse(r#"{a="\u0041\x42-\"q\"-é"}"#).unwrap();
        let LogQlQuery::Log(sel) = q else { panic!() };
        assert_eq!(sel.matchers[0].value, "AB-\"q\"-é");
        // Escaped multibyte char passes through without desyncing.
        let q = parse("{a=\"\\é\"}").unwrap();
        let LogQlQuery::Log(sel) = q else { panic!() };
        assert_eq!(sel.matchers[0].value, "é");
        assert!(parse(r#"{a="\uD800"}"#).is_err()); // surrogate: not a scalar
    }

    #[test]
    fn rejects_garbage() {
        assert!(parse("").is_err());
        assert!(parse("{unclosed=\"x\"").is_err());
        assert!(parse(r#"avg(count_over_time({a="b"}[5m]))"#).is_err());
        assert!(parse(r#"{a="b"} trailing"#).is_err());
    }
}
