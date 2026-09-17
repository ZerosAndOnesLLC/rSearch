//! Keyword `normalizer` support (issue #85): a fixed, non-tokenizing
//! filter chain applied to a keyword field's value at index time *and* to
//! the query input of every term-level query, exactly as OpenSearch does.
//!
//! OpenSearch builds normalizers from `settings.analysis.normalizer` and
//! allows only filters that cannot split a value into several tokens. The
//! chain is resolved when the mapping is stored, so a split carries the
//! filters it was written with and never depends on settings a later node
//! may not have.

use std::borrow::Cow;

use serde::{Deserialize, Serialize};
use tantivy::tokenizer::{AsciiFoldingFilter, RawTokenizer, TextAnalyzer};

/// One filter in a normalizer chain. Only OpenSearch's non-tokenizing
/// filters that have a meaning for a whole keyword value are supported.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NormalizerFilter {
    /// `lowercase`.
    Lowercase,
    /// `uppercase`.
    Uppercase,
    /// `asciifolding`: fold non-ASCII characters to their ASCII form.
    AsciiFolding,
    /// `trim`: strip leading and trailing whitespace.
    Trim,
}

impl NormalizerFilter {
    /// Parse an OpenSearch token-filter name.
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "lowercase" => Some(Self::Lowercase),
            "uppercase" => Some(Self::Uppercase),
            "asciifolding" => Some(Self::AsciiFolding),
            "trim" => Some(Self::Trim),
            _ => None,
        }
    }

    /// The OpenSearch name this filter was parsed from.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Lowercase => "lowercase",
            Self::Uppercase => "uppercase",
            Self::AsciiFolding => "asciifolding",
            Self::Trim => "trim",
        }
    }

    fn apply<'a>(&self, value: Cow<'a, str>) -> Cow<'a, str> {
        match self {
            Self::Lowercase => {
                if value.chars().any(char::is_uppercase) {
                    Cow::Owned(value.to_lowercase())
                } else {
                    value
                }
            }
            Self::Uppercase => {
                if value.chars().any(char::is_lowercase) {
                    Cow::Owned(value.to_uppercase())
                } else {
                    value
                }
            }
            Self::Trim => match value {
                Cow::Borrowed(s) => Cow::Borrowed(s.trim()),
                Cow::Owned(s) => Cow::Owned(s.trim().to_string()),
            },
            Self::AsciiFolding => {
                if value.is_ascii() {
                    return value;
                }
                // Tantivy's folding filter works on a token stream; the
                // raw tokenizer makes the whole value one token, which is
                // what a normalizer folds.
                let mut analyzer = TextAnalyzer::builder(RawTokenizer::default())
                    .filter(AsciiFoldingFilter)
                    .build();
                let mut stream = analyzer.token_stream(&value);
                match stream.next() {
                    Some(token) => Cow::Owned(token.text.clone()),
                    None => Cow::Owned(String::new()),
                }
            }
        }
    }
}

/// A resolved normalizer: the filter chain, in declaration order.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Normalizer {
    /// Filters applied left to right.
    pub filters: Vec<NormalizerFilter>,
}

impl Normalizer {
    /// Apply the chain to a value. Borrowed input is returned untouched
    /// when no filter changes it, which is the common case on ingest.
    pub fn apply<'a>(&self, value: &'a str) -> Cow<'a, str> {
        self.filters
            .iter()
            .fold(Cow::Borrowed(value), |acc, filter| filter.apply(acc))
    }

    /// The ES-shaped definition (`settings.analysis.normalizer.<name>`).
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "type": "custom",
            "filter": self.filters.iter().map(|f| f.as_str()).collect::<Vec<_>>(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn applies_filters_in_order() {
        let lower = Normalizer { filters: vec![NormalizerFilter::Lowercase] };
        assert_eq!(lower.apply("Kirsten Andersen"), "kirsten andersen");
        assert_eq!(lower.apply("already"), "already");

        let chain = Normalizer {
            filters: vec![
                NormalizerFilter::Trim,
                NormalizerFilter::AsciiFolding,
                NormalizerFilter::Lowercase,
            ],
        };
        assert_eq!(chain.apply("  Ångström  "), "angstrom");
        assert_eq!(chain.apply("PLAIN"), "plain");
    }

    #[test]
    fn unknown_filter_names_are_rejected() {
        assert!(NormalizerFilter::parse("stop").is_none());
        assert_eq!(NormalizerFilter::parse("lowercase"), Some(NormalizerFilter::Lowercase));
    }
}
