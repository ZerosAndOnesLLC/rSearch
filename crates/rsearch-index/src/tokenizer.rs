//! The `standard` analyzer: OpenSearch/Elasticsearch `standard` semantics
//! on Tantivy.
//!
//! Tantivy's built-in `default` tokenizer splits on every non-alphanumeric
//! character, so `tech_admin` indexes as `tech` + `admin` and a `term`
//! query for the whole value can never match (issue #66). OpenSearch's
//! standard tokenizer follows Unicode word-break rules (UAX #29), under
//! which `_` joins words and `-` splits them, then drops tokens holding
//! no letter or digit and lowercases the rest. This module reproduces
//! that so queries written against OpenSearch return the same documents.

use tantivy::Index;
use tantivy::tokenizer::{LowerCaser, TextAnalyzer, Token, TokenStream, Tokenizer};
use unicode_segmentation::{UWordBoundIndices, UnicodeSegmentation};

/// Name the standard analyzer is registered under on every index.
pub const STANDARD_TOKENIZER: &str = "standard";

/// OpenSearch's `max_token_length` for the standard tokenizer: longer
/// words are split at this many characters.
pub const MAX_TOKEN_LENGTH: usize = 255;

/// UAX #29 word-break tokenizer (the OpenSearch `standard` tokenizer).
#[derive(Clone, Default)]
pub struct StandardTokenizer {
    token: Token,
}

/// Token stream over UAX #29 words; punctuation- and whitespace-only
/// words are skipped, over-long words are split.
pub struct StandardTokenStream<'a> {
    text: &'a str,
    words: UWordBoundIndices<'a>,
    /// Remainder of a word longer than [`MAX_TOKEN_LENGTH`], as byte
    /// offsets into `text`, still to be emitted in chunks.
    long_word: Option<(usize, usize)>,
    token: &'a mut Token,
}

impl Tokenizer for StandardTokenizer {
    type TokenStream<'a> = StandardTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> StandardTokenStream<'a> {
        self.token.reset();
        StandardTokenStream {
            text,
            words: text.split_word_bound_indices(),
            long_word: None,
            token: &mut self.token,
        }
    }
}

impl StandardTokenStream<'_> {
    /// Next (from, to) byte span to emit: the pending chunk of a long
    /// word, else the next word containing a letter or digit.
    fn next_span(&mut self) -> Option<(usize, usize)> {
        if let Some((from, to)) = self.long_word.take() {
            return Some(self.chunk(from, to));
        }
        for (from, word) in self.words.by_ref() {
            if word.chars().any(char::is_alphanumeric) {
                return Some(self.chunk(from, from + word.len()));
            }
        }
        None
    }

    /// Cut the span at [`MAX_TOKEN_LENGTH`] characters, queueing the rest.
    fn chunk(&mut self, from: usize, to: usize) -> (usize, usize) {
        let word = &self.text[from..to];
        match word.char_indices().nth(MAX_TOKEN_LENGTH) {
            Some((cut, _)) => {
                self.long_word = Some((from + cut, to));
                (from, from + cut)
            }
            None => (from, to),
        }
    }
}

impl TokenStream for StandardTokenStream<'_> {
    fn advance(&mut self) -> bool {
        self.token.text.clear();
        self.token.position = self.token.position.wrapping_add(1);
        match self.next_span() {
            Some((from, to)) => {
                self.token.offset_from = from;
                self.token.offset_to = to;
                self.token.text.push_str(&self.text[from..to]);
                true
            }
            None => false,
        }
    }

    fn token(&self) -> &Token {
        self.token
    }

    fn token_mut(&mut self) -> &mut Token {
        self.token
    }
}

/// The standard analyzer: [`StandardTokenizer`] + lowercasing (no stop
/// words, as in OpenSearch's default).
pub fn standard_analyzer() -> TextAnalyzer {
    TextAnalyzer::builder(StandardTokenizer::default())
        .filter(LowerCaser)
        .build()
}

/// Register rSearch's analyzers on an index. Must run on every index
/// handle before it is written to or searched with an analyzed query:
/// Tantivy resolves tokenizers by name from the handle, not from the
/// index files.
pub fn register_tokenizers(index: &Index) {
    index
        .tokenizers()
        .register(STANDARD_TOKENIZER, standard_analyzer());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tokens(text: &str) -> Vec<String> {
        let mut analyzer = standard_analyzer();
        let mut stream = analyzer.token_stream(text);
        let mut out = Vec::new();
        while stream.advance() {
            out.push(stream.token().text.clone());
        }
        out
    }

    #[test]
    fn underscore_joins_hyphen_splits() {
        // The issue #66 values, and the same shapes verified against
        // OpenSearch 3.6.
        assert_eq!(tokens("tech_admin"), ["tech_admin"]);
        assert_eq!(tokens("not printed"), ["not", "printed"]);
        assert_eq!(tokens("Foo-Bar_baz"), ["foo", "bar_baz"]);
        assert_eq!(tokens("Hello, happy tax payer!"), ["hello", "happy", "tax", "payer"]);
    }

    #[test]
    fn keeps_numbers_and_word_internal_punctuation() {
        assert_eq!(tokens("3.14 v1.2.3 o'neil a.b"), ["3.14", "v1.2.3", "o'neil", "a.b"]);
        assert_eq!(tokens("user@example.com"), ["user", "example.com"]);
        assert_eq!(tokens("  --- ... "), Vec::<String>::new());
        assert_eq!(tokens("日本語"), ["日", "本", "語"]);
    }

    #[test]
    fn positions_and_offsets_follow_emitted_tokens_only() {
        let mut analyzer = standard_analyzer();
        let mut stream = analyzer.token_stream("a, b");
        assert!(stream.advance());
        assert_eq!((stream.token().position, stream.token().offset_from, stream.token().offset_to), (0, 0, 1));
        assert!(stream.advance());
        assert_eq!((stream.token().position, stream.token().offset_from, stream.token().offset_to), (1, 3, 4));
        assert!(!stream.advance());
    }

    #[test]
    fn splits_words_over_max_token_length() {
        let long = "a".repeat(MAX_TOKEN_LENGTH + 2);
        let got = tokens(&long);
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].len(), MAX_TOKEN_LENGTH);
        assert_eq!(got[1], "aa");
        // Character count, not bytes.
        let long = "é".repeat(MAX_TOKEN_LENGTH);
        assert_eq!(tokens(&long), [long.clone()]);
    }
}
