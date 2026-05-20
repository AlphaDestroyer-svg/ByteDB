use super::token::Token;
use crate::error::{QueryError, Result};

pub struct Lexer {
    input: Vec<char>,
    pos: usize,
}

impl Lexer {
    pub fn new(input: &str) -> Self {
        Lexer {
            input: input.chars().collect(),
            pos: 0,
        }
    }

    pub fn tokenize(&mut self) -> Result<Vec<Token>> {
        let mut tokens = Vec::new();
        loop {
            let tok = self.next_token()?;
            if tok == Token::Eof {
                tokens.push(tok);
                break;
            }
            tokens.push(tok);
        }
        Ok(tokens)
    }

    fn next_token(&mut self) -> Result<Token> {
        self.skip_whitespace();

        if self.pos >= self.input.len() {
            return Ok(Token::Eof);
        }

        let ch = self.input[self.pos];

        // Comments
        if ch == '-' && self.peek_next() == Some('-') {
            self.skip_line_comment();
            return self.next_token();
        }

        match ch {
            '(' => { self.pos += 1; Ok(Token::LParen) }
            ')' => { self.pos += 1; Ok(Token::RParen) }
            '{' => { self.pos += 1; Ok(Token::LBrace) }
            '}' => { self.pos += 1; Ok(Token::RBrace) }
            '[' => { self.pos += 1; Ok(Token::LBracket) }
            ']' => { self.pos += 1; Ok(Token::RBracket) }
            ',' => { self.pos += 1; Ok(Token::Comma) }
            ';' => { self.pos += 1; Ok(Token::Semicolon) }
            ':' => {
                self.pos += 1;
                if self.pos < self.input.len() && self.input[self.pos] == ':' {
                    self.pos += 1;
                    Ok(Token::DoubleColon)
                } else {
                    Ok(Token::Colon)
                }
            }
            '+' => { self.pos += 1; Ok(Token::Plus) }
            '-' => { self.pos += 1; Ok(Token::Minus) }
            '*' => { self.pos += 1; Ok(Token::Star) }
            '/' => { self.pos += 1; Ok(Token::Slash) }
            '%' => { self.pos += 1; Ok(Token::Percent) }
            '.' => { self.pos += 1; Ok(Token::Dot) }
            '$' => {
                self.pos += 1;
                if self.pos < self.input.len() && self.input[self.pos] == '.' {
                    self.pos += 1;
                    Ok(Token::DollarDot)
                } else {
                    Err(QueryError::Parse("Expected '.' after '$'".into()))
                }
            }
            '=' => {
                self.pos += 1;
                if self.pos < self.input.len() && self.input[self.pos] == '=' {
                    self.pos += 1;
                }
                Ok(Token::Eq)
            }
            '!' => {
                self.pos += 1;
                if self.pos < self.input.len() && self.input[self.pos] == '=' {
                    self.pos += 1;
                    Ok(Token::Neq)
                } else {
                    Err(QueryError::Parse("Expected '=' after '!'".into()))
                }
            }
            '<' => {
                self.pos += 1;
                if self.pos < self.input.len() {
                    match self.input[self.pos] {
                        '=' => { self.pos += 1; Ok(Token::Lte) }
                        '>' => { self.pos += 1; Ok(Token::Neq) }
                        _ => Ok(Token::Lt)
                    }
                } else {
                    Ok(Token::Lt)
                }
            }
            '>' => {
                self.pos += 1;
                if self.pos < self.input.len() && self.input[self.pos] == '=' {
                    self.pos += 1;
                    Ok(Token::Gte)
                } else {
                    Ok(Token::Gt)
                }
            }
            '"' | '\'' => self.read_string(ch),
            c if c.is_ascii_digit() => self.read_number(),
            c if c.is_alphabetic() || c == '_' => self.read_identifier(),
            _ => {
                self.pos += 1;
                Err(QueryError::Parse(format!("Unexpected character: '{}'", ch)))
            }
        }
    }

    fn read_string(&mut self, quote: char) -> Result<Token> {
        self.pos += 1;
        let mut s = String::new();
        while self.pos < self.input.len() && self.input[self.pos] != quote {
            if self.input[self.pos] == '\\' {
                self.pos += 1;
                if self.pos < self.input.len() {
                    match self.input[self.pos] {
                        'n' => s.push('\n'),
                        't' => s.push('\t'),
                        '\\' => s.push('\\'),
                        c => { s.push('\\'); s.push(c); }
                    }
                }
            } else {
                s.push(self.input[self.pos]);
            }
            self.pos += 1;
        }
        if self.pos >= self.input.len() {
            return Err(QueryError::Parse("Unterminated string".into()));
        }
        self.pos += 1;
        Ok(Token::StringLit(s))
    }

    fn read_number(&mut self) -> Result<Token> {
        let start = self.pos;
        let mut is_float = false;

        while self.pos < self.input.len() && (self.input[self.pos].is_ascii_digit() || self.input[self.pos] == '.') {
            if self.input[self.pos] == '.' {
                if is_float {
                    break;
                }
                is_float = true;
            }
            self.pos += 1;
        }

        let num_str: String = self.input[start..self.pos].iter().collect();
        if is_float {
            let val: f64 = num_str.parse()
                .map_err(|_| QueryError::Parse(format!("Invalid float: {}", num_str)))?;
            Ok(Token::FloatLit(val))
        } else {
            let val: i64 = num_str.parse()
                .map_err(|_| QueryError::Parse(format!("Invalid integer: {}", num_str)))?;
            Ok(Token::IntLit(val))
        }
    }

    fn read_identifier(&mut self) -> Result<Token> {
        let start = self.pos;
        while self.pos < self.input.len() && (self.input[self.pos].is_alphanumeric() || self.input[self.pos] == '_') {
            self.pos += 1;
        }
        let ident: String = self.input[start..self.pos].iter().collect();

        if let Some(keyword) = Token::is_keyword(&ident) {
            Ok(keyword)
        } else {
            Ok(Token::Ident(ident))
        }
    }

    fn skip_whitespace(&mut self) {
        while self.pos < self.input.len() && self.input[self.pos].is_whitespace() {
            self.pos += 1;
        }
    }

    fn skip_line_comment(&mut self) {
        while self.pos < self.input.len() && self.input[self.pos] != '\n' {
            self.pos += 1;
        }
    }

    fn peek_next(&self) -> Option<char> {
        if self.pos + 1 < self.input.len() {
            Some(self.input[self.pos + 1])
        } else {
            None
        }
    }
}
