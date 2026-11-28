namespace Glacier.Cluster.Polaris;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using Glacier.Polaris;

/// <summary>
/// Tokenizer and recursive-descent predicate compiler parsing SQL-style filter expressions
/// (including compound <c>AND</c>/<c>OR</c>/<c>NOT</c>, parentheses, <c>IS [NOT] NULL</c>,
/// and quoted/escaped string literals) into strongly-typed Glacier.Polaris <see cref="Expr"/> instances.
/// </summary>
public static class PredicateCompiler
{
    public static Expr Compile(string expression)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(expression);

        var tokens = Tokenize(expression);
        if (tokens.Count == 0)
        {
            throw new FormatException($"Empty predicate expression: '{expression}'");
        }

        var parser = new Parser(tokens, expression);
        AstNode ast = parser.ParseOr();
        if (!parser.IsAtEnd)
        {
            throw new FormatException($"Unexpected trailing token '{parser.Peek().Text}' in predicate: '{expression}'");
        }

        return EvaluateAst(ast);
    }

    /// <summary>
    /// Splits a fused operator pipeline string on <paramref name="delimiter"/> while ignoring
    /// delimiter characters enclosed inside single- or double-quoted string literals.
    /// </summary>
    public static IReadOnlyList<string> SplitPipelinePayload(string payload, char delimiter = '|')
    {
        if (string.IsNullOrEmpty(payload))
        {
            return Array.Empty<string>();
        }

        var segments = new List<string>();
        var current = new StringBuilder();
        char activeQuote = '\0';

        for (int i = 0; i < payload.Length; i++)
        {
            char c = payload[i];

            if (activeQuote != '\0')
            {
                current.Append(c);
                if (c == '\\' && i + 1 < payload.Length)
                {
                    current.Append(payload[++i]);
                }
                else if (c == activeQuote)
                {
                    if (i + 1 < payload.Length && payload[i + 1] == activeQuote)
                    {
                        current.Append(payload[++i]);
                    }
                    else
                    {
                        activeQuote = '\0';
                    }
                }
                continue;
            }

            if (c == '\'' || c == '"')
            {
                activeQuote = c;
                current.Append(c);
            }
            else if (c == delimiter)
            {
                segments.Add(current.ToString());
                current.Clear();
            }
            else
            {
                current.Append(c);
            }
        }

        segments.Add(current.ToString());
        return segments;
    }

    private enum TokenKind
    {
        Identifier,
        StringLiteral,
        NumberLiteral,
        Operator,
        LeftParen,
        RightParen
    }

    private readonly record struct Token(TokenKind Kind, string Text);

    private static List<Token> Tokenize(string expression)
    {
        var tokens = new List<Token>();
        int i = 0;
        int len = expression.Length;

        while (i < len)
        {
            char c = expression[i];
            if (char.IsWhiteSpace(c))
            {
                i++;
                continue;
            }

            if (c == '(')
            {
                tokens.Add(new Token(TokenKind.LeftParen, "("));
                i++;
                continue;
            }

            if (c == ')')
            {
                tokens.Add(new Token(TokenKind.RightParen, ")"));
                i++;
                continue;
            }

            if (c == '\'' || c == '"')
            {
                char quote = c;
                i++;
                var sb = new StringBuilder();
                bool closed = false;

                while (i < len)
                {
                    char ch = expression[i];
                    if (ch == '\\')
                    {
                        if (i + 1 >= len)
                        {
                            throw new FormatException($"Unterminated escape sequence in string literal: '{expression}'");
                        }
                        char escaped = expression[i + 1];
                        sb.Append(escaped switch
                        {
                            '\\' => '\\',
                            '\'' => '\'',
                            '"' => '"',
                            'n' => '\n',
                            'r' => '\r',
                            't' => '\t',
                            _ => escaped
                        });
                        i += 2;
                        continue;
                    }

                    if (ch == quote)
                    {
                        if (i + 1 < len && expression[i + 1] == quote)
                        {
                            sb.Append(quote);
                            i += 2;
                            continue;
                        }

                        i++;
                        closed = true;
                        break;
                    }

                    sb.Append(ch);
                    i++;
                }

                if (!closed)
                {
                    throw new FormatException($"Unclosed string literal in predicate: '{expression}'");
                }

                tokens.Add(new Token(TokenKind.StringLiteral, sb.ToString()));
                continue;
            }

            // Operator runs: consume contiguous operator symbols to cleanly reject ===, <=>, ~=, :=, etc.
            if (IsOperatorChar(c))
            {
                int start = i;
                while (i < len && IsOperatorChar(expression[i]))
                {
                    i++;
                }

                string opText = expression.Substring(start, i - start);
                if (opText is not (">=" or "<=" or "==" or "!=" or ">" or "<" or "="))
                {
                    throw new FormatException($"Unsupported operator '{opText}' in predicate: '{expression}'");
                }

                tokens.Add(new Token(TokenKind.Operator, opText == "=" ? "==" : opText));
                continue;
            }

            // Numeric literal (including optional leading '-' when followed by digit)
            if (char.IsDigit(c) || (c == '-' && i + 1 < len && (char.IsDigit(expression[i + 1]) || expression[i + 1] == '.')))
            {
                int start = i;
                if (c == '-') i++;
                bool seenDot = false;
                while (i < len && (char.IsDigit(expression[i]) || (!seenDot && expression[i] == '.')))
                {
                    if (expression[i] == '.') seenDot = true;
                    i++;
                }
                if (i < len && (expression[i] == 'e' || expression[i] == 'E'))
                {
                    int expPos = i + 1;
                    if (expPos < len && (expression[expPos] == '+' || expression[expPos] == '-')) expPos++;
                    if (expPos < len && char.IsDigit(expression[expPos]))
                    {
                        i = expPos;
                        while (i < len && char.IsDigit(expression[i])) i++;
                    }
                }

                // Trailing quote or invalid identifier character directly glued to number
                if (i < len && (expression[i] == '\'' || expression[i] == '"'))
                {
                    throw new FormatException($"Unexpected quote in numeric literal in predicate: '{expression}'");
                }

                tokens.Add(new Token(TokenKind.NumberLiteral, expression.Substring(start, i - start)));
                continue;
            }

            // Identifier / keyword
            if (IsIdentifierStart(c))
            {
                int start = i;
                while (i < len && IsIdentifierPart(expression[i]))
                {
                    i++;
                }

                // Reject malformed unclosed trailing quote attached to identifier (e.g., unclosed')
                if (i < len && (expression[i] == '\'' || expression[i] == '"'))
                {
                    throw new FormatException($"Unexpected trailing quote in identifier in predicate: '{expression}'");
                }

                tokens.Add(new Token(TokenKind.Identifier, expression.Substring(start, i - start)));
                continue;
            }

            throw new FormatException($"Unexpected character '{c}' in predicate: '{expression}'");
        }

        return tokens;
    }

    private static bool IsOperatorChar(char c) =>
        c is '>' or '<' or '=' or '!' or '~' or ':' or '?';

    private static bool IsIdentifierStart(char c) =>
        char.IsLetter(c) || c == '_';

    private static bool IsIdentifierPart(char c) =>
        char.IsLetterOrDigit(c) || c is '_' or '.' or '$';

    private abstract record AstNode;
    private sealed record BinaryLogicalNode(bool IsAnd, AstNode Left, AstNode Right) : AstNode;
    private sealed record NullCheckNode(string Column, bool IsNotNull) : AstNode;
    private sealed record ComparisonNode(string Column, string Operator, Token ValueToken) : AstNode;

    private sealed class Parser
    {
        private readonly List<Token> _tokens;
        private readonly string _original;
        private int _pos;

        public Parser(List<Token> tokens, string original)
        {
            _tokens = tokens;
            _original = original;
        }

        public bool IsAtEnd => _pos >= _tokens.Count;
        public Token Peek() => _tokens[_pos];

        public AstNode ParseOr()
        {
            AstNode left = ParseAnd();
            while (!IsAtEnd && MatchKeyword("OR"))
            {
                AstNode right = ParseAnd();
                left = new BinaryLogicalNode(IsAnd: false, left, right);
            }
            return left;
        }

        private AstNode ParseAnd()
        {
            AstNode left = ParseNot();
            while (!IsAtEnd && MatchKeyword("AND"))
            {
                AstNode right = ParseNot();
                left = new BinaryLogicalNode(IsAnd: true, left, right);
            }
            return left;
        }

        private AstNode ParseNot()
        {
            if (!IsAtEnd && MatchKeyword("NOT"))
            {
                AstNode inner = ParseNot();
                return Negate(inner);
            }

            return ParsePrimary();
        }

        private AstNode ParsePrimary()
        {
            if (IsAtEnd)
            {
                throw new FormatException($"Unexpected end of predicate expression: '{_original}'");
            }

            Token tok = Peek();
            if (tok.Kind == TokenKind.LeftParen)
            {
                _pos++;
                AstNode inner = ParseOr();
                if (IsAtEnd || Peek().Kind != TokenKind.RightParen)
                {
                    throw new FormatException($"Missing closing ')' in predicate: '{_original}'");
                }
                _pos++;
                return inner;
            }

            if (tok.Kind != TokenKind.Identifier)
            {
                throw new FormatException($"Predicate missing column identifier: '{_original}'");
            }

            string colName = tok.Text;
            _pos++;

            if (IsAtEnd)
            {
                throw new FormatException($"Unsupported or missing comparison operator in predicate: '{_original}'");
            }

            if (MatchKeyword("IS"))
            {
                bool isNotNull = MatchKeyword("NOT");
                if (!MatchKeyword("NULL"))
                {
                    throw new FormatException($"Expected NULL after IS{(isNotNull ? " NOT" : string.Empty)} in predicate: '{_original}'");
                }
                return new NullCheckNode(colName, isNotNull);
            }

            Token opTok = Peek();
            if (opTok.Kind != TokenKind.Operator)
            {
                throw new FormatException($"Unsupported or missing comparison operator in predicate: '{_original}'");
            }
            _pos++;

            if (IsAtEnd)
            {
                throw new FormatException($"Predicate missing value literal in predicate: '{_original}'");
            }

            Token valTok = Peek();
            if (valTok.Kind is not (TokenKind.StringLiteral or TokenKind.NumberLiteral or TokenKind.Identifier))
            {
                throw new FormatException($"Unable to parse value literal '{valTok.Text}' in predicate: '{_original}'");
            }
            _pos++;

            return new ComparisonNode(colName, opTok.Text, valTok);
        }

        private bool MatchKeyword(string keyword)
        {
            if (!IsAtEnd && _tokens[_pos].Kind == TokenKind.Identifier &&
                _tokens[_pos].Text.Equals(keyword, StringComparison.OrdinalIgnoreCase))
            {
                _pos++;
                return true;
            }
            return false;
        }
    }

    private static AstNode Negate(AstNode node) => node switch
    {
        BinaryLogicalNode bin => new BinaryLogicalNode(!bin.IsAnd, Negate(bin.Left), Negate(bin.Right)),
        NullCheckNode nc => new NullCheckNode(nc.Column, !nc.IsNotNull),
        ComparisonNode cmp => new ComparisonNode(cmp.Column, cmp.Operator switch
        {
            "==" => "!=",
            "!=" => "==",
            ">" => "<=",
            "<=" => ">",
            "<" => ">=",
            ">=" => "<",
            _ => throw new InvalidOperationException($"Cannot negate operator '{cmp.Operator}'.")
        }, cmp.ValueToken),
        _ => throw new InvalidOperationException("Unsupported AST node in NOT expression.")
    };

    private static Expr EvaluateAst(AstNode node)
    {
        switch (node)
        {
            case BinaryLogicalNode bin:
                return bin.IsAnd
                    ? EvaluateAst(bin.Left) & EvaluateAst(bin.Right)
                    : EvaluateAst(bin.Left) | EvaluateAst(bin.Right);

            case NullCheckNode nc:
            {
                var colExpr = Expr.Col(nc.Column);
                return nc.IsNotNull ? colExpr.IsNotNull() : colExpr.IsNull();
            }

            case ComparisonNode cmp:
                return CompileComparison(cmp.Column, cmp.Operator, cmp.ValueToken);

            default:
                throw new InvalidOperationException("Unsupported predicate AST node.");
        }
    }

    private static Expr CompileComparison(string colName, string op, Token valTok)
    {
        var colExpr = Expr.Col(colName);

        if (valTok.Kind == TokenKind.StringLiteral)
        {
            var litExpr = Expr.Lit(valTok.Text);
            return op switch
            {
                "==" => colExpr == litExpr,
                "!=" => colExpr != litExpr,
                _ => throw new NotSupportedException($"Operator '{op}' is not supported for string comparisons.")
            };
        }

        if (valTok.Kind == TokenKind.Identifier)
        {
            if (bool.TryParse(valTok.Text, out bool boolVal))
            {
                var litExpr = Expr.Lit(boolVal);
                return op switch
                {
                    "==" => colExpr == litExpr,
                    "!=" => colExpr != litExpr,
                    _ => throw new NotSupportedException($"Operator '{op}' is not supported for boolean comparisons.")
                };
            }

            throw new FormatException($"Unable to parse value literal '{valTok.Text}' in predicate.");
        }

        // Numeric literal: 32-bit int first for native Polaris series type matching, then 64-bit long, then double.
        if (int.TryParse(valTok.Text, NumberStyles.Integer, CultureInfo.InvariantCulture, out int intVal))
        {
            var litExpr = Expr.Lit(intVal);
            return ApplyNumericOp(colExpr, litExpr, op);
        }

        if (long.TryParse(valTok.Text, NumberStyles.Integer, CultureInfo.InvariantCulture, out long longVal))
        {
            var litExpr = Expr.Lit(longVal);
            return ApplyNumericOp(colExpr, litExpr, op);
        }

        if (double.TryParse(valTok.Text, NumberStyles.Float, CultureInfo.InvariantCulture, out double doubleVal))
        {
            var litExpr = Expr.Lit(doubleVal);
            return ApplyNumericOp(colExpr, litExpr, op);
        }

        throw new FormatException($"Unable to parse value literal '{valTok.Text}' in predicate.");
    }

    private static Expr ApplyNumericOp(Expr colExpr, Expr litExpr, string op) => op switch
    {
        ">" => colExpr > litExpr,
        "<" => colExpr < litExpr,
        ">=" => colExpr >= litExpr,
        "<=" => colExpr <= litExpr,
        "==" => colExpr == litExpr,
        "!=" => colExpr != litExpr,
        _ => throw new InvalidOperationException($"Unsupported numeric operator '{op}'.")
    };
}
