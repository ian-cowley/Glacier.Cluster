namespace Glacier.Cluster.Polaris;

using System;
using System.Globalization;
using Glacier.Polaris;

/// <summary>
/// Fast, zero-allocation predicate compiler parsing SQL-style filter expressions
/// into strongly-typed Glacier.Polaris <see cref="Expr"/> instances.
/// </summary>
public static class PredicateCompiler
{
    public static Expr Compile(string expression)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(expression);
        var span = expression.AsSpan().Trim();

        // Reject non-SQL comparison operators
        if (span.IndexOf("~=") >= 0 || span.IndexOf(":=") >= 0)
        {
            throw new FormatException($"Unsupported non-SQL comparison operator in predicate: '{expression}'");
        }

        // 1. Binary Comparison Search (>=, <=, ==, !=, >, <, =)
        int opIdx;
        string op;
        int opLen;

        if ((opIdx = span.IndexOf(">=")) >= 0) { op = ">="; opLen = 2; }
        else if ((opIdx = span.IndexOf("<=")) >= 0) { op = "<="; opLen = 2; }
        else if ((opIdx = span.IndexOf("==")) >= 0) { op = "=="; opLen = 2; }
        else if ((opIdx = span.IndexOf("!=")) >= 0) { op = "!="; opLen = 2; }
        else if ((opIdx = span.IndexOf(">")) >= 0) { op = ">"; opLen = 1; }
        else if ((opIdx = span.IndexOf("<")) >= 0) { op = "<"; opLen = 1; }
        else if ((opIdx = span.IndexOf("=")) >= 0) { op = "=="; opLen = 1; }
        else
        {
            throw new FormatException($"Unsupported or missing comparison operator in predicate: '{expression}'");
        }

        var colName = span.Slice(0, opIdx).Trim().ToString();
        if (colName.Length == 0)
        {
            throw new FormatException($"Predicate missing column identifier: '{expression}'");
        }

        var valSpan = span.Slice(opIdx + opLen).Trim();
        if (valSpan.Length == 0)
        {
            throw new FormatException($"Predicate missing value literal in predicate: '{expression}'");
        }

        var colExpr = Expr.Col(colName);

        // String literal ('val' or "val")
        if ((valSpan.StartsWith("'") && valSpan.EndsWith("'")) ||
            (valSpan.StartsWith("\"") && valSpan.EndsWith("\"")))
        {
            string literal = valSpan.Slice(1, valSpan.Length - 2).ToString();
            var litExpr = Expr.Lit(literal);

            return op switch
            {
                "==" => colExpr == litExpr,
                "!=" => colExpr != litExpr,
                _ => throw new NotSupportedException($"Operator '{op}' is not supported for string comparisons.")
            };
        }

        // Boolean literal (true / false)
        if (bool.TryParse(valSpan, out bool boolVal))
        {
            var litExpr = Expr.Lit(boolVal);
            return op switch
            {
                "==" => colExpr == litExpr,
                "!=" => colExpr != litExpr,
                _ => throw new NotSupportedException($"Operator '{op}' is not supported for boolean comparisons.")
            };
        }

        // Integer literal (try 32-bit int first for native Polaris series type matching, then 64-bit long)
        if (int.TryParse(valSpan, NumberStyles.Integer, CultureInfo.InvariantCulture, out int intVal))
        {
            var litExpr = Expr.Lit(intVal);
            return op switch
            {
                ">" => colExpr > litExpr,
                "<" => colExpr < litExpr,
                ">=" => colExpr >= litExpr,
                "<=" => colExpr <= litExpr,
                "==" => colExpr == litExpr,
                "!=" => colExpr != litExpr,
                _ => throw new InvalidOperationException()
            };
        }

        if (long.TryParse(valSpan, NumberStyles.Integer, CultureInfo.InvariantCulture, out long longVal))
        {
            var litExpr = Expr.Lit(longVal);
            return op switch
            {
                ">" => colExpr > litExpr,
                "<" => colExpr < litExpr,
                ">=" => colExpr >= litExpr,
                "<=" => colExpr <= litExpr,
                "==" => colExpr == litExpr,
                "!=" => colExpr != litExpr,
                _ => throw new InvalidOperationException()
            };
        }

        // Floating-point literal
        if (double.TryParse(valSpan, NumberStyles.Float, CultureInfo.InvariantCulture, out double doubleVal))
        {
            var litExpr = Expr.Lit(doubleVal);
            return op switch
            {
                ">" => colExpr > litExpr,
                "<" => colExpr < litExpr,
                ">=" => colExpr >= litExpr,
                "<=" => colExpr <= litExpr,
                "==" => colExpr == litExpr,
                "!=" => colExpr != litExpr,
                _ => throw new InvalidOperationException()
            };
        }

        throw new FormatException($"Unable to parse value literal '{valSpan.ToString()}' in predicate.");
    }
}
