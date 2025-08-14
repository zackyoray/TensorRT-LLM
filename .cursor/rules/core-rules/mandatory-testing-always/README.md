# Mandatory Testing Always

Documentation and rules for mandatory testing always.

## Overview

This rule enforces mandatory unit testing for all generated code across all programming languages. It ensures that every function, method, or class is accompanied by comprehensive tests covering happy paths, edge cases, and error conditions. The goal is to maintain a minimum 80% code coverage and promote test-driven development practices. This rule applies to all code generation tasks and requires tests to be created in the same response as the implementation.

## MDC Rules

| Rule File | Description |
|-----------|-------------|
| [mandatory-testing-always.mdc](./mandatory-testing-always.mdc) | Enforces unit testing for all generated code across all languages and contexts |

## Usage

These MDC rules are automatically applied by Cursor when working with matching file types.

To use these rules:
1. Ensure the MDC rules are in your project
2. The rules will apply based on their glob patterns
3. Check individual `.mdc` files for specific patterns and behaviors

## Navigation

[← Back to 00-core-rules](../README.md)
