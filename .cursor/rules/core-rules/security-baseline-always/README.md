# Security Baseline Always

Documentation and rules for security baseline always.

## Overview

This rule establishes mandatory security standards that must be applied to all generated code and configurations. It covers secure coding practices, input validation, authentication, authorization, encryption, secret management, and container security contexts. The rule ensures compliance with OWASP guidelines and implements defense-in-depth principles across all layers of the application stack. These security controls are non-negotiable and apply to every piece of code, regardless of language or platform.

## MDC Rules

| Rule File | Description |
|-----------|-------------|
| [security-baseline-always.mdc](./security-baseline-always.mdc) | Enforces security best practices for all code generation and development activities |

## Usage

These MDC rules are automatically applied by Cursor when working with matching file types.

To use these rules:
1. Ensure the MDC rules are in your project
2. The rules will apply based on their glob patterns
3. Check individual `.mdc` files for specific patterns and behaviors

## Navigation

[← Back to 00-core-rules](../README.md)
