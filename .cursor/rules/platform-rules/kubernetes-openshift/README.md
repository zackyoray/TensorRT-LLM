# Kubernetes OpenShift

Documentation and rules for Kubernetes and OpenShift platform operations.

## Overview

This directory contains comprehensive rules and best practices for developing and maintaining applications on Kubernetes and OpenShift platforms. It covers deployment configurations, security contexts, resource management, RBAC, network policies, and platform-specific patterns. The rules ensure applications follow cloud-native principles, implement proper security controls, and optimize for container orchestration environments.

## MDC Rules

| Rule File | Description |
|-----------|-------------|
| [kubernetes.mdc](./kubernetes.mdc) | This rule provides comprehensive best practices for developing and maintaining Kubernetes applications and infrastructure, covering coding standards, security, performance, testing, and deployment. |
| [openshift.mdc](./openshift.mdc) | Openshift best practices and guidelines |

## Usage

These MDC rules are automatically applied by Cursor when working with matching file types.

To use these rules:
1. Ensure the MDC rules are in your project
2. The rules will apply based on their glob patterns
3. Check individual `.mdc` files for specific patterns and behaviors

## Navigation

[← Back to 10-platform-rules](../README.md)
