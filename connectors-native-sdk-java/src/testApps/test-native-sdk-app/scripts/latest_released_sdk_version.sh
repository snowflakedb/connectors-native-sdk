#!/bin/bash
# Copyright (c) 2024 Snowflake Inc.

TAGS=$(git tag | grep "connectors_native_sdk")
VERSIONS=$(echo "$TAGS" | sed -r 's/.*_([0-9]+\.[0-9]+\.[0-9]+)$/\1/')

MAJOR_VERSION=$(echo "$VERSIONS" | cut -d "." -f1 | sort -nr | head -n 1)
MINOR_VERSION=$(echo "$VERSIONS" | grep -o "$MAJOR_VERSION\.[0-9]\+\.[0-9]\+" | cut -d "." -f2 | sort -nr | head -n 1)
PATCH_VERSION=$(echo "$VERSIONS" | grep -o "$MAJOR_VERSION\.$MINOR_VERSION\.[0-9]\+" | cut -d "." -f3 | sort -nr | head -n 1)

echo "$MAJOR_VERSION.$MINOR_VERSION.$PATCH_VERSION"
