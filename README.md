# CascadeStore

[![Java Version](https://img.shields.io/badge/Java-17-orange.svg)](https://openjdk.java.net/projects/jdk/17/)

A Java 17 key-value store built on a Log-Structured Merge-Tree (LSM-tree) design.

## Overview

CascadeStore prioritizes write-heavy traffic by buffering updates in memory, logging them for durability, and periodically flushing sorted tables to disk. Compaction runs in the background to merge those tables and keep read paths predictable.

The layout follows a conventional LSM stack:
- MemTable for live writes
- Write-ahead log (WAL) for recovery after crashes
- SSTables for persistent, sorted storage
- Background compaction to control file count and space

## Requirements

- JDK 17 or later
- Apache Maven 3.6+ installed locally (this repository does not include `mvnw` wrapper scripts)
