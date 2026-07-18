You are working on a **full Rust rewrite** of OrioleDB extension to rust using pgrx.

## Repository Structure
* Original C implementation:

  * `src/`
  * `include/`

* Rust implementation:
  * `orioledb-rs/`
The Rust implementation is **not** intended to be a thin wrapper around the C implementation.
The goal is to completely replace the C code with idiomatic Rust.
---
# Critical Rule
**Do NOT create wrappers around the C implementation.**

This means:

* Do NOT use `extern "C"` simply to call the existing C implementation.
* Do NOT leave placeholder implementations.
* Do NOT generate shim layers.
* Do NOT keep logic inside C because it is "easier."
Every algorithm, data structure, state machine, parser, serializer, iterator, and subsystem must be implemented in Rust.
The C code is only the specification.
---

# Current Problem
Many files currently contain things like:
* extern C wrappers
* empty implementations
* TODO stubs
* placeholder functions
* unsafe wrappers calling C
* or files or implementations missing

This is **not acceptable**.
Replace them with actual Rust implementations that faithfully reproduce the original C behavior.

---

# Migration Order

Complete work in the following order.

## Phase 1 — Cleanup
Before porting additional code, clean up the Rust project.
For every file inside `orioledb-rs`:
### 1. Convert comments
Replace C-style comments with idiomatic Rust documentation.
Convert:
* block comments
* inline comments
* function descriptions
* module descriptions

into proper Rust style:

* `///`
* `//!`
* normal `//`

Write comments naturally in Rust instead of mechanically copying C comments.

---

### 2. Fix Rust Analyzer warnings
Resolve all warnings including:

* non_snake_case
* non_camel_case_types
* upper_case_globals where appropriate
* unused imports
* dead code (unless intentionally unfinished)
* unused variables
* unnecessary mutability
* needless clones
* needless returns
* unreachable code

The project should move steadily toward a warning-free state.

---

### 3. Reduce unsafe

Current code contains excessive `unsafe`.

Refactor so that:

* unsafe is isolated
* safe APIs wrap unsafe internals
* raw pointers become references where possible
* pointer arithmetic becomes slices/iterators
* ownership replaces manual memory management
* lifetimes replace manual tracking

Unsafe should exist only where absolutely required.

---

# Phase 2 — Port the Engine

Port one module at a time.

Complete an entire module before moving on.

Port in this order:

0. orioledb.c → `lib.rs`
1. btree
2. catalog
3. checkpoint
4. rewind
5. s3
6. transam
7. utils
8. other modules...
finally compile or test

---

# File-by-File Process

For every file:

1. Read the corresponding C implementation.
2. Understand the algorithm completely.
3. Implement it in Rust.
4. Remove any shim or wrapper.
5. Remove unnecessary extern declarations.
6. Replace C memory management with Rust ownership.
7. Replace macros with Rust equivalents.
8. Replace structs with idiomatic Rust structs.
9. Replace enums with Rust enums.
10. Replace linked lists, arrays, and hash structures with appropriate Rust collections where doing so preserves semantics.
11. Keep behavior identical.

Do not skip files.

Do not partially port files.

Finish each file before proceeding.

---

# Porting Requirements

The Rust implementation should embrace Rust rather than imitate C.

Prefer:

* ownership
* borrowing
* RAII
* Result
* Option
* iterators
* slices
* traits
* enums
* pattern matching
* const generics where appropriate

Avoid:

* global mutable state
* raw pointers
* manual allocation
* C naming conventions
* C-style casts
* memcpy-style programming when safe Rust can express the same behavior

---

# Behaviour Preservation

Maintain identical behavior to the C implementation.

This includes:

* on-disk formats
* WAL compatibility
* page layouts
* B-tree behavior
* transaction semantics
* checkpoint logic
* recovery
* concurrency behavior
* serialization formats

Behavioral compatibility is mandatory.

---

# Quality Expectations

Every completed file should:

* compile
* avoid unnecessary unsafe
* use idiomatic Rust
* include proper Rust documentation
* have no placeholder implementations
* have no TODO stubs
* have no "call into C" implementations
* preserve functionality

---

# Working Strategy

Only work on one file at a time.

For each file:

1. Read C implementation.
2. Understand algorithm.
3. Rewrite in Rust.
4. Compile.
5. Fix warnings.
6. Reduce unsafe.
7. Verify behavior.
8. Move to the next file.

Never perform mass automated conversions without understanding the original implementation.

---

# Success Criteria

The project is complete only when:

* there are no C implementation shims,
* no placeholder implementations remain,
* all target modules are fully rewritten in Rust,
* `extern "C"` exists only where required for PostgreSQL integration or unavoidable FFI boundaries,
* Rust Analyzer warnings are eliminated,
* unsafe code is minimized and encapsulated,
* and the Rust implementation faithfully reproduces the original OrioleDB behavior without depending on the C implementation.
