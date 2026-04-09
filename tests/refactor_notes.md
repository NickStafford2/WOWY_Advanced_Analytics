# Background
Most of my tests are hopelessly out of date. You will find tests referencing code from before the rewrite. 
I am in the process of deciding what to do with each.

# Philosophy
Tests should not from refactoring or restructuring. 
I want good test coverage.


## Testing Rules
1. Test **public behavior**, not internal implementation
   * Do not depend on private functions, call order, or structure
2. Prefer **input → output → observable effects**
   * Assert results, state changes, or outputs—not intermediate steps
3. Separate test types
   * unit: isolated, fast, no real dependencies
   * integration: components working together
   * e2e: full system via external interface
4. **Mock only external boundaries**
* Do not mock internal modules unnecessarily
5. Keep tests **refactor-safe**
   * Tests must pass if implementation changes but behavior does not
6. Avoid **overspecification**
   * Do not assert exact strings or formatting unless required
7. Assume **validated inputs inside the system**
   * Do not duplicate validation tests across layers
8. Mirror project structure and keep tests focused
   * One behavior per test

## Core Principle

Test contracts and observable behavior. Ignore implementation details.

# Notes
Don't waste a ton of tokens.
Don't simply rewrite the old tests. Think logically and decide what should be tested. Consider boundaries. 
Look out for code smells and fix them when writing new tests. 

# Current Status
I renamed a bunch of tests and moved them to approximately where i think they should go.
old tests are written as old_test_something.py

# What you must do
Select one old test file.
Read it.
Decide if it is worth keeping. If it is not, stop execution and tell me to delete it. 
If it is worth keeping, tell me what i should do with it. 

