# Code Standards Checker Spec

Script name: `scripts/check_code_standards.py`

Purpose:
This program reads the repository and reports code that breaks the project's package, naming, and export rules.

## Rules To Enforce

### Function naming
- A function that is only used inside its own module must start with `_`.
- A function that is imported by another module must not start with `_`.
- `from somewhere import _foo` is always a violation.
- `from _somewhere import foo` is not a violation if it is within the same subpackage. 

### Module naming inside a subpackage
- A module inside a subpackage should start with `_` by default.
- If that module defines any function that is used outside the subpackage, that module must not start with `_`.
- A non-underscored module inside a subpackage is allowed when it owns public cross-subpackage behavior.
- An underscored module is not dead code. It may be imported within its own subpackage.

### Package boundaries
- Private helpers may be shared within the same subpackage.
- Crossing a subpackage boundary should go through the package's declared public surface.
- The checker should support declared public entry modules for each package or subpackage.
- Imports that bypass that declared public surface should be violations.

### Package exports
- A package with a public API should define that API explicitly in `__init__.py`.
- Public package symbols should be explicitly imported in `__init__.py`.
- Public package symbols should be listed in `__all__`.
- The checker should only require exports for symbols intended to be package API, not every public symbol in every file.

### Wrapper detection
- A module that only forwards calls or re-exports other modules without owning meaningful logic should be flagged.
- The checker should not require wrapper modules such as `api.py`.

## What The Program Should Report
- File paths
- Line numbers when relevant
- Clear message describing the violated rule

## Suggested Design
- One CLI entrypoint: `scripts/check_code_standards.py`
- Small internal modules under a dedicated package for:
  - import graph and symbol usage
  - boundary rules
  - export rules
  - wrapper detection
  - reporting

## Important behavior
- The tool should analyze all actual usage across the repo.
- It should determine whether a function is module-private, subpackage-private, or used outside the subpackage.
- It should use that usage to decide whether function and module names follow the rules.


Your goal is to evaluate these rules and inform me if they make sense and conform to reasonable code guidelines. 
