from __future__ import annotations

import argparse
import ast
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class BoundaryRule:
    package: str
    public_modules: frozenset[str]


@dataclass(frozen=True)
class BoundaryError:
    path: Path
    line: int
    message: str


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Reject imports of private symbols and imports that bypass declared package public modules."
        )
    )
    parser.add_argument(
        "--rule",
        action="append",
        required=True,
        help=(
            "Rule in the form package:module1,module2 . "
            "Example: rawr_analytics.metrics.wowy:models,validation"
        ),
    )
    parser.add_argument(
        "paths",
        nargs="*",
        type=Path,
        default=[Path("src"), Path("scripts"), Path("tests")],
        help="Directories or files to scan. Defaults to src, scripts, tests.",
    )
    return parser


def _main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    rules = [_parse_rule(raw_rule) for raw_rule in args.rule]
    python_files = _collect_python_files(args.paths)
    errors: list[BoundaryError] = []
    for path in python_files:
        errors.extend(_check_file(path=path, rules=rules))

    if not errors:
        print("package boundary checks passed")
        return 0

    for error in errors:
        print(f"{error.path}:{error.line}: {error.message}")
    return 1


def _parse_rule(raw_rule: str) -> BoundaryRule:
    if ":" not in raw_rule:
        raise ValueError(f"Invalid rule {raw_rule!r}; expected package:module1,module2")
    package, raw_modules = raw_rule.split(":", maxsplit=1)
    modules = frozenset(module.strip() for module in raw_modules.split(",") if module.strip())
    if not package or not modules:
        raise ValueError(f"Invalid rule {raw_rule!r}; expected package:module1,module2")
    return BoundaryRule(package=package, public_modules=modules)


def _collect_python_files(paths: list[Path]) -> list[Path]:
    files: list[Path] = []
    for path in paths:
        if path.is_file() and path.suffix == ".py":
            files.append(path)
            continue
        if path.is_dir():
            files.extend(sorted(path.rglob("*.py")))
    return sorted(set(files))


def _check_file(path: Path, *, rules: list[BoundaryRule]) -> list[BoundaryError]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    module_name = _module_name_for_path(path)
    is_package_init = path.name == "__init__.py"

    errors: list[BoundaryError] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            errors.extend(_check_import_node(path=path, node=node, rules=rules))
        elif isinstance(node, ast.ImportFrom):
            errors.extend(
                _check_import_from_node(
                    path=path,
                    module_name=module_name,
                    is_package_init=is_package_init,
                    node=node,
                    rules=rules,
                )
            )
    return errors


def _check_import_node(
    *,
    path: Path,
    node: ast.Import,
    rules: list[BoundaryRule],
) -> list[BoundaryError]:
    errors: list[BoundaryError] = []
    for alias in node.names:
        imported_module = alias.name
        if _has_private_segment(imported_module):
            errors.append(
                BoundaryError(
                    path=path,
                    line=node.lineno,
                    message=f"private module import is not allowed: {imported_module}",
                )
            )
        rule = _matching_rule(imported_module, rules)
        if rule is None:
            continue
        if not _is_allowed_module_import(imported_module, rule):
            errors.append(
                BoundaryError(
                    path=path,
                    line=node.lineno,
                    message=(
                        f"import bypasses public boundary for {rule.package}: {imported_module}. "
                        f"Allowed imports are {_format_allowed_imports(rule)}"
                    ),
                )
            )
    return errors


def _check_import_from_node(
    *,
    path: Path,
    module_name: str | None,
    is_package_init: bool,
    node: ast.ImportFrom,
    rules: list[BoundaryRule],
) -> list[BoundaryError]:
    errors: list[BoundaryError] = []
    resolved_module = _resolve_import_from_module(
        module_name=module_name,
        is_package_init=is_package_init,
        module=node.module,
        level=node.level,
    )

    if resolved_module is not None and _has_private_segment(resolved_module):
        errors.append(
            BoundaryError(
                path=path,
                line=node.lineno,
                message=f"private module import is not allowed: {resolved_module}",
            )
        )

    for alias in node.names:
        if alias.name.startswith("_"):
            errors.append(
                BoundaryError(
                    path=path,
                    line=node.lineno,
                    message=f"private symbol import is not allowed: {alias.name}",
                )
            )

        imported_target = _resolve_imported_target(
            resolved_module=resolved_module,
            alias_name=alias.name,
        )
        if imported_target is not None and _has_private_segment(imported_target):
            errors.append(
                BoundaryError(
                    path=path,
                    line=node.lineno,
                    message=f"private module import is not allowed: {imported_target}",
                )
            )

        target_for_rule = resolved_module or imported_target
        if target_for_rule is None:
            continue
        rule = _matching_rule(target_for_rule, rules)
        if rule is None:
            continue
        if not _is_allowed_from_import(
            resolved_module=resolved_module,
            imported_target=imported_target,
            rule=rule,
        ):
            errors.append(
                BoundaryError(
                    path=path,
                    line=node.lineno,
                    message=(
                        f"import bypasses public boundary for {rule.package}: "
                        f"{_format_import_target(resolved_module, alias.name)}. "
                        f"Allowed imports are {_format_allowed_imports(rule)}"
                    ),
                )
            )

    return errors


def _module_name_for_path(path: Path) -> str | None:
    parts = path.parts
    if "src" not in parts:
        return None
    src_index = parts.index("src")
    relative_parts = list(parts[src_index + 1 :])
    if not relative_parts:
        return None
    relative_parts[-1] = Path(relative_parts[-1]).stem
    if relative_parts[-1] == "__init__":
        relative_parts = relative_parts[:-1]
    return ".".join(relative_parts)


def _resolve_import_from_module(
    *,
    module_name: str | None,
    is_package_init: bool,
    module: str | None,
    level: int,
) -> str | None:
    if level == 0:
        return module
    if module_name is None:
        return None

    current_parts = module_name.split(".")
    drop_count = max(level - (1 if is_package_init else 0), 0)
    if drop_count > len(current_parts):
        return module
    base_parts = current_parts[:-drop_count] if drop_count else current_parts
    if module is None:
        return ".".join(base_parts)
    return ".".join([*base_parts, *module.split(".")])


def _resolve_imported_target(
    *,
    resolved_module: str | None,
    alias_name: str,
) -> str | None:
    if resolved_module is None:
        return None
    if alias_name == "*":
        return resolved_module
    return f"{resolved_module}.{alias_name}"


def _matching_rule(imported_module: str, rules: list[BoundaryRule]) -> BoundaryRule | None:
    for rule in rules:
        if imported_module == rule.package or imported_module.startswith(f"{rule.package}."):
            return rule
    return None


def _is_allowed_module_import(imported_module: str, rule: BoundaryRule) -> bool:
    if imported_module == rule.package:
        return True
    for module_name in rule.public_modules:
        if imported_module == f"{rule.package}.{module_name}":
            return True
    return False


def _is_allowed_from_import(
    *,
    resolved_module: str | None,
    imported_target: str | None,
    rule: BoundaryRule,
) -> bool:
    if resolved_module == rule.package:
        return True
    for module_name in rule.public_modules:
        if resolved_module == f"{rule.package}.{module_name}":
            return True
    if imported_target == rule.package:
        return True
    for module_name in rule.public_modules:
        if imported_target == f"{rule.package}.{module_name}":
            return True
    return False


def _has_private_segment(imported_module: str) -> bool:
    return any(segment.startswith("_") for segment in imported_module.split("."))


def _format_import_target(resolved_module: str | None, alias_name: str) -> str:
    if resolved_module is None:
        return alias_name
    return f"{resolved_module}.{alias_name}"


def _format_allowed_imports(rule: BoundaryRule) -> str:
    allowed = [rule.package, *[f"{rule.package}.{name}" for name in sorted(rule.public_modules)]]
    return ", ".join(allowed)


if __name__ == "__main__":
    raise SystemExit(_main())
