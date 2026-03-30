from __future__ import annotations

import argparse
import ast
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ExportRequirement:
    module_name: str
    kind: str


@dataclass(frozen=True)
class ExportError:
    package_dir: Path
    message: str


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Require explicit package exports in __init__.py for public top-level symbols."
    )
    parser.add_argument(
        "package_dirs",
        nargs="+",
        type=Path,
        help="Package directories to validate, for example src/rawr_analytics/metrics/wowy",
    )
    parser.add_argument(
        "--function-module",
        action="append",
        default=["api"],
        help="Module whose public top-level functions must be re-exported by __init__.py",
    )
    parser.add_argument(
        "--class-module",
        action="append",
        default=["models"],
        help="Module whose public top-level classes must be re-exported by __init__.py",
    )
    return parser


def _main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    requirements = [
        *[
            ExportRequirement(module_name=module_name, kind="function")
            for module_name in args.function_module
        ],
        *[
            ExportRequirement(module_name=module_name, kind="class")
            for module_name in args.class_module
        ],
    ]

    errors: list[ExportError] = []
    for package_dir in args.package_dirs:
        errors.extend(_check_package_exports(package_dir=package_dir, requirements=requirements))

    if not errors:
        print("package export checks passed")
        return 0

    for error in errors:
        print(f"{error.package_dir}: {error.message}")
    return 1


def _check_package_exports(
    *,
    package_dir: Path,
    requirements: list[ExportRequirement],
) -> list[ExportError]:
    errors: list[ExportError] = []
    if not package_dir.is_dir():
        return [ExportError(package_dir=package_dir, message="package directory not found")]

    init_path = package_dir / "__init__.py"
    if not init_path.is_file():
        return [ExportError(package_dir=package_dir, message="missing __init__.py")]

    init_tree = _parse_python_file(init_path)
    imported_names = _collect_imported_names(init_tree)
    exported_names = _collect_all_names(init_tree)
    if exported_names is None:
        errors.append(
            ExportError(
                package_dir=package_dir,
                message="__init__.py must define __all__ as a literal list/tuple/set of strings",
            )
        )
        return errors

    for requirement in requirements:
        module_path = package_dir / f"{requirement.module_name}.py"
        if not module_path.is_file():
            errors.append(
                ExportError(
                    package_dir=package_dir,
                    message=f"missing {module_path.name}",
                )
            )
            continue
        public_names = _collect_public_names(module_path, kind=requirement.kind)
        for public_name in public_names:
            if public_name not in imported_names:
                errors.append(
                    ExportError(
                        package_dir=package_dir,
                        message=(
                            f"{public_name!r} from {module_path.name} is public but not explicitly "
                            "imported in __init__.py"
                        ),
                    )
                )
            if public_name not in exported_names:
                errors.append(
                    ExportError(
                        package_dir=package_dir,
                        message=(
                            f"{public_name!r} from {module_path.name} is public but missing from "
                            "__all__ in __init__.py"
                        ),
                    )
                )

    return errors


def _parse_python_file(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _collect_imported_names(tree: ast.Module) -> set[str]:
    names: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.ImportFrom):
            for alias in node.names:
                if alias.name == "*":
                    continue
                names.add(alias.asname or alias.name)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                bound_name = alias.asname or alias.name.split(".")[-1]
                names.add(bound_name)
    return names


def _collect_all_names(tree: ast.Module) -> set[str] | None:
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if isinstance(target, ast.Name) and target.id == "__all__":
                return _string_collection_literal(node.value)
    return None


def _collect_public_names(module_path: Path, *, kind: str) -> list[str]:
    tree = _parse_python_file(module_path)
    public_names: list[str] = []
    for node in tree.body:
        if kind == "function" and isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            if not node.name.startswith("_"):
                public_names.append(node.name)
        elif kind == "class" and isinstance(node, ast.ClassDef):
            if not node.name.startswith("_"):
                public_names.append(node.name)
    return public_names


def _string_collection_literal(node: ast.AST) -> set[str] | None:
    if not isinstance(node, ast.List | ast.Tuple | ast.Set):
        return None
    values: set[str] = set()
    for item in node.elts:
        if not isinstance(item, ast.Constant) or not isinstance(item.value, str):
            return None
        values.add(item.value)
    return values


if __name__ == "__main__":
    raise SystemExit(_main())
