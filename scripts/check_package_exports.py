from __future__ import annotations

import argparse
import ast
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ExportError:
    path: Path
    message: str


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Require explicit package exports in __init__.py for public top-level symbols "
            "defined in public package modules."
        )
    )
    parser.add_argument(
        "package_dirs",
        nargs="+",
        type=Path,
        help="Package directories to validate, for example src/rawr_analytics/metrics/wowy",
    )
    parser.add_argument(
        "--check-functions",
        action="store_true",
        help="Check public top-level functions.",
    )
    parser.add_argument(
        "--check-classes",
        action="store_true",
        help="Check public top-level classes.",
    )
    return parser


def _main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    check_functions = args.check_functions or not args.check_classes
    check_classes = args.check_classes or not args.check_functions

    errors: list[ExportError] = []
    for package_dir in args.package_dirs:
        errors.extend(
            _check_package_exports(
                package_dir=package_dir,
                check_functions=check_functions,
                check_classes=check_classes,
            )
        )

    if not errors:
        print("package export checks passed")
        return 0

    for error in errors:
        print(f"{error.path}: {error.message}")
    return 1


def _check_package_exports(
    *,
    package_dir: Path,
    check_functions: bool,
    check_classes: bool,
) -> list[ExportError]:
    if not package_dir.is_dir():
        return [ExportError(path=package_dir, message="package directory not found")]

    init_path = package_dir / "__init__.py"
    if not init_path.is_file():
        return [ExportError(path=package_dir, message="missing __init__.py")]

    init_tree = _parse_python_file(init_path)
    imported_names = _collect_imported_names(init_tree)
    exported_names = _collect_all_names(init_tree)
    if exported_names is None:
        return [
            ExportError(
                path=init_path,
                message="__init__.py must define __all__ as a literal list/tuple/set of strings",
            )
        ]

    errors: list[ExportError] = []
    for module_path in _collect_package_modules(package_dir):
        public_names = _collect_public_names(
            module_path,
            check_functions=check_functions,
            check_classes=check_classes,
        )
        if not public_names:
            continue

        if _is_private_module(module_path):
            for public_name in public_names:
                errors.append(
                    ExportError(
                        path=module_path,
                        message=(
                            f"private module defines public symbol {public_name!r}; "
                            "public package symbols must live in non-underscored modules"
                        ),
                    )
                )
            continue

        for public_name in public_names:
            if public_name not in imported_names:
                errors.append(
                    ExportError(
                        path=init_path,
                        message=(
                            f"{public_name!r} from {module_path.name} is public but not explicitly "
                            "imported in __init__.py"
                        ),
                    )
                )
            if public_name not in exported_names:
                errors.append(
                    ExportError(
                        path=init_path,
                        message=(
                            f"{public_name!r} from {module_path.name} is public but missing from "
                            "__all__ in __init__.py"
                        ),
                    )
                )

    return errors


def _parse_python_file(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _collect_package_modules(package_dir: Path) -> list[Path]:
    return sorted(
        path
        for path in package_dir.iterdir()
        if path.is_file() and path.suffix == ".py" and path.name != "__init__.py"
    )


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
                names.add(alias.asname or alias.name.split(".")[-1])
    return names


def _collect_all_names(tree: ast.Module) -> set[str] | None:
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if isinstance(target, ast.Name) and target.id == "__all__":
                return _string_collection_literal(node.value)
    return None


def _collect_public_names(
    module_path: Path,
    *,
    check_functions: bool,
    check_classes: bool,
) -> list[str]:
    tree = _parse_python_file(module_path)
    public_names: list[str] = []
    for node in tree.body:
        if check_functions and isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            if not node.name.startswith("_"):
                public_names.append(node.name)
        elif check_classes and isinstance(node, ast.ClassDef):
            if not node.name.startswith("_"):
                public_names.append(node.name)
    return public_names


def _is_private_module(module_path: Path) -> bool:
    return module_path.stem.startswith("_")


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
