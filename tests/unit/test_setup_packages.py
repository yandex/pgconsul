# encoding: utf-8
"""Unit test reproducing MDB-41951: setup.py must include all subpackages.

Reproduces the behave failure where pgconsul crashed inside the Docker
container with ``ModuleNotFoundError: No module named 'pgconsul.switchover'``.
Root cause: ``setup.py`` listed only ``packages=['pgconsul']`` and did not
include the ``pgconsul.switchover`` subpackage, so ``pip install`` never
copied ``src/switchover/`` into site-packages.

See: tests/features/targeted_switchover.feature (stuck on the
``Given a replication slot ...`` step because pgconsul was in FATAL state).
"""

import ast
import os
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
SETUP_PY = REPO_ROOT / 'setup.py'
SRC_DIR = REPO_ROOT / 'src'

# Directories under src/ that are not real pgconsul subpackages and must be
# excluded from the package-discovery check (e.g. the local virtualenv).
_EXCLUDED_DIRS = frozenset({'venv', '__pycache__', '.pytest_cache', 'build', 'dist'})


def _extract_setup_call(setup_py_path: Path) -> ast.Call:
    """Parse setup.py and return the ``setup(...)`` AST call node."""
    source = setup_py_path.read_text(encoding='utf-8')
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and getattr(node.func, 'id', None) == 'setup':
            return node
    raise AssertionError('setup() call not found in setup.py')


def _get_keyword(call_node: ast.Call, name: str) -> ast.AST | None:
    for kw in call_node.keywords:
        if kw.arg == name:
            return kw.value
    return None


def _collect_subpackages(src_dir: Path) -> set[str]:
    """Return the set of importable package names under src/."""
    packages: set[str] = set()
    if not src_dir.is_dir():
        return packages
    # src/ itself is the top-level package (pgconsul) because src/__init__.py exists.
    if (src_dir / '__init__.py').exists():
        packages.add('pgconsul')
    for root, dirs, _files in os.walk(src_dir):
        dirs[:] = [d for d in dirs if d not in _EXCLUDED_DIRS]
        for d in dirs:
            if (Path(root) / d / '__init__.py').exists():
                rel = Path(root, d).relative_to(src_dir)
                parts = ('pgconsul',) + rel.parts
                packages.add('.'.join(parts))
    return packages


class TestSetupPyPackages:
    """Ensure setup.py declares every subpackage under src/."""

    def test_packages_keyword_includes_all_subpackages(self) -> None:
        """The ``packages`` keyword in setup.py must cover every subpackage.

        This is the red test for the MDB-41951 behave failure: before the fix
        ``packages`` was a hard-coded list ``['pgconsul']`` that missed
        ``pgconsul.switchover``.
        """
        call_node = _extract_setup_call(SETUP_PY)
        packages_node = _get_keyword(call_node, 'packages')
        assert packages_node is not None, 'setup() must declare a packages keyword'

        # Resolve the declared packages. We support two forms:
        #   1. find_packages(where='src')  — the fixed version
        #   2. ['pgconsul', ...]           — explicit list (old form)
        declared: set[str]
        if isinstance(packages_node, ast.Call):
            func_name = getattr(packages_node.func, 'id', None) or getattr(packages_node.func, 'attr', None)
            assert func_name == 'find_packages', (
                f'packages must use find_packages or an explicit list, got {func_name}'
            )
            # find_packages(where='src') — emulate it.
            where_arg = None
            for kw in packages_node.keywords:
                if kw.arg == 'where':
                    if isinstance(kw.value, ast.Constant):
                        where_arg = kw.value.value
            where_dir = REPO_ROOT / (where_arg or '.')
            declared = set(_collect_subpackages(where_dir))
        elif isinstance(packages_node, (ast.List, ast.Tuple)):
            declared = {
                el.value for el in packages_node.elts if isinstance(el, ast.Constant)
            }
        else:
            pytest.fail(f'Unsupported packages= expression: {ast.dump(packages_node)}')

        expected = _collect_subpackages(SRC_DIR)
        missing = expected - declared
        assert not missing, (
            f'setup.py packages= is missing subpackages: {sorted(missing)}. '
            f'Declared: {sorted(declared)}. Expected: {sorted(expected)}.'
        )

    def test_package_dir_maps_root_to_src(self) -> None:
        """package_dir must map the project root to src/ so subpackages resolve.

        With the old ``package_dir={'pgconsul': 'src'}`` form, only the
        top-level package was mapped and subpackages were never installed.
        The fix uses ``package_dir={'': 'src'}``.
        """
        call_node = _extract_setup_call(SETUP_PY)
        pkg_dir_node = _get_keyword(call_node, 'package_dir')
        assert pkg_dir_node is not None, 'setup() must declare package_dir'

        # Accept either {'': 'src'} (preferred) or {'pgconsul': 'src'} (legacy,
        # but only correct when packages= lists every subpackage explicitly).
        if isinstance(pkg_dir_node, ast.Dict):
            mapping = {
                k.value: v.value
                for k, v in zip(pkg_dir_node.keys, pkg_dir_node.values)
                if isinstance(k, ast.Constant) and isinstance(v, ast.Constant)
            }
            assert mapping.get('') == 'src' or mapping.get('pgconsul') == 'src', (
                f'package_dir must map root or pgconsul to src, got {mapping}'
            )
        else:
            pytest.fail(f'Unsupported package_dir expression: {ast.dump(pkg_dir_node)}')
