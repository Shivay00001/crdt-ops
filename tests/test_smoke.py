"""Smoke test for the Go CRDT library (no Go toolchain in this environment).

Verifies the module layout is complete and the Go test file exercises the
constructors exported by the library.
"""
import re
from pathlib import Path

ROOT = Path(__file__).parent.parent


def test_module_layout():
    assert (ROOT / "go.mod").exists(), "go.mod missing"
    assert (ROOT / "crdt_ops.go").exists(), "crdt_ops.go missing"
    assert (ROOT / "crdt_ops_test.go").exists(), "crdt_ops_test.go missing"
    mod = (ROOT / "go.mod").read_text()
    assert re.search(r"^module \S+", mod, re.M)


def test_go_test_file_covers_constructors():
    src = (ROOT / "crdt_ops.go").read_text()
    test = (ROOT / "crdt_ops_test.go").read_text()
    assert "package crdt" in src
    constructors = re.findall(r"^func (New\w+)\(", src, re.M)
    assert constructors, "no constructors found"
    missing = [c for c in constructors if c not in test]
    assert not missing, f"constructors not covered by go tests: {missing}"
