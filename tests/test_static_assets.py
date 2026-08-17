from pathlib import Path


def test_table_sort_helper_supports_numeric_keys_and_stable_ties():
    script = (Path(__file__).parent.parent / "worker_health/pool_classifier_web/static/table-sort.js").read_text()

    assert "export function initSortableTable" in script
    assert "numericColumns = []" in script
    assert "missingLast = []" in script
    assert "Number(left.dataset.sortIndex) - Number(right.dataset.sortIndex)" in script
    assert "storageKey" in script
    assert "localStorage.setItem(storageKey" in script
    assert "restoreSort()" in script
