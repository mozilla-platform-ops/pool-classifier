export function initSortableTable(table, {numericColumns = [], missingLast = [], initialColumn, initialDirection, storageKey} = {}) {
  const headers = [...table.querySelectorAll('th')];
  const rows = [...table.tBodies[0].rows];
  const numeric = new Set(numericColumns);
  const missing = new Set(missingLast);
  rows.forEach((row, index) => row.dataset.sortIndex ||= index);

  function sortValue(row, index) {
    return row.children[index].dataset.sortValue || row.children[index].textContent.trim();
  }

  function applySort(index, ascending) {
    headers.forEach(header => delete header.dataset.sort);
    headers[index].dataset.sort = ascending ? 'asc' : 'desc';
    rows.sort((left, right) => {
      const leftValue = sortValue(left, index);
      const rightValue = sortValue(right, index);
      if (missing.has(index) && (!leftValue || !rightValue)) {
        return leftValue === rightValue ? 0 : leftValue ? -1 : 1;
      }
      const comparison = numeric.has(index)
        ? Number(leftValue) - Number(rightValue)
        : leftValue.localeCompare(rightValue, undefined, {numeric: true, sensitivity: 'base'});
      return (ascending ? comparison : -comparison)
        || Number(left.dataset.sortIndex) - Number(right.dataset.sortIndex);
    });
    rows.forEach(row => table.tBodies[0].append(row));
  }

  function saveSort(index, ascending) {
    if (!storageKey) return;
    try {
      localStorage.setItem(storageKey, JSON.stringify({index, direction: ascending ? 'asc' : 'desc'}));
    } catch (_error) {
      // Sorting still works when browser storage is unavailable.
    }
  }

  function restoreSort() {
    if (!storageKey) return false;
    try {
      const saved = JSON.parse(localStorage.getItem(storageKey));
      if (!Number.isInteger(saved?.index) || saved.index < 0 || saved.index >= headers.length) return false;
      if (saved.direction !== 'asc' && saved.direction !== 'desc') return false;
      applySort(saved.index, saved.direction === 'asc');
      return true;
    } catch (_error) {
      return false;
    }
  }

  headers.forEach((header, index) => header.addEventListener('click', () => {
    const ascending = header.dataset.sort
      ? header.dataset.sort === 'desc'
      : header.dataset.defaultDirection === 'asc';
    applySort(index, ascending);
    saveSort(index, ascending);
  }));

  if (!restoreSort() && initialColumn !== undefined) applySort(initialColumn, initialDirection === 'asc');
}
