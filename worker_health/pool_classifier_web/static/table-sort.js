export function initSortableTable(table, {numericColumns = [], missingLast = [], initialColumn, initialDirection} = {}) {
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

  headers.forEach((header, index) => header.addEventListener('click', () => {
    const ascending = header.dataset.sort
      ? header.dataset.sort === 'desc'
      : header.dataset.defaultDirection === 'asc';
    applySort(index, ascending);
  }));

  if (initialColumn !== undefined) applySort(initialColumn, initialDirection === 'asc');
}
