import React, { useState, useEffect, useRef } from 'react';
import { useFileStore } from '../store';

const FileTable = ({ data, columns, type }) => {
  const { differences, keyColumns } = useFileStore();
  const [highlightedRows, setHighlightedRows] = useState(new Set());
  const [highlightedCells, setHighlightedCells] = useState(new Map());
  const tableRef = useRef(null);
  
  // Build a map of highlighted rows and cells based on differences
  useEffect(() => {
    if (!differences || differences.length === 0) {
      setHighlightedRows(new Set());
      setHighlightedCells(new Map());
      return;
    }
    
    const rows = new Set();
    const cells = new Map();
    
    // Helper function to create a key for a row
    const getRowKey = (keyValues) => {
      return Object.entries(keyValues)
        .map(([key, value]) => `${key}:${value}`)
        .join('|');
    };
    
    // Process differences to highlight rows and cells
    differences.forEach(diff => {
      const rowKey = getRowKey(diff.key_values);
      
      if (diff.status === 'missing_in_source' && type === 'target') {
        rows.add(rowKey);
      } else if (diff.status === 'missing_in_target' && type === 'source') {
        rows.add(rowKey);
      } else if (diff.status === 'values_differ') {
        // For value differences, highlight specific cells
        diff.differences.forEach(cellDiff => {
          const columnName = cellDiff.column;
          if (type === 'source') {
            cells.set(`${rowKey}|${columnName}`, cellDiff.source_value);
          } else if (type === 'target') {
            cells.set(`${rowKey}|${columnName}`, cellDiff.target_value);
          }
        });
      }
    });
    
    setHighlightedRows(rows);
    setHighlightedCells(cells);
  }, [differences, type]);
  
  // Check if a row should be highlighted
  const isRowHighlighted = (row) => {
    if (!keyColumns.length || !highlightedRows.size) return false;
    
    // Create a key for this row
    const rowKey = keyColumns
      .map(key => `${key}:${row[key]}`)
      .join('|');
    
    return highlightedRows.has(rowKey);
  };
  
  // Check if a cell should be highlighted
  const isCellHighlighted = (row, columnName) => {
    if (!keyColumns.length || !highlightedCells.size) return false;
    
    // Create a key for this row
    const rowKey = keyColumns
      .map(key => `${key}:${row[key]}`)
      .join('|');
    
    // Check if this cell is in the highlighted cells map
    return highlightedCells.has(`${rowKey}|${columnName}`);
  };
  
  // Get the CSS class for a row
  const getRowClass = (row) => {
    if (!isRowHighlighted(row)) return '';
    
    if (type === 'source') {
      return 'row-missing-target';
    } else {
      return 'row-missing-source';
    }
  };
  
  if (!data.length || !columns.length) {
    return <div className="p-4 text-gray-500">No data to display</div>;
  }
  
  return (
    <div className="overflow-auto" ref={tableRef}>
      <table className="diff-table w-full border-collapse">
        <thead>
          <tr>
            {columns.map(column => (
              <th key={column} className="sticky top-0 z-10">
                {column}
                {keyColumns.includes(column) && (
                  <span className="ml-1 text-blue-500">🔑</span>
                )}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {data.map((row, rowIndex) => (
            <tr key={rowIndex} className={getRowClass(row)}>
              {columns.map(column => (
                <td 
                  key={`${rowIndex}-${column}`}
                  className={isCellHighlighted(row, column) ? 'cell-different' : ''}
                >
                  {row[column] ?? ''}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};

export default FileTable; 