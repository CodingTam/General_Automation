import React from 'react';

const DiffTable = ({ differences, keyColumns }) => {
  if (!differences || differences.length === 0) {
    return <div className="p-4 text-gray-500">No differences found</div>;
  }
  
  // Helper to format a key value nicely
  const formatKeyValue = (keyValues) => {
    return Object.entries(keyValues)
      .map(([key, value]) => `${key}: ${value}`)
      .join(', ');
  };
  
  // Get status text and color
  const getStatusInfo = (status) => {
    switch (status) {
      case 'missing_in_source':
        return { text: 'Missing in Source', color: 'text-red-600' };
      case 'missing_in_target':
        return { text: 'Missing in Target', color: 'text-orange-600' };
      case 'values_differ':
        return { text: 'Values Differ', color: 'text-blue-600' };
      default:
        return { text: status, color: 'text-gray-600' };
    }
  };
  
  return (
    <table className="diff-table w-full border-collapse">
      <thead>
        <tr>
          <th>Key</th>
          <th>Status</th>
          <th>Column</th>
          <th>Source Value</th>
          <th>Target Value</th>
        </tr>
      </thead>
      <tbody>
        {differences.map((diff, index) => {
          const { text: statusText, color: statusColor } = getStatusInfo(diff.status);
          
          if (diff.status === 'values_differ') {
            // For value differences, show each column difference as a row
            return diff.differences.map((cellDiff, cellIndex) => (
              <tr key={`${index}-${cellIndex}`}>
                {cellIndex === 0 ? (
                  // Only show key values in the first row for this difference
                  <td rowSpan={diff.differences.length}>
                    {formatKeyValue(diff.key_values)}
                  </td>
                ) : null}
                {cellIndex === 0 ? (
                  // Only show status in the first row for this difference
                  <td rowSpan={diff.differences.length} className={statusColor}>
                    {statusText}
                  </td>
                ) : null}
                <td>{cellDiff.column}</td>
                <td className="cell-different">{cellDiff.source_value}</td>
                <td className="cell-different">{cellDiff.target_value}</td>
              </tr>
            ));
          } else {
            // For missing rows, just show one row
            return (
              <tr key={index} className={diff.status === 'missing_in_source' ? 'row-missing-source' : 'row-missing-target'}>
                <td>{formatKeyValue(diff.key_values)}</td>
                <td className={statusColor}>{statusText}</td>
                <td colSpan={3} className="text-center">
                  {diff.status === 'missing_in_source' 
                    ? 'Row exists only in target file' 
                    : 'Row exists only in source file'}
                </td>
              </tr>
            );
          }
        }).flat()}
      </tbody>
    </table>
  );
};

export default DiffTable; 