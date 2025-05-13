import React, { useState } from 'react';
import { useFileStore } from '../store';
import MultiSelect from './MultiSelect';

const ComparisonHeader = () => {
  const { 
    sourceFile, 
    targetFile, 
    commonColumns, 
    keyColumns, 
    setKeyColumns, 
    compareFiles,
    loadDecodeFile,
    reset 
  } = useFileStore();
  
  const [decodeFile, setDecodeFile] = useState('');
  const [decodeColumn, setDecodeColumn] = useState('');
  
  const handleCompare = (e) => {
    e.preventDefault();
    compareFiles();
  };
  
  const handleDecodeUpload = (e) => {
    e.preventDefault();
    if (decodeFile && decodeColumn) {
      loadDecodeFile(decodeFile, decodeColumn);
      setDecodeFile('');
      setDecodeColumn('');
    }
  };
  
  return (
    <div className="bg-white shadow p-4">
      <h1 className="text-xl font-bold text-gray-800 mb-4">File Comparator</h1>
      
      {/* Main Controls */}
      <div className="flex flex-col md:flex-row gap-4 mb-4">
        {/* Key Column Selection */}
        <div className="flex-1">
          <label className="block text-sm font-medium text-gray-700 mb-1">
            Select Key Column(s)
          </label>
          <div className="flex gap-2">
            <MultiSelect
              options={commonColumns.map(col => ({ value: col, label: col }))}
              value={keyColumns}
              onChange={setKeyColumns}
              placeholder="Select columns for matching rows..."
              className="flex-1"
            />
            <button
              onClick={handleCompare}
              disabled={!sourceFile || !targetFile || keyColumns.length === 0}
              className="btn btn-primary"
            >
              Compare
            </button>
            <button
              onClick={reset}
              className="btn btn-secondary"
            >
              Reset
            </button>
          </div>
        </div>
      </div>
      
      {/* Decode Panel */}
      <div className="p-3 bg-gray-50 rounded-md">
        <h3 className="text-sm font-medium text-gray-700 mb-2">Optional Decode File</h3>
        <div className="flex flex-col md:flex-row gap-3">
          <div className="flex-1">
            <label className="block text-xs text-gray-600 mb-1">
              Decode File Path
            </label>
            <input
              type="text"
              value={decodeFile}
              onChange={(e) => setDecodeFile(e.target.value)}
              placeholder="Path to decode file (CSV)"
              className="w-full px-3 py-2 border border-gray-300 rounded-md text-sm"
            />
          </div>
          <div className="flex-1">
            <label className="block text-xs text-gray-600 mb-1">
              Column to Decode
            </label>
            <select
              value={decodeColumn}
              onChange={(e) => setDecodeColumn(e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 rounded-md text-sm"
            >
              <option value="">Select a column</option>
              {commonColumns.map(col => (
                <option key={col} value={col}>{col}</option>
              ))}
            </select>
          </div>
          <div className="flex items-end">
            <button
              onClick={handleDecodeUpload}
              disabled={!decodeFile || !decodeColumn}
              className="btn btn-secondary"
            >
              Apply Decode
            </button>
          </div>
        </div>
      </div>
    </div>
  );
};

export default ComparisonHeader; 