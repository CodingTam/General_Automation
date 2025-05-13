import React, { useEffect } from 'react';
import SplitPane from 'react-split-pane';
import { useFileStore } from './store';
import ComparisonHeader from './components/ComparisonHeader';
import FileTable from './components/FileTable';
import DiffTable from './components/DiffTable';
import './App.css';

function App() {
  const { 
    initializeFromUrl,
    sourceFile,
    targetFile,
    keyColumns,
    differences,
    summary,
    isLoading,
    error
  } = useFileStore();

  // Initialize from URL parameters when the app loads
  useEffect(() => {
    initializeFromUrl();
  }, [initializeFromUrl]);

  return (
    <div className="flex flex-col h-screen bg-gray-100">
      <ComparisonHeader />
      
      <div className="flex-1 flex flex-col overflow-hidden">
        {/* Main comparison area with split pane */}
        <SplitPane
          split="vertical"
          defaultSize="50%"
          style={{ position: 'relative' }}
          pane1Style={{ overflow: 'auto' }}
          pane2Style={{ overflow: 'auto' }}
        >
          <div className="h-full flex flex-col">
            <div className="bg-blue-50 p-2 text-sm font-medium border-b">
              Source: {sourceFile?.file_path || 'No file loaded'}
              {sourceFile && <span className="ml-2 text-gray-500">({sourceFile.row_count} rows)</span>}
            </div>
            <div className="flex-1 overflow-auto">
              <FileTable data={sourceFile?.data || []} columns={sourceFile?.columns || []} type="source" />
            </div>
          </div>
          <div className="h-full flex flex-col">
            <div className="bg-green-50 p-2 text-sm font-medium border-b">
              Target: {targetFile?.file_path || 'No file loaded'}
              {targetFile && <span className="ml-2 text-gray-500">({targetFile.row_count} rows)</span>}
            </div>
            <div className="flex-1 overflow-auto">
              <FileTable data={targetFile?.data || []} columns={targetFile?.columns || []} type="target" />
            </div>
          </div>
        </SplitPane>

        {/* Differences panel */}
        {differences.length > 0 && (
          <div className="h-1/3 border-t-2 border-gray-300 overflow-auto">
            <div className="bg-gray-200 p-2 text-sm font-medium border-b flex justify-between items-center">
              <div>Differences ({summary.total_differences})</div>
              <div className="flex gap-4 text-xs">
                <span className="text-red-600">Missing in Source: {summary.missing_in_source}</span>
                <span className="text-orange-600">Missing in Target: {summary.missing_in_target}</span>
                <span className="text-blue-600">Value Differences: {summary.values_differ}</span>
              </div>
            </div>
            <DiffTable differences={differences} keyColumns={keyColumns} />
          </div>
        )}
      </div>

      {/* Loading indicator */}
      {isLoading && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white p-6 rounded-lg shadow-lg">
            <div className="animate-spin rounded-full h-12 w-12 border-t-2 border-b-2 border-blue-500 mx-auto"></div>
            <div className="mt-4 text-center">Loading...</div>
          </div>
        </div>
      )}

      {/* Error message */}
      {error && (
        <div className="fixed inset-x-0 top-4 flex justify-center z-50">
          <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded shadow-md">
            <strong className="font-bold">Error: </strong>
            <span className="block sm:inline">{error}</span>
          </div>
        </div>
      )}
    </div>
  );
}

export default App; 