import { create } from 'zustand';
import axios from 'axios';

// URL of the API (FastAPI backend)
const API_URL = 'http://localhost:8081';

export const useFileStore = create((set, get) => ({
  // File data
  sourceFile: null,
  targetFile: null,
  commonColumns: [],
  keyColumns: [],
  
  // Comparison results
  differences: [],
  summary: {
    total_differences: 0,
    missing_in_source: 0,
    missing_in_target: 0,
    values_differ: 0
  },
  
  // Loading and error states
  isLoading: false,
  error: null,
  
  // Initialize from URL parameters
  initializeFromUrl: () => {
    // Get source and target file paths from window object
    // These are set by the FastAPI backend in the HTML template
    const sourceFile = window.sourceFile;
    const targetFile = window.targetFile;
    
    if (sourceFile && targetFile) {
      get().loadFiles(sourceFile, targetFile);
    }
  },
  
  // Load files from backend
  loadFiles: async (sourcePath, targetPath) => {
    try {
      set({ isLoading: true, error: null });
      
      const response = await axios.post(`${API_URL}/load_files`, {
        source_file: sourcePath,
        target_file: targetPath,
        key_columns: [] // Empty at this point
      });
      
      const { source, target, common_columns } = response.data;
      
      set({
        sourceFile: source,
        targetFile: target,
        commonColumns: common_columns,
        keyColumns: [], // Reset key columns
        differences: [], // Reset differences
        summary: { // Reset summary
          total_differences: 0,
          missing_in_source: 0,
          missing_in_target: 0,
          values_differ: 0
        },
        isLoading: false
      });
    } catch (error) {
      set({ 
        isLoading: false, 
        error: error.response?.data?.detail || error.message || 'Failed to load files' 
      });
    }
  },
  
  // Set key columns
  setKeyColumns: (columns) => {
    set({ keyColumns: columns });
  },
  
  // Compare files
  compareFiles: async () => {
    const { sourceFile, targetFile, keyColumns } = get();
    
    if (!sourceFile || !targetFile) {
      set({ error: 'Please load source and target files first' });
      return;
    }
    
    if (!keyColumns.length) {
      set({ error: 'Please select at least one key column' });
      return;
    }
    
    try {
      set({ isLoading: true, error: null });
      
      const response = await axios.post(`${API_URL}/compare`, {
        source_file: sourceFile.file_path,
        target_file: targetFile.file_path,
        key_columns: keyColumns
      });
      
      set({
        differences: response.data.results,
        summary: response.data.summary,
        isLoading: false
      });
    } catch (error) {
      set({ 
        isLoading: false, 
        error: error.response?.data?.detail || error.message || 'Failed to compare files' 
      });
    }
  },
  
  // Load decode file
  loadDecodeFile: async (filePath, columnName) => {
    try {
      set({ isLoading: true, error: null });
      
      const response = await axios.post(`${API_URL}/decode`, {
        decode_file: filePath,
        column_name: columnName
      });
      
      // Apply the decode mapping to the source and target data
      const { sourceFile, targetFile } = get();
      const { column_name, mapping } = response.data;
      
      if (sourceFile) {
        const updatedSourceData = sourceFile.data.map(row => {
          if (row[column_name] && mapping[row[column_name]]) {
            return {
              ...row,
              [`${column_name}_decoded`]: mapping[row[column_name]]
            };
          }
          return row;
        });
        
        set({
          sourceFile: {
            ...sourceFile,
            data: updatedSourceData,
            columns: [...new Set([...sourceFile.columns, `${column_name}_decoded`])]
          }
        });
      }
      
      if (targetFile) {
        const updatedTargetData = targetFile.data.map(row => {
          if (row[column_name] && mapping[row[column_name]]) {
            return {
              ...row,
              [`${column_name}_decoded`]: mapping[row[column_name]]
            };
          }
          return row;
        });
        
        set({
          targetFile: {
            ...targetFile,
            data: updatedTargetData,
            columns: [...new Set([...targetFile.columns, `${column_name}_decoded`])]
          }
        });
      }
      
      set({ isLoading: false });
    } catch (error) {
      set({ 
        isLoading: false, 
        error: error.response?.data?.detail || error.message || 'Failed to load decode file' 
      });
    }
  },
  
  // Reset state
  reset: () => {
    set({
      sourceFile: null,
      targetFile: null,
      commonColumns: [],
      keyColumns: [],
      differences: [],
      summary: {
        total_differences: 0,
        missing_in_source: 0,
        missing_in_target: 0,
        values_differ: 0
      },
      error: null
    });
  }
})); 