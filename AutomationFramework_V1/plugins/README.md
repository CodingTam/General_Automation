# File Comparator Plugin

A Python plugin that allows users to visually compare two files (CSV/Excel) side-by-side, similar to Beyond Compare.

## Features

- Compare CSV and Excel files visually in a side-by-side view
- Highlight differences between the files
- Select key columns for row matching
- Optional decode file support for mapping codes to descriptions
- Detailed difference summary with categorized differences
- Works entirely in the browser, no external tools needed

## Installation

### Requirements

- Python 3.7+
- FastAPI
- Pandas
- React (for frontend development)

### Dependencies

Install the required Python packages:

```bash
pip install fastapi uvicorn pandas openpyxl
```

## Usage

### Basic Usage

```python
from plugin.file_comparator import launch_comparator_plugin

# Launch the comparator with source and target files
launch_comparator_plugin(
    source_file="path/to/source.csv", 
    target_file="path/to/target.csv"
)
```

This will:
1. Start a FastAPI server if not already running
2. Open a browser window with the comparison UI
3. Load the source and target files automatically

### Workflow

1. Once the UI is loaded, select one or more key columns from the dropdown
2. Click the "Compare" button to show differences
3. Differences will be highlighted in both tables and summarized in the panel below
4. Optional: Load a decode file to translate codes into human-readable descriptions

## Building the Frontend

The plugin includes a pre-built React frontend. If you want to modify and rebuild it:

1. Navigate to the `frontend` directory:
   ```bash
   cd plugin/file_comparator/frontend
   ```

2. Install dependencies:
   ```bash
   npm install
   ```

3. Build the frontend:
   ```bash
   npm run build
   ```

This will automatically copy the built files to the `static` directory used by FastAPI.

## Architecture

- **Backend**: FastAPI server for file handling and comparison
- **Frontend**: React + TailwindCSS for UI components
- **State Management**: Zustand for simple state management
- **Integration**: FastAPI serves the React app as static files

## License

MIT 