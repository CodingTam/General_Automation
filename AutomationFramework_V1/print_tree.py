import os
from pathlib import Path

def print_tree(directory, prefix="", exclude_dirs={'.git', '__pycache__', '.venv', 'filecomp_env', 'full_codebase_zip'}):
    paths = sorted(Path(directory).iterdir())
    pointers = ["├── " if i < len(paths) - 1 else "└── " for i in range(len(paths))]
    for pointer, path in zip(pointers, paths):
        if path.name.startswith('.') or path.name in exclude_dirs:
            continue
        print(prefix + pointer + path.name)
        if path.is_dir():
            extension = "│   " if pointer == "├── " else "    "
            print_tree(path, prefix=prefix + extension, exclude_dirs=exclude_dirs)

if __name__ == "__main__":
    print(".")
    print_tree(".") 