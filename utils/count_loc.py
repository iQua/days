import os
import re
from pathlib import Path


def count_loc_rust(directory):
    """Counts non-empty, non-comment lines of code in Rust files recursively,
       excluding test code (lines before #[cfg(test)]) and the tests/ directory.

    Args:
        directory (str): The path to the directory to analyze.

    Returns:
        tuple: A tuple containing (dict: file_counts, int: total_loc)
    """
    file_counts = {}
    total_loc = 0

    for root, _, files in os.walk(directory):
        # Skip 'tests' and 'target' directories and their subdirectories
        path_parts = root.split(os.sep)
        if "tests" in path_parts or "target" in path_parts:
            continue

        for file in files:
            if file.endswith(".rs"):
                file_path = os.path.join(root, file)
                loc_count = count_lines_before_test(file_path)
                file_counts[file_path] = loc_count
                total_loc += loc_count

    return file_counts, total_loc


def count_lines_before_test(file_path):
    """Counts non-empty, non-comment lines of code in a single Rust file
        before #[cfg(test)].

    Args:
        file_path (str): The path to the Rust file.

    Returns:
        int: The number of non-empty, non-comment lines of code.
    """
    line_count = 0
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()

            # Check for test marker, exit if found
            if re.match(r"^#\[cfg\(test\)\]", line):
                break

            # Skip empty lines and comments
            if not line or line.startswith("//"):
                continue

            line_count += 1

    return line_count


def sanitize_base_directory(path_str):
    """Removes any trailing 'target' segment from the provided base directory."""
    expanded = os.path.expanduser(path_str.strip())
    if not expanded:
        return expanded

    path = Path(expanded)
    if "target" not in path.parts:
        return os.path.normpath(expanded)

    target_index = path.parts.index("target")
    sanitized_parts = path.parts[:target_index]
    sanitized_path = Path(*sanitized_parts) if sanitized_parts else Path(".")

    # Normalize to remove redundant separators like trailing slashes
    return os.path.normpath(str(sanitized_path))


if __name__ == "__main__":
    raw_directory = input("Enter the directory to analyze: ")
    target_directory = sanitize_base_directory(raw_directory)

    if raw_directory.strip() and target_directory != os.path.normpath(raw_directory.strip()):
        print(f"Sanitized base directory to exclude 'target/': {target_directory}")

    if not os.path.isdir(target_directory):
        print(f"{target_directory} is not a valid directory.")
    else:
        loc_data, total_loc = count_loc_rust(target_directory)

        if loc_data:
            for file_path, count in loc_data.items():
                print(f"{file_path}: {count}")

            print(f"\nTotal: {total_loc} lines of code.")
        else:
            print("No Rust files found in the specified directory.")
