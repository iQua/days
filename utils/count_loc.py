import os
import re


def count_loc_rust(directory):
    """Counts non-empty, non-comment lines of code in Rust files recursively,
       excluding test code (lines before #[cfg(test)]).

    Args:
        directory (str): The path to the directory to analyze.

    Returns:
        tuple: A tuple containing (dict: file_counts, int: total_loc)
    """
    file_counts = {}
    total_loc = 0

    for root, _, files in os.walk(directory):
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


if __name__ == "__main__":
    target_directory = input("Enter the directory to analyze: ")

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
