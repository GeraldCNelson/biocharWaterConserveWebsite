import os

def generate_directory_map(start_path, level=0):
    """Generate a map of the directory structure starting from the given path."""
    for item in os.listdir(start_path):
        item_path = os.path.join(start_path, item)
        indent = '    ' * level
        if os.path.isdir(item_path):
            print(f"{indent}{item}/")
            generate_directory_map(item_path, level + 1)
        else:
            print(f"{indent}{item}")

# Set the starting directory (adjust this path as needed)
starting_directory = os.getcwd()  # Current working directory
print(starting_directory)
generate_directory_map(starting_directory)