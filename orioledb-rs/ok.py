import os
import re

def transform_struct_array(match_text):
    """
    Parses a C static const array of structures and rewrites it into an
    idiomatic Rust read-only static slice of structures.
    """
    # Extract structural components
    header_match = re.search(r'static\s+const\s+struct\s+(\w+)\s+(\w+)\[\]\s*=\s*\{', match_text)
    if not header_match:
        return match_text

    struct_type = header_match.group(1).strip()
    array_name = header_match.group(2).strip()

    # Convert name to UPPER_SNAKE_CASE for Rust static style conventions
    rust_name = re.sub(r'(?<!^)(?=[A-Z])', '_', array_name).upper()
    # Map the type name to CamelCase
    rust_type = "".join([part.capitalize() for part in struct_type.split('_')])

    # Extract all elements inside the outer braces
    body_match = re.search(r'=\s*\{(.*)\};', match_text, re.DOTALL)
    if not body_match:
        return match_text

    raw_body = body_match.group(1).strip()

    # Parse individual entries matching {...}
    entries = re.findall(r'\{([^{}]+)\}', raw_body)
    rust_entries = []

    for entry in entries:
        # Split tokens by comma
        tokens = [t.strip() for t in entry.split(',')]
        if not tokens:
            continue

        # Transform string literals or NULL tokens
        processed_tokens = []
        for token in tokens:
            if token == 'NULL':
                processed_tokens.append('std::ptr::null()')
            elif token.startswith('"') and token.endswith('"'):
                # Append c-string termination or keep standard literal depending on implementation
                processed_tokens.append(f"b{token}\\0\".as_ptr() as *const std::os::raw::c_char")
            else:
                processed_tokens.append(token)

        # Format as an anonymous initialization mapping to the structural layout
        rust_entries.append(f"\t{rust_type} {{ {', '.join(processed_tokens)} }}")

    # Combine into a clean Rust static block statement
    rust_code = f"static {rust_name}: &[{rust_type}] = &[\n"
    rust_code += ",\n".join(rust_entries)
    rust_code += "\n];\n"
    return rust_code

def process_file(file_path):
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        content = f.read()

    # Regex targeting the multi-line signature block of static const arrays down to its closing balance };
    array_pattern = r'static\s+const\s+struct\s+\w+\s+\w+\[\]\s*=\s*\{.*?\};'

    updated_content = re.sub(array_pattern, lambda m: transform_struct_array(m.group(0)), content, flags=re.DOTALL)

    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(updated_content)

def walk_and_convert(target_dir):
    print(f"Migrating C structural array declarations across '{target_dir}'...")
    for root, _, files in os.walk(target_dir):
        for file in files:
            if file.endswith('.rs'):
                file_path = os.path.join(root, file)
                process_file(file_path)
                print(f"✓ Transformed static configuration array: {file_path}")
    print("Static structural configuration mapping complete.")

if __name__ == "__main__":
    walk_and_convert('src')
