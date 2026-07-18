import os
import re

def to_upper_snake(name):
    """Converts camelCase or mixedCase names into UPPER_SNAKE_CASE."""
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
    return s2.replace('-', '_').upper()

def process_line(line):
    """
    Evaluates individual lines. Safely skips functions and filters out
    standalone global variables for conversion.
    """
    trimmed = line.strip()

    # Absolute Guardrails: Do not touch comments, functions, macro calls, or block logic
    if (not trimmed or
        trimmed.startswith('//') or
        trimmed.startswith('/*') or
        '(' in trimmed or
        ')' in trimmed or
        '{' in trimmed or
        '}' in trimmed or
        trimmed.startswith('pub mod') or
        trimmed.startswith('use ')):
        return line

    # Ensure it looks like a variable definition ending with a semicolon
    if not trimmed.endswith(';'):
        return line

    is_static = trimmed.startswith('static ')
    clean_line = re.sub(r'^static\s+', '', trimmed)

    # Match syntax pattern: Type name = value; or Type name;
    match = re.match(r'^([\w\s:&*]+?)\s+\b([A-Za-z0-9_]+)\b(?:\s*=\s*(.*?))?\s*;', clean_line)
    if not match:
        return line

    raw_type = match.group(1).strip()
    var_name = match.group(2).strip()
    raw_val = match.group(3).strip() if match.group(3) else None

    # Skip accidental keyword matches
    if var_name in ['return', 'pub', 'static', 'fn', 'struct', 'enum']:
        return line

    # 1. Normalize Names to UPPER_SNAKE_CASE
    rust_name = to_upper_snake(var_name)

    # 2. Normalize and Map Types
    if '*' in raw_type or '&mut' in raw_type:
        clean_t = raw_type.replace('*', '').replace('&mut', '').replace(':', '').strip()
        rust_type = f"*mut {clean_t}"
        default_val = "std::ptr::null_mut()"
    else:
        type_mapping = {
            'bool': ('bool', 'false'),
            'int': ('std::os::raw::c_int', '0'),
            'Size': ('Size', '0'),
            'Pointer': ('Pointer', 'std::ptr::null_mut()')
        }
        rust_type, default_val = type_mapping.get(raw_type, (raw_type, 'std::mem::zeroed()'))

    # 3. Handle Assignment Values
    if raw_val:
        if raw_val == 'NULL':
            rust_val = "std::ptr::null_mut()"
        elif raw_val.lower() in ['true', 'false']:
            rust_val = raw_val.lower()
        else:
            rust_val = raw_val
    else:
        rust_val = default_val

    # 4. Construct Final Rust Code String, matching the original line's indentation
    indent = line[:len(line) - len(line.lstrip())]
    visibility = "" if is_static else "pub "

    return f"{indent}{visibility}static mut {rust_name}: {rust_type} = {rust_val};\n"

def process_file(file_path):
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        lines = f.readlines()

    new_lines = []
    for line in lines:
        new_lines.append(process_line(line))

    with open(file_path, 'w', encoding='utf-8') as f:
        f.writelines(new_lines)

def walk_and_convert(target_dir):
    print(f"Executing strict line-by-line variable parsing across '{target_dir}'...")
    for root, _, files in os.walk(target_dir):
        for file in files:
            if file.endswith('.rs'):
                file_path = os.path.join(root, file)
                process_file(file_path)
                print(f"✓ Formatted variables safely: {file_path}")
    print("Variable declaration conversion completed successfully!")

if __name__ == "__main__":
    walk_and_convert('src')
