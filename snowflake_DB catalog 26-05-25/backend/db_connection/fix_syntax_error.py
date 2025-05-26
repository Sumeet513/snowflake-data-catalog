"""
Script to fix syntax error in snowflake_metadata.py
This is a quick solution to fix the indentation error in the get_business_terms method
"""
import os
import re

def fix_syntax_error():
    """
    Fix the syntax error in snowflake_metadata.py by correcting the indentation 
    in the get_business_terms method.
    """
    filepath = os.path.join(os.path.dirname(__file__), 'snowflake_metadata.py')
    
    print(f"Attempting to fix syntax error in: {filepath}")
    
    # Check if the file exists
    if not os.path.exists(filepath):
        print(f"Error: File not found: {filepath}")
        return False
    
    # Read the file content
    with open(filepath, 'r', encoding='utf-8') as file:
        content = file.read()
    
    # Pattern to match the problematic indentation
    # This looks for the return block followed by an incorrectly indented except block
    pattern = r"(\s+return\s*\{\s*'status':\s*'success',\s*'terms':\s*terms,\s*'count':\s*len\(terms\)\s*\}\s*)\n(\s*)except\s+Exception\s+as\s+e:"
    
    # The correctly indented replacement
    replacement = r"\1\n\n        except Exception as e:"
    
    # Apply the fix
    fixed_content = re.sub(pattern, replacement, content)
    
    # Check if a change was made
    if fixed_content == content:
        print("No syntax error pattern found or already fixed")
        return False
    
    # Write the fixed content back to the file
    with open(filepath, 'w', encoding='utf-8') as file:
        file.write(fixed_content)
    
    print("Syntax error fixed successfully!")
    return True

if __name__ == "__main__":
    fixed = fix_syntax_error()
    if fixed:
        print("Syntax error was fixed. Please restart the server.")
    else:
        print("No changes were made to the file.") 