"""
Script to set up the Snowflake catalog tables manually.

This is a standalone script that can be run directly to set up the catalog tables
before running the main application.

Usage:
  python setup_snowflake_catalog.py <account> <username> <password> [warehouse] [role]

Example:
  python setup_snowflake_catalog.py myaccount myuser mypassword compute_wh accountadmin
"""
import sys
import os
import json

# Add parent directory to sys.path to import from backend
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

# Import the setup function
from db_connection.setup_catalog import setup_snowflake_catalog

def main():
    """Run the setup script with command line arguments or from a config file."""
    # First, try to load from a config file
    config_path = os.path.join(os.path.dirname(__file__), 'snowflake_config.json')
    
    if os.path.exists(config_path):
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
                print(f"Loaded configuration from {config_path}")
                
                account = config.get('account')
                username = config.get('username')
                password = config.get('password')
                warehouse = config.get('warehouse')
                role = config.get('role')
                
                if account and username and password:
                    setup_snowflake_catalog(account, username, password, warehouse, role)
                    return
                else:
                    print("Configuration file is missing required fields")
        except Exception as e:
            print(f"Error loading configuration file: {str(e)}")
    
    # If config file doesn't exist or is invalid, use command line arguments
    if len(sys.argv) < 4:
        print(__doc__)
        sys.exit(1)
    
    account = sys.argv[1]
    username = sys.argv[2]
    password = sys.argv[3]
    warehouse = sys.argv[4] if len(sys.argv) > 4 else None
    role = sys.argv[5] if len(sys.argv) > 5 else None
    
    setup_snowflake_catalog(account, username, password, warehouse, role)

if __name__ == "__main__":
    main() 