import argparse

parser = argparse.ArgumentParser(
    description="Download Sentinel-1 TIFF files from S3 based on a uuid parquet"
)
parser.add_argument("--uuids", required=True, help="Path to uuids file with 'id' and 's3_path' columns")
parser.add_argument("--output", required=True, help="Output directory where folders with ID names will be created")
parser.add_argument("--token-dotenv", default="copernicus_tokens.env", help="Path to .env file")
parser.add_argument("--login-dotenv",default="copernicus_login.env",)
parser.add_argument("--search-workers", type=int, default=32, help="Number of parallel workers for searching (default: 32)")
parser.add_argument("--download-workers", type=int, default=4, help="Number of parallel workers (default: 4)")

args = parser.parse_args()

import sys
script_name = sys.argv[0]
cmd_parts = [f"uv run {script_name}"]

for action in parser._actions:
    if action.dest == 'help':
        continue
    
    # Get the actual value from parsed args
    value = getattr(args, action.dest, None)
    
    if value is not None:
        arg_name = action.dest.replace('_', '-')
        
        # Handle list arguments
        if isinstance(value, list):
            if value:  # Only add if list is not empty
                cmd_parts.append(f"--{arg_name} {' '.join(map(str, value))}")
        else:
            cmd_parts.append(f"--{arg_name} {value}")

print("Effective command:")
print(' \\\n  '.join(cmd_parts))
print()