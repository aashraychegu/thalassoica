import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone
import re
import argparse


def convert_to_parquet(input_file: str, output_file: str, compression: str = 'snappy'):
    """
    Convert cyclone data to Parquet using pure PyArrow.
    
    Parameters:
    -----------
    input_file : str
        Path to input cyclone data file
    output_file : str
        Path to output Parquet file
    compression : str
        Compression algorithm (snappy, gzip, brotli, zstd, none)
    """
    # Read and filter data lines
    with open(input_file, 'r') as f:
        data_lines = [
            line.strip().split(',') 
            for line in f 
            if line.strip() and re.match(r'^\d{8},', line.strip())
        ]
    
    # Extract columns
    timestamps = []
    latitudes = []
    longitudes = []
    sizes = []
    
    for row in data_lines:
        date_str = row[0].strip()
        hour = int(row[1].strip())
        
        # Parse datetime
        year = int(date_str[:4])
        month = int(date_str[4:6])
        day = int(date_str[6:8])
        dt = datetime(year, month, day, hour, 0, 0, tzinfo=timezone.utc)
        
        timestamps.append(dt)
        latitudes.append(float(row[2].strip()))
        longitudes.append(float(row[3].strip()))
        sizes.append(float(row[6].strip()))
    
    # Create PyArrow table
    table = pa.table({
        'datetime_start': pa.array(timestamps, type=pa.timestamp('us', tz='UTC')),
        'latitude': pa.array(latitudes, type=pa.float64()),
        'longitude': pa.array(longitudes, type=pa.float64()),
        'size': pa.array(sizes, type=pa.float64())
    })
    
    # Write to Parquet
    pq.write_table(
        table,
        output_file,
        compression=compression
    )
    
    print(f"Converted {len(table)} records to {output_file}")
    print(f"Compression: {compression}")
    return table


def main():
    parser = argparse.ArgumentParser(
        description='Convert cyclone tracking data to Parquet format',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s input.txt output.parquet
  %(prog)s input.txt output.parquet --compression gzip
  %(prog)s input.txt output.parquet --compression none --verbose
        """
    )
    
    parser.add_argument(
        'input_file',
        type=str,
        help='Input cyclone data file (comma-delimited text)'
    )
    
    parser.add_argument(
        'output_file',
        type=str,
        help='Output Parquet file path'
    )
    
    parser.add_argument(
        '-c', '--compression',
        type=str,
        choices=['snappy', 'gzip', 'brotli', 'zstd', 'none'],
        default='snappy',
        help='Compression algorithm (default: snappy)'
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Show detailed output including sample records'
    )
    
    args = parser.parse_args()
    
    # Convert file
    table = convert_to_parquet(
        args.input_file,
        args.output_file,
        args.compression
    )
    
    # Show details if verbose
    if args.verbose:
        print(f"\nSchema:")
        print(table.schema)
        print(f"\nFirst 5 records:")
        print(table.slice(0, 5).to_pandas())


if __name__ == "__main__":
    main()