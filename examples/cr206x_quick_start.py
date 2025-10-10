#!/usr/bin/env python3
"""
CR206X Quick Start Examples

This script provides simple, runnable examples for accessing Campbell Scientific
CR206X data loggers. Copy and modify these examples for your specific needs.

Usage:
    python cr206x_quick_start.py
"""

import sys
import os

# Add parent directory to path to import biochar_app modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from biochar_app.cr206x_data_access import CR206XDataLogger
import pandas as pd


def example_1_tcp_connection():
    """
    Example 1: Connect to CR206X via TCP/IP
    
    This is the recommended method for the biochar project.
    Update the host IP address to match your base station.
    """
    print("\n" + "="*60)
    print("Example 1: TCP/IP Connection to CR206X")
    print("="*60)
    
    # Configuration - UPDATE THESE VALUES
    HOST = '192.168.1.100'  # Replace with your base station IP or IPv6 address
    PORT = 6785             # PakBus TCP port (standard)
    
    print(f"Attempting connection to {HOST}:{PORT}...")
    
    # Create logger instance
    logger = CR206XDataLogger(
        connection_type='tcp',
        host=HOST,
        port=PORT
    )
    
    # Try to connect
    if logger.connect():
        print("✓ Connection successful!\n")
        
        # List available tables
        print("Available data tables:")
        tables = logger.list_tables()
        for table in tables:
            print(f"  - {table}")
        
        # Get Table1 definition
        print("\nTable1 structure:")
        definition = logger.get_table_definition('Table1')
        if definition:
            for field in definition.get('fields', []):
                print(f"  {field['name']:15} {field['type']:10} {field['units']}")
        
        # Collect recent data
        print("\nCollecting last 5 records...")
        data = logger.collect_data('Table1', records=5)
        if data:
            print(f"Retrieved {len(data)} records")
            for i, record in enumerate(data, 1):
                print(f"\nRecord {i}:")
                for key, value in record.items():
                    print(f"  {key}: {value}")
        
        # Download to file
        print("\nDownloading data to file...")
        output_file = 'CR206X_Table1_download.dat'
        if logger.download_data_to_file('Table1', output_file):
            print(f"✓ Data saved to {output_file}")
        
        # Disconnect
        logger.disconnect()
        print("\n✓ Disconnected")
        
    else:
        print("✗ Connection failed")
        print("\nTroubleshooting:")
        print("  1. Check that the IP address is correct")
        print("  2. Verify network connectivity: ping", HOST)
        print("  3. Ensure port 6785 is open (firewall)")
        print("  4. Confirm the logger is powered and online")


def example_2_serial_connection():
    """
    Example 2: Connect to CR206X via RS-232 Serial Port
    
    Use this for direct connection to the logger.
    Update the serial port to match your system.
    """
    print("\n" + "="*60)
    print("Example 2: Serial Port Connection to CR206X")
    print("="*60)
    
    # Configuration - UPDATE THESE VALUES
    SERIAL_PORT = '/dev/ttyUSB0'  # Linux: /dev/ttyUSB0, Windows: COM1, COM2, etc.
    BAUD_RATE = 9600              # Standard baud rate for CR206X
    
    print(f"Attempting connection to {SERIAL_PORT} at {BAUD_RATE} baud...")
    
    # Create logger instance
    logger = CR206XDataLogger(
        connection_type='serial',
        serial_port=SERIAL_PORT,
        baud_rate=BAUD_RATE
    )
    
    # Try to connect
    if logger.connect():
        print("✓ Serial connection successful!\n")
        
        # Download data to file
        print("Downloading data from Table1...")
        output_file = 'CR206X_serial_download.dat'
        if logger.download_data_to_file('Table1', output_file):
            print(f"✓ Data saved to {output_file}")
        
        # Disconnect
        logger.disconnect()
        print("✓ Disconnected")
        
    else:
        print("✗ Connection failed")
        print("\nTroubleshooting:")
        print("  1. Check that the serial port name is correct")
        print("  2. Verify the cable is connected (null modem cable)")
        print("  3. Ensure no other program is using the port")
        print("  4. Try different baud rates (115200, 38400, 9600)")
        print("\nNote: pyserial must be installed: pip install pyserial")


def example_3_read_dat_file():
    """
    Example 3: Read Existing .dat File
    
    This shows how to read a previously downloaded .dat file.
    This is the current method used in the biochar project.
    """
    print("\n" + "="*60)
    print("Example 3: Reading Existing .dat File")
    print("="*60)
    
    # Example file path - UPDATE THIS PATH
    dat_file = "../biochar_app/data-raw/datfiles_2023/S1T_Table1.dat"
    
    print(f"Reading file: {dat_file}")
    
    if not os.path.exists(dat_file):
        print(f"✗ File not found: {dat_file}")
        print("\nThis example requires actual data files.")
        print("You can download files using Examples 1 or 2,")
        print("or use PC400 software to create .dat files.")
        return
    
    try:
        # Read the .dat file (skip 4 Campbell Scientific header rows)
        df = pd.read_csv(
            dat_file,
            skiprows=4,  # Skip TOA5 format headers
            na_values=["", "NA", "NAN"],
            names=["timestamp", "RECORD", 
                   "VWC_1_Avg", "EC_1_Avg", "T_1_Avg",
                   "VWC_2_Avg", "EC_2_Avg", "T_2_Avg",
                   "VWC_3_Avg", "EC_3_Avg", "T_3_Avg"],
            parse_dates=["timestamp"]
        )
        
        print(f"✓ Successfully loaded {len(df)} records\n")
        
        # Display summary
        print("Data Summary:")
        print(f"  Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
        print(f"  Total records: {len(df)}")
        print(f"  Columns: {', '.join(df.columns)}")
        
        # Display first few records
        print("\nFirst 5 records:")
        print(df.head().to_string())
        
        # Basic statistics
        print("\nBasic Statistics (VWC at depth 1):")
        print(f"  Mean: {df['VWC_1_Avg'].mean():.4f} m³/m³")
        print(f"  Min:  {df['VWC_1_Avg'].min():.4f} m³/m³")
        print(f"  Max:  {df['VWC_1_Avg'].max():.4f} m³/m³")
        
    except Exception as e:
        print(f"✗ Error reading file: {e}")


def example_4_complete_workflow():
    """
    Example 4: Complete Workflow for One Logger
    
    This demonstrates the full process:
    1. Connect to logger
    2. Download data
    3. Read and process the data
    4. Display results
    """
    print("\n" + "="*60)
    print("Example 4: Complete Workflow")
    print("="*60)
    
    # Configuration
    LOGGER_NAME = "S1T"
    HOST = "192.168.1.100"  # Update with your IP
    OUTPUT_FILE = f"{LOGGER_NAME}_Table1.dat"
    
    print(f"Complete workflow for logger: {LOGGER_NAME}\n")
    
    # Step 1: Connect and download
    print("Step 1: Connecting to logger...")
    logger = CR206XDataLogger(connection_type='tcp', host=HOST)
    
    if logger.connect():
        print("✓ Connected\n")
        
        print("Step 2: Downloading data...")
        if logger.download_data_to_file('Table1', OUTPUT_FILE):
            print(f"✓ Downloaded to {OUTPUT_FILE}\n")
            
            logger.disconnect()
            
            # Step 3: Read the downloaded file
            print("Step 3: Reading downloaded file...")
            df = pd.read_csv(
                OUTPUT_FILE,
                skiprows=4,
                na_values=["", "NA", "NAN"],
                parse_dates=['TIMESTAMP']
            )
            print(f"✓ Loaded {len(df)} records\n")
            
            # Step 4: Process and display
            print("Step 4: Processing data...")
            print(f"Date range: {df['TIMESTAMP'].min()} to {df['TIMESTAMP'].max()}")
            print(f"\nSample data (first 3 records):")
            print(df.head(3).to_string())
            
            print("\n✓ Workflow complete!")
            
        else:
            print("✗ Download failed")
            logger.disconnect()
    else:
        print("✗ Connection failed")
        print("\nNote: This example requires actual hardware.")
        print("See examples 1 and 2 for troubleshooting tips.")


def main():
    """Main function - run all examples"""
    print("\n" + "="*70)
    print(" CR206X Data Logger Quick Start Examples")
    print("="*70)
    print("\nThese examples demonstrate how to access Campbell Scientific CR206X")
    print("data loggers used in the biochar water conservation project.")
    
    # Show menu
    print("\n" + "="*70)
    print("Available Examples:")
    print("="*70)
    print("  1. TCP/IP Connection (Recommended)")
    print("  2. Serial Port Connection")
    print("  3. Read Existing .dat File")
    print("  4. Complete Workflow")
    print("  0. Run All Examples")
    print("  Q. Quit")
    
    choice = input("\nSelect example (1-4, 0 for all, Q to quit): ").strip()
    
    if choice == '1':
        example_1_tcp_connection()
    elif choice == '2':
        example_2_serial_connection()
    elif choice == '3':
        example_3_read_dat_file()
    elif choice == '4':
        example_4_complete_workflow()
    elif choice == '0':
        example_1_tcp_connection()
        example_2_serial_connection()
        example_3_read_dat_file()
        example_4_complete_workflow()
    elif choice.upper() == 'Q':
        print("\nExiting...")
        return
    else:
        print(f"\nInvalid choice: {choice}")
    
    print("\n" + "="*70)
    print("For more information, see:")
    print("  - biochar_app/cr206x_data_access.py (main module)")
    print("  - biochar_app/markdown/cr206x_access_guide.md (detailed guide)")
    print("  - Campbell Scientific documentation: https://www.campbellsci.com")
    print("="*70 + "\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user. Exiting...")
        sys.exit(0)
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
