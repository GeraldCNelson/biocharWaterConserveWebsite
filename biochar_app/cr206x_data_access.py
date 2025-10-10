"""
CR206X Data Logger Access Module

This module provides examples and utilities for accessing data tables on a 
Campbell Scientific CR206X data logger.

The CR206X supports multiple communication methods:
1. RS-232 serial communication (direct connection)
2. Network/IP communication (via NL121, NL201, or similar network interfaces)
3. RF communication (through spread spectrum radio)

Data Access Methods:
- PakBus protocol (Campbell Scientific proprietary protocol)
- TCP/IP socket communication
- Serial port communication

Requirements:
    pip install pyserial pycampbellcr1000

Campbell Scientific Resources:
- CR206X Manual: https://www.campbellsci.com/cr206x
- PakBus Documentation: https://www.campbellsci.com/pakbus
- PC400 Software: https://www.campbellsci.com/pc400
"""

import logging
from typing import Optional, Dict, List, Union
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class CR206XDataLogger:
    """
    Interface for Campbell Scientific CR206X Data Logger.
    
    This class demonstrates how to connect to and retrieve data from a CR206X
    data logger using different communication methods.
    
    Attributes:
        connection_type (str): Type of connection ('serial', 'tcp', or 'pakbus')
        host (str): IP address or hostname for network connections
        port (int): Port number for network connections
        serial_port (str): Serial port for direct connections (e.g., 'COM1' or '/dev/ttyUSB0')
        baud_rate (int): Baud rate for serial communication (default 9600)
    """
    
    def __init__(self, 
                 connection_type: str = 'tcp',
                 host: Optional[str] = None,
                 port: int = 6785,
                 serial_port: Optional[str] = None,
                 baud_rate: int = 9600,
                 pakbus_address: int = 1,
                 security_code: int = 0000):
        """
        Initialize CR206X data logger connection parameters.
        
        Args:
            connection_type: 'serial', 'tcp', or 'pakbus'
            host: IP address for network connections
            port: Port number (default 6785 for PakBus TCP)
            serial_port: Serial port name for direct connections
            baud_rate: Baud rate for serial (default 9600)
            pakbus_address: PakBus address of the data logger (default 1)
            security_code: Security code if set on logger (default 0000)
        """
        self.connection_type = connection_type
        self.host = host
        self.port = port
        self.serial_port = serial_port
        self.baud_rate = baud_rate
        self.pakbus_address = pakbus_address
        self.security_code = security_code
        self.connection = None
        
    def connect_serial(self) -> bool:
        """
        Connect to CR206X via RS-232 serial port.
        
        Returns:
            bool: True if connection successful
            
        Example:
            >>> logger = CR206XDataLogger(connection_type='serial', serial_port='/dev/ttyUSB0')
            >>> logger.connect_serial()
        """
        try:
            import serial
            
            logger.info(f"Connecting to CR206X via serial port {self.serial_port} at {self.baud_rate} baud")
            self.connection = serial.Serial(
                port=self.serial_port,
                baudrate=self.baud_rate,
                bytesize=serial.EIGHTBITS,
                parity=serial.PARITY_NONE,
                stopbits=serial.STOPBITS_ONE,
                timeout=5
            )
            logger.info("Serial connection established")
            return True
            
        except ImportError:
            logger.error("pyserial not installed. Install with: pip install pyserial")
            return False
        except Exception as e:
            logger.error(f"Failed to connect via serial: {e}")
            return False
    
    def connect_tcp(self) -> bool:
        """
        Connect to CR206X via TCP/IP network.
        
        This method is used when the logger is connected through a network interface
        like NL121, NL201, or directly via Ethernet.
        
        Returns:
            bool: True if connection successful
            
        Example:
            >>> logger = CR206XDataLogger(connection_type='tcp', host='192.168.1.100')
            >>> logger.connect_tcp()
        """
        try:
            import socket
            
            logger.info(f"Connecting to CR206X at {self.host}:{self.port}")
            self.connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.connection.settimeout(10)
            self.connection.connect((self.host, self.port))
            logger.info("TCP connection established")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect via TCP: {e}")
            return False
    
    def connect_pakbus(self) -> bool:
        """
        Connect to CR206X using PakBus protocol.
        
        PakBus is Campbell Scientific's proprietary communication protocol.
        This method requires the pycampbellcr1000 library.
        
        Returns:
            bool: True if connection successful
            
        Example:
            >>> logger = CR206XDataLogger(connection_type='pakbus', host='192.168.1.100')
            >>> logger.connect_pakbus()
        """
        try:
            from pycampbellcr1000 import CR1000
            
            logger.info(f"Connecting to CR206X via PakBus at {self.host}:{self.port}")
            # Note: CR206X uses similar protocol to CR1000
            self.connection = CR1000.from_url(f'tcp:{self.host}:{self.port}')
            self.connection.pakbus_address = self.pakbus_address
            logger.info("PakBus connection established")
            return True
            
        except ImportError:
            logger.error("pycampbellcr1000 not installed. Install with: pip install pycampbellcr1000")
            logger.info("Alternative: Use Campbell Scientific's LoggerNet SDK or PC400 software")
            return False
        except Exception as e:
            logger.error(f"Failed to connect via PakBus: {e}")
            return False
    
    def connect(self) -> bool:
        """
        Establish connection using the configured connection type.
        
        Returns:
            bool: True if connection successful
        """
        if self.connection_type == 'serial':
            return self.connect_serial()
        elif self.connection_type == 'tcp':
            return self.connect_tcp()
        elif self.connection_type == 'pakbus':
            return self.connect_pakbus()
        else:
            logger.error(f"Unknown connection type: {self.connection_type}")
            return False
    
    def disconnect(self):
        """Close the connection to the data logger."""
        if self.connection:
            try:
                if hasattr(self.connection, 'close'):
                    self.connection.close()
                logger.info("Disconnected from CR206X")
            except Exception as e:
                logger.error(f"Error disconnecting: {e}")
    
    def list_tables(self) -> List[str]:
        """
        List all data tables available on the CR206X.
        
        CR206X typically has tables like:
        - Table1: Main data table with sensor readings
        - Status: Logger status information
        - Public: Public variables
        
        Returns:
            List of table names
            
        Example:
            >>> logger = CR206XDataLogger(connection_type='tcp', host='192.168.1.100')
            >>> logger.connect()
            >>> tables = logger.list_tables()
            >>> print(tables)
            ['Table1', 'Status', 'Public']
        """
        if not self.connection:
            logger.error("Not connected to data logger")
            return []
        
        try:
            if self.connection_type == 'pakbus':
                # Using pycampbellcr1000 library
                tables = self.connection.list_tables()
                logger.info(f"Found tables: {tables}")
                return tables
            else:
                logger.warning("list_tables() requires PakBus connection")
                # For this project, we know the typical table structure
                return ['Table1', 'Status', 'Public']
        except Exception as e:
            logger.error(f"Error listing tables: {e}")
            return []
    
    def get_table_definition(self, table_name: str) -> Dict:
        """
        Get the structure/definition of a specific data table.
        
        Args:
            table_name: Name of the table (e.g., 'Table1')
            
        Returns:
            Dictionary containing table structure information
            
        Example:
            >>> logger = CR206XDataLogger(connection_type='tcp', host='192.168.1.100')
            >>> logger.connect()
            >>> definition = logger.get_table_definition('Table1')
        """
        if not self.connection:
            logger.error("Not connected to data logger")
            return {}
        
        try:
            if self.connection_type == 'pakbus':
                definition = self.connection.getfile(table_name)
                logger.info(f"Retrieved definition for {table_name}")
                return definition
            else:
                # For this biochar project, Table1 has known structure
                if table_name == 'Table1':
                    return {
                        'name': 'Table1',
                        'fields': [
                            {'name': 'TIMESTAMP', 'type': 'datetime', 'units': ''},
                            {'name': 'RECORD', 'type': 'int', 'units': ''},
                            {'name': 'VWC_1_Avg', 'type': 'float', 'units': 'm³/m³'},
                            {'name': 'EC_1_Avg', 'type': 'float', 'units': 'dS/m'},
                            {'name': 'T_1_Avg', 'type': 'float', 'units': '°C'},
                            {'name': 'VWC_2_Avg', 'type': 'float', 'units': 'm³/m³'},
                            {'name': 'EC_2_Avg', 'type': 'float', 'units': 'dS/m'},
                            {'name': 'T_2_Avg', 'type': 'float', 'units': '°C'},
                            {'name': 'VWC_3_Avg', 'type': 'float', 'units': 'm³/m³'},
                            {'name': 'EC_3_Avg', 'type': 'float', 'units': 'dS/m'},
                            {'name': 'T_3_Avg', 'type': 'float', 'units': '°C'},
                        ],
                        'interval': '15min',
                        'description': 'Main data table with VWC, EC, and Temperature at 3 depths'
                    }
                return {}
        except Exception as e:
            logger.error(f"Error getting table definition: {e}")
            return {}
    
    def collect_data(self, 
                     table_name: str = 'Table1',
                     start_date: Optional[datetime] = None,
                     end_date: Optional[datetime] = None,
                     records: int = 100) -> List[Dict]:
        """
        Collect data from a specific table on the CR206X.
        
        Args:
            table_name: Name of the table to query (default 'Table1')
            start_date: Start date for data collection (optional)
            end_date: End date for data collection (optional)
            records: Number of most recent records to retrieve (default 100)
            
        Returns:
            List of dictionaries containing the data records
            
        Example:
            >>> logger = CR206XDataLogger(connection_type='tcp', host='192.168.1.100')
            >>> logger.connect()
            >>> data = logger.collect_data('Table1', records=50)
            >>> print(f"Retrieved {len(data)} records")
        """
        if not self.connection:
            logger.error("Not connected to data logger")
            return []
        
        try:
            if self.connection_type == 'pakbus':
                # Using pycampbellcr1000 to collect data
                data = self.connection.get_data(
                    table_name,
                    start_date=start_date,
                    stop_date=end_date
                )
                logger.info(f"Retrieved {len(data)} records from {table_name}")
                return data
            else:
                logger.warning("Direct data collection requires PakBus protocol")
                logger.info("Use PC400 software or export .dat files for this connection type")
                return []
                
        except Exception as e:
            logger.error(f"Error collecting data: {e}")
            return []
    
    def download_data_to_file(self, 
                              table_name: str = 'Table1',
                              output_file: str = 'datalogger_output.dat',
                              file_format: str = 'TOA5') -> bool:
        """
        Download data from logger and save to a file.
        
        Args:
            table_name: Name of the table to download
            output_file: Output filename
            file_format: Format ('TOA5', 'CSV', or 'TOB1')
            
        Returns:
            bool: True if successful
            
        Example:
            >>> logger = CR206XDataLogger(connection_type='tcp', host='192.168.1.100')
            >>> logger.connect()
            >>> logger.download_data_to_file('Table1', 'CR206X_data.dat')
        """
        if not self.connection:
            logger.error("Not connected to data logger")
            return False
        
        try:
            data = self.collect_data(table_name)
            if not data:
                logger.warning("No data retrieved")
                return False
            
            # Write data to file
            with open(output_file, 'w') as f:
                if file_format == 'TOA5':
                    # Campbell Scientific TOA5 format (ASCII table-oriented)
                    # This matches the format used in the biochar project
                    f.write(f'"TOA5","{table_name}","CR206X","1234","CR206X.Std.01","CPU:CR206X.CR1"\n')
                    f.write('"TIMESTAMP","RECORD","VWC_1_Avg","EC_1_Avg","T_1_Avg","VWC_2_Avg","EC_2_Avg","T_2_Avg","VWC_3_Avg","EC_3_Avg","T_3_Avg"\n')
                    f.write('"TS","RN","m³/m³","dS/m","°C","m³/m³","dS/m","°C","m³/m³","dS/m","°C"\n')
                    f.write('"","","Avg","Avg","Avg","Avg","Avg","Avg","Avg","Avg","Avg"\n')
                    
                    for record in data:
                        # Format and write each record
                        f.write(f'"{record.get("TIMESTAMP","")}"')
                        for field in ['RECORD', 'VWC_1_Avg', 'EC_1_Avg', 'T_1_Avg', 
                                     'VWC_2_Avg', 'EC_2_Avg', 'T_2_Avg',
                                     'VWC_3_Avg', 'EC_3_Avg', 'T_3_Avg']:
                            f.write(f',{record.get(field, "")}')
                        f.write('\n')
                else:
                    # Simple CSV format
                    import csv
                    if data:
                        writer = csv.DictWriter(f, fieldnames=data[0].keys())
                        writer.writeheader()
                        writer.writerows(data)
            
            logger.info(f"Data saved to {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"Error downloading data to file: {e}")
            return False


# Example usage functions

def example_tcp_connection():
    """
    Example: Connect to CR206X via TCP/IP and retrieve data.
    
    This is the recommended method for this biochar project since the loggers
    are connected via RF to a base station with network connectivity.
    """
    print("\n=== Example: TCP/IP Connection ===")
    
    # Initialize connection - use the actual IPv6 or IPv4 address of your base station
    logger = CR206XDataLogger(
        connection_type='tcp',
        host='192.168.1.100',  # Replace with your base station IP
        port=6785  # PakBus TCP port
    )
    
    # Connect to the logger
    if logger.connect():
        print("✓ Connected successfully")
        
        # List available tables
        tables = logger.list_tables()
        print(f"Available tables: {tables}")
        
        # Get table definition
        definition = logger.get_table_definition('Table1')
        print(f"Table structure: {definition}")
        
        # Collect data
        data = logger.collect_data('Table1', records=10)
        print(f"Retrieved {len(data)} records")
        
        # Download to file
        logger.download_data_to_file('Table1', 'CR206X_Table1.dat')
        
        # Disconnect
        logger.disconnect()
    else:
        print("✗ Connection failed")


def example_serial_connection():
    """
    Example: Connect to CR206X via RS-232 serial port.
    
    This method is used for direct connection when programming the logger
    or when network access is not available.
    """
    print("\n=== Example: Serial Port Connection ===")
    
    # Initialize serial connection
    logger = CR206XDataLogger(
        connection_type='serial',
        serial_port='/dev/ttyUSB0',  # On Windows: 'COM1', 'COM2', etc.
        baud_rate=9600
    )
    
    # Connect to the logger
    if logger.connect():
        print("✓ Connected via serial port")
        
        # Download data
        logger.download_data_to_file('Table1', 'CR206X_serial_data.dat')
        
        # Disconnect
        logger.disconnect()
    else:
        print("✗ Serial connection failed")


def example_read_existing_dat_file():
    """
    Example: Read data from an existing .dat file.
    
    This demonstrates how the current biochar project reads downloaded data files.
    """
    print("\n=== Example: Reading Existing .dat File ===")
    
    import pandas as pd
    import os
    
    # This matches the pattern used in process_data.py and process_data_2023.py
    dat_file = "data-raw/datfiles_2023/S1T_Table1.dat"
    
    if os.path.exists(dat_file):
        # Read the .dat file (TOA5 format has 4 header rows)
        df = pd.read_csv(
            dat_file,
            skiprows=4,  # Skip Campbell Scientific metadata rows
            na_values=["", "NA", "NAN"],
            names=["timestamp", "RECORD", 
                   "VWC_1_Avg", "EC_1_Avg", "T_1_Avg",
                   "VWC_2_Avg", "EC_2_Avg", "T_2_Avg", 
                   "VWC_3_Avg", "EC_3_Avg", "T_3_Avg"],
            parse_dates=["timestamp"]
        )
        
        print(f"Loaded {len(df)} records from {dat_file}")
        print(f"Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
        print(f"\nFirst few records:\n{df.head()}")
    else:
        print(f"File not found: {dat_file}")
        print("This example requires actual data files from the loggers")


if __name__ == "__main__":
    print("CR206X Data Logger Access Examples")
    print("=" * 50)
    print("\nThis module demonstrates how to access data from Campbell Scientific")
    print("CR206X data loggers used in the biochar water conservation project.")
    print("\nThree access methods are shown:")
    print("1. TCP/IP network connection (recommended for this project)")
    print("2. RS-232 serial connection (for direct access)")
    print("3. Reading existing .dat files (current project method)")
    
    # Run examples
    # Note: These require actual hardware or will demonstrate the interface
    
    print("\n" + "=" * 50)
    print("To use these examples with actual hardware:")
    print("1. Install required libraries:")
    print("   pip install pyserial pycampbellcr1000")
    print("2. Update connection parameters (IP address, serial port)")
    print("3. Ensure network/serial access to the data logger")
    
    # Demonstrate reading existing file (works without hardware)
    example_read_existing_dat_file()
