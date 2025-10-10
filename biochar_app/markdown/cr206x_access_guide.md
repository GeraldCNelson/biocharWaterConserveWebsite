# CR206X Data Logger Access Guide

## Overview

This guide explains how to programmatically access data tables on Campbell Scientific CR206X data loggers used in the biochar water conservation project. The CR206X stores sensor data in tables that can be accessed through various communication methods.

## Data Logger Configuration

### Hardware Setup
- **Model**: Campbell Scientific CR206X
- **Memory**: 512 KB
- **Data Collection**: Every 15 minutes
- **Tables**: Table1 (main data), Status, Public
- **Communication**: RS-232 port, RF radio (910-918 MHz)

### Data Table Structure

#### Table1 (Main Data Table)
The primary data table contains sensor readings at three depths (6", 12", 18"):

| Field Name | Data Type | Units | Description |
|------------|-----------|-------|-------------|
| TIMESTAMP | datetime | - | Date and time of reading |
| RECORD | integer | - | Sequential record number |
| VWC_1_Avg | float | m³/m³ | Volumetric water content at depth 1 (6") |
| EC_1_Avg | float | dS/m | Electrical conductivity at depth 1 |
| T_1_Avg | float | °C | Temperature at depth 1 |
| VWC_2_Avg | float | m³/m³ | Volumetric water content at depth 2 (12") |
| EC_2_Avg | float | dS/m | Electrical conductivity at depth 2 |
| T_2_Avg | float | °C | Temperature at depth 2 |
| VWC_3_Avg | float | m³/m³ | Volumetric water content at depth 3 (18") |
| EC_3_Avg | float | dS/m | Electrical conductivity at depth 3 |
| T_3_Avg | float | °C | Temperature at depth 3 |

## Communication Methods

### Method 1: TCP/IP Network Access (Recommended)

This is the recommended method for the biochar project since loggers communicate via RF to a base station with network connectivity.

**Connection Flow:**
```
CR206X Logger → RF Radio → Base Station (CR800) → Network Interface → Starlink → Internet → Your PC
```

**Python Example:**
```python
from biochar_app.cr206x_data_access import CR206XDataLogger

# Connect via TCP/IP
logger = CR206XDataLogger(
    connection_type='tcp',
    host='2001:db8::1',  # IPv6 address from Starlink
    port=6785            # PakBus TCP port
)

if logger.connect():
    # List available tables
    tables = logger.list_tables()
    print(f"Available tables: {tables}")
    
    # Get table definition
    definition = logger.get_table_definition('Table1')
    
    # Collect recent data (last 100 records)
    data = logger.collect_data('Table1', records=100)
    
    # Download to file
    logger.download_data_to_file('Table1', 'S1T_Table1.dat')
    
    logger.disconnect()
```

### Method 2: RS-232 Serial Connection

Used for direct connection when programming the logger or when network is unavailable.

**Python Example:**
```python
from biochar_app.cr206x_data_access import CR206XDataLogger

# Connect via serial port
logger = CR206XDataLogger(
    connection_type='serial',
    serial_port='/dev/ttyUSB0',  # On Windows: 'COM1', 'COM2', etc.
    baud_rate=9600
)

if logger.connect():
    logger.download_data_to_file('Table1', 'datalogger_output.dat')
    logger.disconnect()
```

**Serial Connection Settings:**
- Baud Rate: 9600 (default)
- Data Bits: 8
- Parity: None
- Stop Bits: 1
- Flow Control: None

### Method 3: Using PC400 Software (GUI Method)

Campbell Scientific provides PC400 software for data collection:

1. **Install PC400**: Download from https://www.campbellsci.com/pc400
2. **Configure Connection**:
   - Open PC400
   - Add new device
   - Select CR206X
   - Configure connection (TCP/IP or Serial)
   - Enter logger address and security code
3. **Download Data**:
   - Connect to logger
   - Select "Collect Data" tab
   - Choose table (Table1)
   - Set date range
   - Click "Collect"
   - Save as .dat file

## Data File Format

### TOA5 Format (Campbell Scientific Standard)

Downloaded data files use the TOA5 (Table Oriented ASCII) format:

```
"TOA5","S1T_Table1","CR206X","1234","CR206X.Std.01","CPU:CR206X.CR1"
"TIMESTAMP","RECORD","VWC_1_Avg","EC_1_Avg","T_1_Avg","VWC_2_Avg","EC_2_Avg","T_2_Avg","VWC_3_Avg","EC_3_Avg","T_3_Avg"
"TS","RN","m³/m³","dS/m","°C","m³/m³","dS/m","°C","m³/m³","dS/m","°C"
"","","Avg","Avg","Avg","Avg","Avg","Avg","Avg","Avg","Avg"
"2023-01-01 00:00:00",0,0.245,0.15,12.5,0.238,0.14,12.3,0.241,0.15,12.1
"2023-01-01 00:15:00",1,0.246,0.15,12.6,0.239,0.14,12.4,0.242,0.15,12.2
...
```

**Header Rows:**
1. File metadata (environment, logger type, program)
2. Column names
3. Units
4. Processing (Avg, Min, Max, etc.)

**Data Rows:**
- Start from row 5
- Comma-separated values
- Timestamps in ISO format

### Reading TOA5 Files

```python
import pandas as pd

# Read TOA5 file (skip 4 header rows)
df = pd.read_csv(
    'S1T_Table1.dat',
    skiprows=4,
    na_values=["", "NA", "NAN"],
    parse_dates=['TIMESTAMP']
)

print(f"Loaded {len(df)} records")
print(df.head())
```

## PakBus Protocol

### What is PakBus?

PakBus is Campbell Scientific's proprietary packet-switched telecommunications protocol. It provides:
- Reliable data transfer
- Multiple logger communication
- Network routing
- Security features

### PakBus Configuration

Each logger has:
- **PakBus Address**: Unique identifier (default: 1)
- **Security Code**: Access control (default: 0000)
- **Port**: TCP port 6785 for network access

### Using pycampbellcr1000

```python
# Install the library
# pip install pycampbellcr1000

from pycampbellcr1000 import CR1000
from datetime import datetime, timedelta

# Connect using PakBus
device = CR1000.from_url('tcp:192.168.1.100:6785')

# Get data from last 24 hours
end_date = datetime.now()
start_date = end_date - timedelta(days=1)

data = device.get_data('Table1', start_date, end_date)

print(f"Retrieved {len(data)} records")
for record in data[:5]:
    print(record)

device.bye()
```

## Network Configuration

### IPv6 Setup (Starlink)

The biochar project uses Starlink with IPv6:

```python
# IPv6 address format
logger = CR206XDataLogger(
    connection_type='tcp',
    host='2001:db8:85a3::8a2e:370:7334',  # Example IPv6
    port=6785
)
```

### IPv4 Setup (Local Network)

```python
# IPv4 address format
logger = CR206XDataLogger(
    connection_type='tcp',
    host='192.168.1.100',
    port=6785
)
```

### Port Forwarding

If accessing remotely:
1. Configure router to forward port 6785
2. Set up firewall rules
3. Use static IP or dynamic DNS

## Troubleshooting

### Connection Issues

**Problem**: Cannot connect to logger via TCP/IP
- Check network connectivity: `ping <logger-ip>`
- Verify port is open: `telnet <logger-ip> 6785`
- Check firewall settings
- Verify logger network interface is working
- Confirm IPv6 configuration if using Starlink

**Problem**: Serial connection fails
- Check cable connection (null modem vs straight-through)
- Verify COM port/device name
- Check baud rate matches logger configuration
- Test with terminal program (PuTTY, Tera Term)

**Problem**: No data returned
- Verify table name (case-sensitive)
- Check date range (logger may not have data for requested period)
- Ensure logger has collected data (check status table)
- Verify memory not full (old data may be overwritten)

### Data Quality Issues

**Problem**: Missing or null values
- Check sensor connections
- Verify sensor is working (check other fields)
- Look for power issues (check battery voltage)

**Problem**: Unrealistic values
- Values > 999999 are error indicators
- Check sensor calibration
- Verify wiring is correct

## Integration with Existing Code

The biochar project currently uses downloaded .dat files. The `cr206x_data_access.py` module can be integrated:

### Option 1: Automated Download Script

```python
from biochar_app.cr206x_data_access import CR206XDataLogger
import schedule
import time

def download_logger_data(logger_name, host):
    """Download data from one logger"""
    logger = CR206XDataLogger(connection_type='tcp', host=host)
    if logger.connect():
        output_file = f"data-raw/datfiles_{datetime.now().year}/{logger_name}_Table1.dat"
        logger.download_data_to_file('Table1', output_file)
        logger.disconnect()

# Schedule daily downloads
schedule.every().day.at("02:00").do(
    download_logger_data, 
    logger_name="S1T",
    host="2001:db8::1"
)

while True:
    schedule.run_pending()
    time.sleep(60)
```

### Option 2: Direct Integration

Modify `process_data.py` to fetch data directly:

```python
def read_logger_data(name, year):
    """Read data directly from logger or from file"""
    
    # Try to read from file first
    filepath = os.path.join(DATA_RAW_DIR, f"datfiles_{year}/{name}_Table1.dat")
    if os.path.exists(filepath):
        # Existing file reading code
        df = pd.read_csv(filepath, skiprows=4, ...)
        return df
    
    # If file doesn't exist, try to fetch from logger
    logger_config = get_logger_config(name)  # Get IP for this logger
    logger = CR206XDataLogger(connection_type='tcp', host=logger_config['host'])
    
    if logger.connect():
        # Download to file then read it
        logger.download_data_to_file('Table1', filepath)
        logger.disconnect()
        df = pd.read_csv(filepath, skiprows=4, ...)
        return df
    
    return None
```

## Additional Resources

### Campbell Scientific Documentation
- **CR206X Manual**: https://www.campbellsci.com/cr206x
- **PakBus Protocol**: https://www.campbellsci.com/pakbus
- **PC400 Software**: https://www.campbellsci.com/pc400
- **LoggerNet**: https://www.campbellsci.com/loggernet
- **Support Forum**: https://www.campbellsci.com/forum

### Python Libraries
- **pyserial**: Serial port communication - https://pyserial.readthedocs.io/
- **pycampbellcr1000**: Campbell logger interface - https://github.com/StefanUlbrich/pycampbellcr1000
- **pandas**: Data processing - https://pandas.pydata.org/

### Related Files in This Project
- `process_data.py`: Main data processing script
- `process_data_2023.py`: 2023-specific data processing
- `techDetails.md`: Technical documentation for the project
- `config.py`: Configuration constants

## Security Considerations

1. **Network Security**:
   - Use VPN for remote access
   - Change default security codes
   - Implement firewall rules
   - Use secure protocols when available

2. **Data Integrity**:
   - Verify checksums for downloaded data
   - Keep backups of original .dat files
   - Log all data downloads

3. **Access Control**:
   - Restrict logger access to authorized users
   - Use separate security codes for different users
   - Monitor access logs

## Example: Complete Workflow

```python
from biochar_app.cr206x_data_access import CR206XDataLogger
from datetime import datetime, timedelta
import pandas as pd

def collect_all_loggers():
    """Collect data from all 12 loggers in the biochar project"""
    
    # Logger names and IP addresses
    loggers = {
        'S1T': '2001:db8::1',
        'S1M': '2001:db8::2',
        'S1B': '2001:db8::3',
        'S2T': '2001:db8::4',
        'S2M': '2001:db8::5',
        'S2B': '2001:db8::6',
        'S3T': '2001:db8::7',
        'S3M': '2001:db8::8',
        'S3B': '2001:db8::9',
        'S4T': '2001:db8::10',
        'S4M': '2001:db8::11',
        'S4B': '2001:db8::12',
    }
    
    year = datetime.now().year
    
    for logger_name, ip_address in loggers.items():
        print(f"Collecting data from {logger_name}...")
        
        logger = CR206XDataLogger(
            connection_type='tcp',
            host=ip_address,
            port=6785
        )
        
        if logger.connect():
            output_file = f"data-raw/datfiles_{year}/{logger_name}_Table1.dat"
            success = logger.download_data_to_file('Table1', output_file)
            
            if success:
                print(f"  ✓ Downloaded to {output_file}")
            else:
                print(f"  ✗ Failed to download")
            
            logger.disconnect()
        else:
            print(f"  ✗ Connection failed")

if __name__ == "__main__":
    collect_all_loggers()
```

---

**Last Updated**: 2025-10-10
**Author**: Biochar Water Conservation Project
**Contact**: See project repository for support
