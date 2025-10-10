# biocharWaterConserveWebsite

Code to create a website that displays data from a research project to examine the water-holding capacity of biochar in a pasture field in Western Colorado.

## CR206X Data Logger Access

This repository includes tools for accessing data from Campbell Scientific CR206X data loggers used in the field study.

### Quick Start

```bash
# Run interactive examples
cd examples
python cr206x_quick_start.py
```

### Documentation

- **[CR206X Access Guide](biochar_app/markdown/cr206x_access_guide.md)** - Comprehensive guide for accessing CR206X data loggers
- **[CR206X Module](biochar_app/cr206x_data_access.py)** - Python module with connection and data retrieval classes
- **[Examples](examples/)** - Ready-to-run example scripts

### Features

- **Multiple Connection Methods**: TCP/IP network, RS-232 serial, or file-based access
- **Data Table Access**: Query and download data tables from loggers
- **TOA5 Format Support**: Read Campbell Scientific data file format
- **PakBus Protocol**: Support for Campbell Scientific's communication protocol
- **Complete Examples**: Working code for all common use cases

### Connection Methods

1. **TCP/IP Network** (Recommended for remote access)
   ```python
   from biochar_app.cr206x_data_access import CR206XDataLogger
   
   logger = CR206XDataLogger(connection_type='tcp', host='192.168.1.100')
   logger.connect()
   logger.download_data_to_file('Table1', 'output.dat')
   logger.disconnect()
   ```

2. **RS-232 Serial** (For direct connection)
   ```python
   logger = CR206XDataLogger(connection_type='serial', serial_port='/dev/ttyUSB0')
   logger.connect()
   logger.download_data_to_file('Table1', 'output.dat')
   logger.disconnect()
   ```

3. **Read Existing Files** (Current project method)
   ```python
   import pandas as pd
   df = pd.read_csv('S1T_Table1.dat', skiprows=4, parse_dates=['TIMESTAMP'])
   ```

### Requirements

```bash
pip install pandas
pip install pyserial  # For serial connections
pip install pycampbellcr1000  # For PakBus protocol
```

See the [examples directory](examples/) for complete working code.
