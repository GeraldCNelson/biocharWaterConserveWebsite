# CR206X Data Logger Examples

This directory contains example code for accessing Campbell Scientific CR206X data loggers used in the biochar water conservation project.

## Quick Start

```bash
# Run the interactive examples
python cr206x_quick_start.py
```

## Files

- **cr206x_quick_start.py**: Interactive examples demonstrating various connection methods and data access patterns

## What You'll Learn

1. **TCP/IP Connection**: Connect to loggers over network (recommended method)
2. **Serial Connection**: Direct RS-232 connection for programming/troubleshooting
3. **Reading .dat Files**: Parse downloaded data files in Campbell Scientific format
4. **Complete Workflow**: End-to-end example from connection to data processing

## Requirements

```bash
# Basic requirements (for reading .dat files)
pip install pandas

# For TCP/IP and PakBus access
pip install pycampbellcr1000

# For serial port access
pip install pyserial
```

## Configuration

Before running the examples, update the following values in the scripts:

### For TCP/IP Connection
```python
HOST = '192.168.1.100'  # Update with your base station IP
PORT = 6785             # PakBus TCP port (standard)
```

### For Serial Connection
```python
SERIAL_PORT = '/dev/ttyUSB0'  # Linux: /dev/ttyUSB0, Windows: COM1, COM2, etc.
BAUD_RATE = 9600              # Standard for CR206X
```

## Data Table Structure

The CR206X loggers in this project collect data at 15-minute intervals with the following structure:

| Field | Type | Units | Description |
|-------|------|-------|-------------|
| TIMESTAMP | datetime | - | Date and time of reading |
| RECORD | int | - | Sequential record number |
| VWC_1_Avg | float | m³/m³ | Volumetric water content at 6" |
| EC_1_Avg | float | dS/m | Electrical conductivity at 6" |
| T_1_Avg | float | °C | Temperature at 6" |
| VWC_2_Avg | float | m³/m³ | Volumetric water content at 12" |
| EC_2_Avg | float | dS/m | Electrical conductivity at 12" |
| T_2_Avg | float | °C | Temperature at 12" |
| VWC_3_Avg | float | m³/m³ | Volumetric water content at 18" |
| EC_3_Avg | float | dS/m | Electrical conductivity at 18" |
| T_3_Avg | float | °C | Temperature at 18" |

## Additional Resources

- **Main Module**: `../biochar_app/cr206x_data_access.py`
- **Detailed Guide**: `../biochar_app/markdown/cr206x_access_guide.md`
- **Campbell Scientific**: https://www.campbellsci.com/cr206x
- **PakBus Protocol**: https://www.campbellsci.com/pakbus

## Troubleshooting

### Connection Issues

**TCP/IP Connection Fails**
- Verify network connectivity: `ping <host>`
- Check firewall settings (port 6785)
- Confirm logger is powered and online
- For IPv6 (Starlink), use full IPv6 address

**Serial Connection Fails**
- Check cable type (null modem vs straight-through)
- Verify port name is correct
- Ensure no other program is using the port
- Try different baud rates

### Data Issues

**No Data Returned**
- Verify table name (case-sensitive: 'Table1')
- Check if logger has data for requested date range
- Ensure logger memory is not full
- Check logger status table

**Invalid Values**
- Values > 999999 are error indicators
- Check sensor connections
- Verify sensor calibration

## Support

For issues specific to this project, see the main repository documentation.

For Campbell Scientific hardware/software support:
- Support Forum: https://www.campbellsci.com/forum
- Technical Support: https://www.campbellsci.com/support
