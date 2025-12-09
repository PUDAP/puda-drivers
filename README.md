# puda-drivers

Hardware drivers for the PUDA (Physical Unified Device Architecture) platform. This package provides Python interfaces for controlling laboratory automation equipment.

## Features

- **Gantry Control**: Control G-code compatible motion systems (e.g., QuBot)
- **Liquid Handling**: Interface with Sartorius rLINE® pipettes and dispensers
- **Serial Communication**: Robust serial port management with automatic reconnection
- **Cross-platform**: Works on Linux, macOS, and Windows

## Installation

### From PyPI

```bash
pip install puda-drivers
```

### From Source

```bash
git clone https://github.com/zhao-bears/puda-drivers.git
cd puda-drivers
pip install -e .
```

## Quick Start

### Gantry Control (GCode)

```python
from puda_drivers.move import GCodeController

# Initialize and connect to a G-code device
gantry = GCodeController(port_name="/dev/ttyACM0", feed=3000)
gantry.connect()

# Home the gantry
gantry.home()

# Move to absolute position
gantry.move_absolute(x=50.0, y=-100.0, z=-10.0)

# Move relative to current position
gantry.move_relative(x=20.0, y=-10.0)

# Query current position
position = gantry.query_position()
print(f"Current position: {position}")

# Disconnect when done
gantry.disconnect()
```

### Liquid Handling (Sartorius)

```python
from puda_drivers.transfer.liquid.sartorius import SartoriusController

# Initialize and connect to pipette
pipette = SartoriusController(port_name="/dev/ttyUSB0")
pipette.connect()
pipette.initialize()

# Attach tip
pipette.attach_tip()

# Aspirate liquid
pipette.aspirate(amount=50.0)  # 50 µL

# Dispense liquid
pipette.dispense(amount=50.0)

# Eject tip
pipette.eject_tip()

# Disconnect when done
pipette.disconnect()
```

### Combined Workflow

```python
from puda_drivers.move import GCodeController
from puda_drivers.transfer.liquid.sartorius import SartoriusController

# Initialize both devices
gantry = GCodeController(port_name="/dev/ttyACM0")
pipette = SartoriusController(port_name="/dev/ttyUSB0")

gantry.connect()
pipette.connect()

# Move to source well
gantry.move_absolute(x=50.0, y=-50.0, z=-20.0)
pipette.aspirate(amount=50.0)

# Move to destination well
gantry.move_absolute(x=150.0, y=-150.0, z=-20.0)
pipette.dispense(amount=50.0)

# Cleanup
pipette.eject_tip()
gantry.disconnect()
pipette.disconnect()
```

## Device Support

### Motion Systems

- **QuBot** (GCode) - Multi-axis gantry systems compatible with G-code commands
  - Supports X, Y, Z, and A axes
  - Configurable feed rates
  - Position synchronization and homing

### Liquid Handling

- **Sartorius rLINE®** - Electronic pipettes and robotic dispensers
  - Aspirate and dispense operations
  - Tip attachment and ejection
  - Configurable speeds and volumes

## Finding Serial Ports

To discover available serial ports on your system:

```python
from puda_drivers.core import list_serial_ports

# List all available ports
ports = list_serial_ports()
for port, desc, hwid in ports:
    print(f"{port}: {desc} [{hwid}]")

# Filter ports by description
sartorius_ports = list_serial_ports(filter_desc="Sartorius")
```

## Requirements

- Python >= 3.14
- pyserial >= 3.5
- See `pyproject.toml` for full dependency list

## Development

### Setup Development Environment

```bash
# Create virtual environment
uv venv

# Activate virtual environment
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
uv sync

# Install package in editable mode
pip install -e .
```

### Building and Publishing

```bash
# Build distribution packages
uv build

# Publish to PyPI
uv publish
# Username: __token__
# Password: <your PyPI API token>
```

### Version Management

```bash
# Set version explicitly
uv version 0.0.1

# Bump version (e.g., 1.2.3 -> 1.3.0)
uv bump minor
```

## Documentation

- [PyPI Package](https://pypi.org/project/puda-drivers/)
- [GitHub Repository](https://github.com/zhao-bears/puda-drivers)
- [Issue Tracker](https://github.com/zhao-bears/puda-drivers/issues)

## License

MIT License - see LICENSE file for details.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request on GitHub.
