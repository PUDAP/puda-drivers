"""
First machine class containing Deck, GCodeController, and SartoriusController.

This class demonstrates the integration of:
- GCodeController: Handles motion control (hardware-specific)
- Deck: Manages labware layout (configuration-agnostic)
- SartoriusController: Handles liquid handling operations
"""

from typing import Optional, Dict, Tuple
from puda_drivers.move import GCodeController, Deck
from puda_drivers.transfer.liquid.sartorius import SartoriusController


class First:
    """
    First machine class integrating motion control, deck management, and liquid handling.
    
    The deck has 16 slots arranged in a 4x4 grid (A1-D4).
    Each slot's origin location is stored for absolute movement calculation.
    """
    
    # Default configuration values
    DEFAULT_QUBOT_PORT = "/dev/ttyACM0"
    DEFAULT_QUBOT_BAUDRATE = 9600
    DEFAULT_QUBOT_FEEDRATE = 3000
    
    DEFAULT_SARTORIUS_PORT = "/dev/ttyUSB0"
    DEFAULT_SARTORIUS_BAUDRATE = 9600
    
    # Default axis limits - customize based on your hardware
    DEFAULT_AXIS_LIMITS = {
        "X": (0, 330),
        "Y": (-440, 0),
        "Z": (-175, 0),
        "A": (-175, 0),
    }
    
    # 4x4 deck slot layout (A1-D4)
    SLOT_ROWS = ["A", "B", "C", "D"]
    SLOT_COLS = ["1", "2", "3", "4"]
    
    # Slot origins
    SLOT_ORIGINS = {
        "A1": (0, 0),
        "A2": (0, 100),
        "A3": (0, 200),
        "A4": (0, 300),
        "B1": (100, 0),
        "B2": (100, 100),
        "B3": (100, 200),
        "B4": (100, 300),
        "C1": (200, 0),
        "C2": (200, 100),
        "C3": (200, 200),
        "C4": (200, 300),
        "D1": (300, 0),
        "D2": (300, 100),
        "D3": (300, 200),
        "D4": (300, 300),
    }
    
    def __init__(
        self,
        qubot_port: Optional[str] = None,
        qubot_baudrate: int = DEFAULT_QUBOT_BAUDRATE,
        sartorius_port: Optional[str] = None,
        sartorius_baudrate: int = DEFAULT_SARTORIUS_BAUDRATE,
        axis_limits: Optional[Dict[str, Tuple[float, float]]] = None,
    ):
        """
        Initialize the First machine.
        
        Args:
            qubot_port: Serial port for GCodeController (e.g., '/dev/ttyACM0')
            qubot_baudrate: Baud rate for GCodeController. Defaults to 9600.
            sartorius_port: Serial port for SartoriusController (e.g., '/dev/ttyUSB0')
            sartorius_baudrate: Baud rate for SartoriusController. Defaults to 9600.
            axis_limits: Dictionary mapping axis names to (min, max) limits.
                        Defaults to DEFAULT_AXIS_LIMITS.
        """
        # Initialize controllers
        self.qubot = GCodeController(
            port_name=qubot_port or self.DEFAULT_QUBOT_PORT,
            baudrate=qubot_baudrate
        )
        self.pipette = SartoriusController(
            port_name=sartorius_port or self.DEFAULT_SARTORIUS_PORT,
            baudrate=sartorius_baudrate
        )
        self.deck = Deck()
        
        # Set axis limits
        limits = axis_limits or self.DEFAULT_AXIS_LIMITS
        for axis, (min_val, max_val) in limits.items():
            self.qubot.set_axis_limits(axis, min_val, max_val)
        
    def get_slot_origin(self, slot: str) -> Tuple[float, float, float]:
        """
        Get the origin coordinates of a slot.
        
        Args:
            slot: Slot name (e.g., 'A1', 'B2')
            
        Returns:
            Tuple of (x, y, z) coordinates for the slot origin
            
        Raises:
            KeyError: If slot name is invalid
        """
        slot = slot.upper()
        if slot not in self.SLOT_ORIGINS:
            raise KeyError(f"Invalid slot name: {slot}. Must be one of {list(self.SLOT_ORIGINS.keys())}")
        return self.SLOT_ORIGINS[slot]
    
    def calculate_absolute_position(self, slot: str, well: Optional[str] = None) -> Tuple[float, float, float]:
        """
        Calculate absolute position for a slot (and optionally a well within that slot).
        
        Args:
            slot: Slot name (e.g., 'A1', 'B2')
            well: Optional well name within the slot (e.g., 'A1' for a well in a tiprack)
            
        Returns:
            Tuple of (x, y, z) absolute coordinates
            
        Note:
            This is a placeholder - actual implementation needed.
        """
        # Get slot origin
        slot_origin = self.get_slot_origin(slot)
        
        # If well is specified, get well position relative to slot and add to slot origin
        if well:
            # TODO: Implement well position calculation
            # labware = self.deck[slot]
            # well_pos = labware.get_well_position(well)
            # return (slot_origin[0] + well_pos[0], slot_origin[1] + well_pos[1], slot_origin[2] + well_pos[2])
            pass
        
        return slot_origin
    
    def disconnect(self):
        """Disconnect all controllers."""
        self.qubot.disconnect()
        self.pipette.disconnect()

