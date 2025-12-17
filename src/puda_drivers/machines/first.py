"""
First machine class containing Deck, GCodeController, and SartoriusController.

This class demonstrates the integration of:
- GCodeController: Handles motion control (hardware-specific)
- Deck: Manages labware layout (configuration-agnostic)
- SartoriusController: Handles liquid handling operations
"""

import time
from typing import Optional, Dict, Tuple, Type
from puda_drivers.move import GCodeController, Deck
from puda_drivers.core import Position
from puda_drivers.transfer.liquid.sartorius import SartoriusController
from puda_drivers.labware import StandardLabware


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
    
    # origin position of Z and A axes
    Z_ORIGIN = Position(x=0, y=0, z=0)
    A_ORIGIN = Position(x=0, y=0, a=0)
    
    # Default axis limits - customize based on your hardware
    DEFAULT_AXIS_LIMITS = {
        "X": (0, 330),
        "Y": (-440, 0),
        "Z": (-175, 0),
        "A": (-175, 0),
    }
    
    # Slot origins (the bottom left corner of the slot relative to the deck origin)
    SLOT_ORIGINS = {
        "A1": Position(x=0, y=0),
        "A2": Position(x=0, y=100),
        "A3": Position(x=0, y=200),
        "A4": Position(x=0, y=300),
        "B1": Position(x=100, y=0),
        "B2": Position(x=100, y=100),
        "B3": Position(x=100, y=200),
        "B4": Position(x=100, y=300),
        "C1": Position(x=200, y=0),
        "C2": Position(x=200, y=100),
        "C3": Position(x=200, y=200),
        "C4": Position(x=200, y=300),
        "D1": Position(x=300, y=0),
        "D2": Position(x=300, y=100),
        "D3": Position(x=300, y=200),
        "D4": Position(x=300, y=300),
    }
    
    def __init__(
        self,
        qubot_port: Optional[str] = None,
        sartorius_port: Optional[str] = None,
        axis_limits: Optional[Dict[str, Tuple[float, float]]] = None,
    ):
        """
        Initialize the First machine.
        
        Args:
            qubot_port: Serial port for GCodeController (e.g., '/dev/ttyACM0')
            sartorius_port: Serial port for SartoriusController (e.g., '/dev/ttyUSB0')
            axis_limits: Dictionary mapping axis names to (min, max) limits.
                        Defaults to DEFAULT_AXIS_LIMITS.
        """
        # Initialize deck
        self.deck = Deck(rows=4, cols=4)

        # Initialize controllers
        self.qubot = GCodeController(
            port_name=qubot_port or self.DEFAULT_QUBOT_PORT,
        )
        # Set axis limits
        limits = axis_limits or self.DEFAULT_AXIS_LIMITS
        for axis, (min_val, max_val) in limits.items():
            self.qubot.set_axis_limits(axis, min_val, max_val)

        # Initialize pipette
        self.pipette = SartoriusController(
            port_name=sartorius_port or self.DEFAULT_SARTORIUS_PORT,
        )
        
    def connect(self):
        """Connect all controllers."""
        self.qubot.connect()
        self.pipette.connect()
        
    def disconnect(self):
        """Disconnect all controllers."""
        self.qubot.disconnect()
        self.pipette.disconnect()
        
    def load_labware(self, slot: str, labware_name: str):
        """Load a labware object into a slot."""
        self.deck.load_labware(slot=slot, labware_name=labware_name)
    
    def load_deck(self, deck_layout: Dict[str, Type[StandardLabware]]):
        """
        Load multiple labware into the deck at once.
        
        Args:
            deck_layout: Dictionary mapping slot names (e.g., "A1") to labware classes.
                        Each class will be instantiated automatically.
        
        Example:
            machine.load_deck({
                "A1": Opentrons96TipRack300,
                "B1": Opentrons96TipRack300,
                "C1": Rubbish,
            })
        """
        for slot, labware_name in deck_layout.items():
            self.load_labware(slot=slot, labware_name=labware_name)
        
    def move_z_to(self, slot: str, well: Optional[str] = None):
        """Move the Z axis to a position."""
        pos = self.get_absolute_position(slot, well)
        # return the offset from the origin
        self.qubot.move_absolute(position=pos - self.Z_ORIGIN)
        
    def move_a_to(self, slot: str, well: Optional[str] = None):
        """Move the A axis to a position."""
        pos = self.get_absolute_position(slot, well)
        # return the offset from the origin
        self.qubot.move_absolute(position=pos - self.A_ORIGIN)
        
    def transfer(self, amount: float, source: Position, destination: Position):
        """Transfer a volume of liquid from a source to a destination."""
        self.qubot.move_absolute(position=source)
        self.pipette.aspirate(amount=amount)
        time.sleep(5)
        self.qubot.move_absolute(position=destination)
        self.pipette.dispense(amount=amount)
        time.sleep(5)
        
    def get_slot_origin(self, slot: str) -> Position:
        """
        Get the origin coordinates of a slot.
        
        Args:
            slot: Slot name (e.g., 'A1', 'B2')
            
        Returns:
            Position for the slot origin
            
        Raises:
            KeyError: If slot name is invalid
        """
        slot = slot.upper()
        if slot not in self.SLOT_ORIGINS:
            raise KeyError(f"Invalid slot name: {slot}. Must be one of {list(self.SLOT_ORIGINS.keys())}")
        return self.SLOT_ORIGINS[slot]
    
    def get_absolute_position(self, slot: str, well: Optional[str] = None) -> Position:
        """
        Get the absolute position for a slot (and optionally a well within that slot) based on the origin
        
        Args:
            slot: Slot name (e.g., 'A1', 'B2')
            well: Optional well name within the slot (e.g., 'A1' for a well in a tiprack)
            
        Returns:
            Position with absolute coordinates
        """
        # Get slot origin
        pos = self.get_slot_origin(slot)
        
        # If well is specified, get well position relative to slot and add to slot origin
        if well:
            pos += self.deck[slot].get_well_position(well)
        
        return pos
