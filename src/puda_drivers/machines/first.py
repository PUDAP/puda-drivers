"""
First machine class containing Deck, GCodeController, and SartoriusController.

This class demonstrates the integration of:
- GCodeController: Handles motion control (hardware-specific)
- Deck: Manages labware layout (configuration-agnostic)
- SartoriusController: Handles liquid handling operations
"""

import logging
import time
from typing import Optional, Dict, Tuple, Type, Union
from puda_drivers.move import GCodeController, Deck
from puda_drivers.core import Position
from puda_drivers.transfer.liquid.sartorius import SartoriusController
from puda_drivers.labware import StandardLabware
from puda_drivers.cv import CameraController


class First:
    """
    First machine class integrating motion control, deck management, liquid handling, and camera.
    
    The deck has 16 slots arranged in a 4x4 grid (A1-D4).
    Each slot's origin location is stored for absolute movement calculation.
    """
    
    # Default configuration values
    DEFAULT_QUBOT_PORT = "/dev/ttyACM0"
    DEFAULT_QUBOT_BAUDRATE = 9600
    DEFAULT_QUBOT_FEEDRATE = 3000
    
    DEFAULT_SARTORIUS_PORT = "/dev/ttyUSB0"
    DEFAULT_SARTORIUS_BAUDRATE = 9600
    
    DEFAULT_CAMERA_INDEX = 0
    
    # origin position of Z and A axes
    Z_ORIGIN = Position(x=0, y=0, z=0)
    A_ORIGIN = Position(x=60, y=0, a=0)
    
    # Default axis limits - customize based on your hardware
    DEFAULT_AXIS_LIMITS = {
        "X": (0, 330),
        "Y": (-440, 0),
        "Z": (-140, 0),
        "A": (-175, 0),
    }
    
    # Height from z and a origin to the deck
    CEILING_HEIGHT = 192.2
    
    # Tip length
    TIP_LENGTH = 59
    
    # Slot origins (the bottom left corner of the slot relative to the deck origin)
    SLOT_ORIGINS = {
        "A1": Position(x=-2, y=-424),
        "A2": Position(x=98, y=-424),
        "A3": Position(x=198, y=-424),
        "A4": Position(x=298, y=-424),
        "B1": Position(x=-2, y=-274),
        "B2": Position(x=98, y=-274),
        "B3": Position(x=198, y=-274),
        "B4": Position(x=298, y=-274),
        "C1": Position(x=-2, y=-124),
        "C2": Position(x=98, y=-124),
        "C3": Position(x=198, y=-124),
        "C4": Position(x=298, y=-124),
    }
    
    def __init__(
        self,
        qubot_port: Optional[str] = None,
        sartorius_port: Optional[str] = None,
        camera_index: Optional[Union[int, str]] = None,
        axis_limits: Optional[Dict[str, Tuple[float, float]]] = None,
    ):
        """
        Initialize the First machine.
        
        Args:
            qubot_port: Serial port for GCodeController (e.g., '/dev/ttyACM0')
            sartorius_port: Serial port for SartoriusController (e.g., '/dev/ttyUSB0')
            camera_index: Camera device index (0 for default) or device path/identifier.
                         Defaults to 0.
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
        
        # Initialize camera
        self.camera = CameraController(
            camera_index=camera_index if camera_index is not None else self.DEFAULT_CAMERA_INDEX,
        )
        
        # Initialize logger
        self._logger = logging.getLogger(__name__)
        self._logger.info(
            "First machine initialized with qubot_port='%s', sartorius_port='%s', camera_index=%s",
            qubot_port or self.DEFAULT_QUBOT_PORT,
            sartorius_port or self.DEFAULT_SARTORIUS_PORT,
            camera_index if camera_index is not None else self.DEFAULT_CAMERA_INDEX,
        )
        
    def connect(self):
        """Connect all controllers."""
        self._logger.info("Connecting all controllers")
        self.qubot.connect()
        self.pipette.connect()
        self.camera.connect()
        self._logger.info("All controllers connected successfully")
        
    def disconnect(self):
        """Disconnect all controllers."""
        self._logger.info("Disconnecting all controllers")
        self.qubot.disconnect()
        self.pipette.disconnect()
        self.camera.disconnect()
        self._logger.info("All controllers disconnected successfully")
        
    def load_labware(self, slot: str, labware_name: str):
        """Load a labware object into a slot."""
        self._logger.info("Loading labware '%s' into slot '%s'", labware_name, slot)
        self.deck.load_labware(slot=slot, labware_name=labware_name)
        self._logger.debug("Labware '%s' loaded into slot '%s'", labware_name, slot)
    
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
        self._logger.info("Loading deck layout with %d labware items", len(deck_layout))
        for slot, labware_name in deck_layout.items():
            self.load_labware(slot=slot, labware_name=labware_name)
        self._logger.info("Deck layout loaded successfully")
        
    def attach_tip(self, slot: str, well: Optional[str] = None):
        """Attach a tip from a slot."""
        if self.pipette.is_tip_attached():
            self._logger.error("Cannot attach tip: tip already attached")
            raise ValueError("Tip already attached")
        
        self._logger.info("Attaching tip from slot '%s'%s", slot, f", well '{well}'" if well else "")
        pos = self.get_absolute_z_position(slot, well)
        self._logger.debug("Moving to position %s for tip attachment", pos)
        # return the offset from the origin
        self.qubot.move_absolute(position=pos)
        
        # attach tip (move slowly down)
        insert_depth = self.deck[slot].get_insert_depth()
        self._logger.debug("Moving down by %s mm to insert tip", insert_depth)
        self.qubot.move_relative(
            position=Position(z=-insert_depth),
            feed=500
        )
        self.pipette.set_tip_attached(attached=True)
        self._logger.info("Tip attached successfully, homing Z axis")
        # must home Z axis after, as pressing in tip might cause it to lose steps
        self.qubot.home(axis="Z")
        self._logger.debug("Z axis homed after tip attachment")
        
    def drop_tip(self, slot: str, well: str):
        """Drop a tip into a slot."""
        if not self.pipette.is_tip_attached():
            self._logger.error("Cannot drop tip: no tip attached")
            raise ValueError("Tip not attached")
        
        self._logger.info("Dropping tip into slot '%s', well '%s'", slot, well)
        pos = self.get_absolute_z_position(slot, well)
        # move up by the tip length
        pos += Position(z=self.TIP_LENGTH)
        self._logger.debug("Moving to position %s (adjusted for tip length) for tip drop", pos)
        self.qubot.move_absolute(position=pos)

        self._logger.debug("Ejecting tip")
        self.pipette.eject_tip()
        time.sleep(5)
        self.pipette.set_tip_attached(attached=False)
        self._logger.info("Tip dropped successfully")
        
    def aspirate_from(self, slot:str, well:str, amount:int):
        """Aspirate a volume of liquid from a slot."""
        if not self.pipette.is_tip_attached():
            self._logger.error("Cannot aspirate: no tip attached")
            raise ValueError("Tip not attached")
        
        self._logger.info("Aspirating %d µL from slot '%s', well '%s'", amount, slot, well)
        pos = self.get_absolute_z_position(slot, well)
        self._logger.debug("Moving Z axis to position %s", pos)
        self.qubot.move_absolute(position=pos)
        self._logger.debug("Aspirating %d µL", amount)
        self.pipette.aspirate(amount=amount)
        time.sleep(5)
        self._logger.info("Aspiration completed: %d µL from slot '%s', well '%s'", amount, slot, well)
        
    def dispense_to(self, slot:str, well:str, amount:int):
        """Dispense a volume of liquid to a slot."""
        if not self.pipette.is_tip_attached():
            self._logger.error("Cannot dispense: no tip attached")
            raise ValueError("Tip not attached")
        
        self._logger.info("Dispensing %d µL to slot '%s', well '%s'", amount, slot, well)
        pos = self.get_absolute_z_position(slot, well)
        self._logger.debug("Moving Z axis to position %s", pos)
        self.qubot.move_absolute(position=pos)
        self._logger.debug("Dispensing %d µL", amount)
        self.pipette.dispense(amount=amount)
        time.sleep(5)
        self._logger.info("Dispense completed: %d µL to slot '%s', well '%s'", amount, slot, well)
        
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
            self._logger.error("Invalid slot name: '%s'. Must be one of %s", slot, list(self.SLOT_ORIGINS.keys()))
            raise KeyError(f"Invalid slot name: {slot}. Must be one of {list(self.SLOT_ORIGINS.keys())}")
        pos = self.SLOT_ORIGINS[slot]
        self._logger.debug("Slot origin for '%s': %s", slot, pos)
        return pos
    
    def get_absolute_z_position(self, slot: str, well: Optional[str] = None) -> Position:
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

        # relative well position from slot origin
        if well:
            well_pos = self.deck[slot].get_well_position(well).get_xy()
            # the deck is rotated 90 degrees clockwise for this machine
            pos += well_pos.swap_xy()
            # get z
            z = Position(z=self.deck[slot].get_height() - self.CEILING_HEIGHT)
            pos += z
            self._logger.debug("Absolute Z position for slot '%s', well '%s': %s", slot, well, pos)
        else:
            self._logger.debug("Absolute Z position for slot '%s': %s", slot, pos)
        return pos
    
    def get_absolute_a_position(self, slot: str, well: Optional[str] = None) -> Position:
        """
        Get the absolute position for a slot (and optionally a well within that slot) based on the origin
        """
        pos = self.get_slot_origin(slot)
        
        if well:
            well_pos = self.deck[slot].get_well_position(well).get_xy()
            pos += well_pos.swap_xy()
            
            # get a
            a = Position(a=self.deck[slot].get_height() - self.CEILING_HEIGHT)
            pos += a
            self._logger.debug("Absolute A position for slot '%s', well '%s': %s", slot, well, pos)
        else:
            self._logger.debug("Absolute A position for slot '%s': %s", slot, pos)
        return pos
