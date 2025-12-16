# src/puda_drivers/labware/opentrons_96_tiprack_300ul/__init__.py

from puda_drivers.labware import StandardLabware
from puda_drivers.core import Position


class Opentrons96TipRack300(StandardLabware):
    def __init__(self):
        """
        Initialize the tip rack using data from definition.json.
        
        Args:
            name: Optional custom name. If None, uses displayName from JSON metadata.
        """
        # Load definition data
        self._definition = self.load_definition()
        
        # Extract dimensions from ordering array
        ordering = self._definition.get("ordering", [])
        rows = len(ordering)
        cols = len(ordering[0]) if ordering else 0
        
        super().__init__(
            name=self._definition.get("metadata", {}).get("displayName", "displayName not found"), 
            rows=rows, 
            cols=cols
        )
        
        # Store well data from JSON for attach_tip logic
        self._well_data = self._definition.get("wells", {})

    def get_well_position(self, well_id: str) -> Position:
        """
        Get the top center position of a well from definition.json.
        
        Args:
            well_id: Well identifier (e.g., "A1", "H12")
            
        Returns:
            Position with x, y, z coordinates
            
        Raises:
            KeyError: If well_id doesn't exist in the tip rack
        """
        # Validate location exists in JSON definition
        well_id_upper = well_id.upper()
        if well_id_upper not in self.wells:
            raise KeyError(f"Well '{well_id}' not found in tip rack definition")
        
        # Get the well data from the definition
        well_data = self._definition.get("wells", {}).get(well_id_upper, {})
        
        # Return position of the well (x, y are already center coordinates)
        return Position(
            x=well_data.get("x", 0.0),
            y=well_data.get("y", 0.0),
            z=well_data.get("z", 0.0), 
        )