"""
Rubbish Labware
"""

from puda_drivers.labware import StandardLabware

class Rubbish(StandardLabware):
    """
    Rubbish Labware
    """
    def __init__(self):
        self._definition = self.load_definition()
        super().__init__(
            name=self._definition.get("metadata", {}).get("displayName", "displayName not found"), 
            rows=1, 
            cols=1
        )

    def get_well_position(self, well_id: str):
        """
        Get the position of a well from the definition.json file
        """
        return {
            "x": .0,
            "y": 0.0,
            "z": 10.0,
        }