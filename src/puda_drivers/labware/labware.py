# src/puda_drivers/labware/labware.py

import json
import inspect
from pathlib import Path
from typing import Dict, Any
from abc import ABC
from typing import List
from puda_drivers.core import Position


class StandardLabware(ABC):
    """
    Generic Parent Class for all Labware on a microplate 
    """
    def __init__(self, labware_name: str):
        """
        Initialize the labware.
        Args:
            name: The name of the labware.
            rows: The number of rows in the labware.
            cols: The number of columns in the labware.
        """
        self._definition = self.load_definition(file_name=labware_name + ".json")
        self.name = self._definition.get("metadata", {}).get("displayName", "displayName not found")
        self._wells = self._definition.get("wells", {})

    def load_definition(self, file_name: str = "definition.json") -> Dict[str, Any]:
        """
        Load a definition.json file from the class's module directory.
        
        This method automatically finds the definition.json file in the
        same directory as the class that defines it.
        
        Args:
            file_name: Name of the definition file (default: "definition.json")
            
        Returns:
            Dictionary containing the labware definition
            
        Raises:
            FileNotFoundError: If the definition file doesn't exist
        """
        # Get the file path of the class that defines this method
        class_file = Path(inspect.getfile(self.__class__))
        definition_path = class_file.parent / file_name
        
        if not definition_path.exists():
            raise FileNotFoundError(
                f"Definition file '{file_name}' not found in {class_file.parent}"
            )
        
        with open(definition_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def get_well_position(self, well_id: str) -> Position:
        """
        Get the position of a well from definition.json.
        
        Args:
            well_id: Well identifier (e.g., "A1", "H12")
            
        Returns:
            Position with x, y, z coordinates
            
        Raises:
            KeyError: If well_id doesn't exist in the tip rack
        """
        # Validate location exists in JSON definition
        well_id_upper = well_id.upper()
        if well_id_upper not in self._wells:
            raise KeyError(f"Well '{well_id}' not found in tip rack definition")
        
        # Get the well data from the definition
        well_data = self._wells.get(well_id_upper, {})
        
        # Return position of the well (x, y are already center coordinates)
        return Position(
            x=well_data.get("x", 0.0),
            y=well_data.get("y", 0.0),
            z=well_data.get("z", 0.0), 
        )
        
    @staticmethod
    def get_available_labware() -> List[str]:
        """
        Get all available labware names from JSON definition files.
        
        Returns:
            Sorted list of labware names (without .json extension) found in the labware directory.
        """
        labware_dir = Path(__file__).parent
        json_files = sorted(labware_dir.glob("*.json"))
        return [f.stem for f in json_files]