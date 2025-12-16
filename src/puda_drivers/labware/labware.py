# src/puda_drivers/labware/labware.py

import json
import inspect
from pathlib import Path
from typing import Dict, Any
from abc import ABC, abstractmethod


class StandardLabware(ABC):
    """
    Generic Parent Class for all Labware on a microplate 
    """
    
    @classmethod
    def load_definition(cls, definition_file: str = "definition.json") -> Dict[str, Any]:
        """
        Load a definition.json file from the calling module's directory.
        
        This class method automatically finds the definition.json file in the
        same directory as the subclass that calls it.
        
        Args:
            definition_file: Name of the definition file (default: "definition.json")
            
        Returns:
            Dictionary containing the labware definition
            
        Raises:
            FileNotFoundError: If the definition file doesn't exist
        """
        # Get the frame of the caller (the class that called this method)
        caller_frame = inspect.stack()[1]
        caller_file = Path(caller_frame.filename)
        definition_path = caller_file.parent / definition_file
        
        if not definition_path.exists():
            raise FileNotFoundError(
                f"Definition file '{definition_file}' not found in {caller_file.parent}"
            )
        
        with open(definition_path, "r", encoding="utf-8") as f:
            return json.load(f)
    
    def __init__(self, name: str,rows: int, cols: int):
        """
        Initialize the labware.
        Args:
            name: The name of the labware.
            rows: The number of rows in the labware.
            cols: The number of columns in the labware.
        """
        self.name = name
        self.rows = rows
        self.cols = cols
        self.wells = {} 
        self._initialize_wells()

    def _initialize_wells(self):
        row_letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        for r in range(self.rows):
            for c in range(self.cols):
                well_name = f"{row_letters[r]}{c+1}"
                self.wells[well_name] = {}

    @abstractmethod
    def get_well_position(self, well_id: str):
        """
        Get the position of a well from the definition.json file
        """