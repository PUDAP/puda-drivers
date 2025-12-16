# src/puda_drivers/move/deck.py

from puda_drivers.labware import StandardLabware


class Deck:
    """
    Deck class for managing labware layout.
    """
    def __init__(self):
        """
        Initialize the deck.
        """
        # A dictionary mapping Slot Names (A1, B4) to Labware Objects
        self.slots = {}

    def load_labware(self, slot: str, labware_obj: StandardLabware):
        """
        Load labware into a slot.
        """
        self.slots[slot.upper()] = labware_obj
        
    def show_deck(self):
        """
        Show the deck layout.
        """
        for slot, labware in self.slots.items():
            print(f"{slot}: {labware.name}")

    def __getitem__(self, key):
        """Allows syntax for: my_deck['B4']"""
        return self.slots[key.upper()]