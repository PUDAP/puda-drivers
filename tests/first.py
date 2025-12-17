"""Test script for the First machine driver."""
from puda_drivers.machines import First
from puda_drivers.labware import get_available_labware

if __name__ == "__main__":
    machine = First()
    
    print(get_available_labware())

    # Define deck layout declaratively and load all at once
    machine.load_deck({
        "A1": "opentrons_96_tiprack_300ul",
        "B1": "opentrons_96_tiprack_300ul",
        "C1": "trash_bin",
        "D1": "opentrons_96_tiprack_300ul",
    })

    print(machine.deck)
    print(machine.deck["B1"].get_well_position("A1"))
    
    # test
    # machine.connect()
    # machine.transfer(
    #     amount=100, 
    #     source=machine.deck["B1"].get_well_position("A1"), 
    #     destination=machine.deck["C1"].get_well_position("A1")
    # )