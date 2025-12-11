import logging
import serial
from puda_drivers.move import GCodeController
from puda_drivers.transfer.liquid.sartorius import SartoriusController
from puda_drivers.core.logging import setup_logging

# --- LOGGING CONFIGURATION ---
setup_logging(
    enable_file_logging=False,
    log_level=logging.INFO,  # Use INFO level for cleaner output during automation
)

# Qubot Configuration
QUBOT_PORT = "/dev/ttyACM0"
QUBOT_BAUDRATE = 9600
QUBOT_FEEDRATE = 3000

# Sartorius Configuration
SARTORIUS_PORT = "/dev/ttyUSB0"
TRANSFER_VOLUME = 50.0  # uL
TIP_LENGTH = 70  # mm

# Define mock coordinates (assuming the Qubot operating space is in mm)
# Note: These coordinates must be within the axis limits set below
ASPIRATE_POS = {"X": 50.0, "Y": -50.0, "Z": -20.0, "A": 0.0}  # Source well location
DISPENSE_POS = {
    "X": 80.0,
    "Y": -80.0,
    "Z": -20.0,
    "A": 0.0,
}  # Destination well location
SAFE_Z_POS = -10.0  # High Z position to prevent collisions (within Z limits)

qubot = None
pipette = None

# 1. Initialize and Connect Qubot
try:
    print("Connecting to qubot")
    # GCodeController connects automatically in __init__, no need to call connect()
    qubot = GCodeController(
        port_name=QUBOT_PORT, baudrate=QUBOT_BAUDRATE, feed=QUBOT_FEEDRATE
    )
    
    # Set axis limits to accommodate the automation coordinates
    # Adjust these based on your actual hardware limits
    qubot.set_axis_limits("X", 0, 200)
    qubot.set_axis_limits("Y", -200, 0)
    qubot.set_axis_limits("Z", -100, 0)
    qubot.set_axis_limits("A", -180, 180)
    
    print("Axis limits configured:")
    all_limits = qubot.get_axis_limits()
    for axis, limits in all_limits.items():
        print(f"  {axis}: [{limits.min}, {limits.max}]")
    
    # Always start with homing
    qubot.home()
except (IOError, ValueError, serial.SerialException) as e:
    print(f"FATAL ERROR: Could not connect to Qubot GCode Controller: \n{e}")
except Exception as e:
    print(f"Unexpected error during Qubot connection: {e}")

# 2. Initialize and Connect the Liquid Handler (Sartorius)
try:
    print("Connecting to pipette")
    # SartoriusController connects automatically in __init__, no need to call connect()
    pipette = SartoriusController(port_name=SARTORIUS_PORT)
    # always start with initializing
    pipette.initialize()
except Exception as e:
    print(f"FATAL ERROR: Could not initialize/connect Sartorius: {e}")
    if qubot:
        qubot.disconnect()


def run_automation():
    """
    Simulates connecting the GCodeController and Sartorius pipette
    and performing a liquid transfer routine.
    """
    print("--- 🤖 Starting Automated Pipetting Routine ---")

    # --- Start of Protocol ---
    try:
        print("Protocol Step 1: Attaching Tip")
        # Simulate moving to a tip rack position and attaching a tip
        print("Moving to Tip Rack and Attaching Tip...")
        pos = qubot.move_absolute(x=50.0, y=-50.0, z=-50.0, feed=3000)
        print(f"  Position after move: {pos}")

        # 4. Protocol Step 2: Aspirate Liquid
        print(f"\nProtocol Step 2: Aspirating {TRANSFER_VOLUME} uL")

        # Move to the source well (ASPIRATE_POS)
        pos = qubot.move_absolute(
            x=ASPIRATE_POS["X"], y=ASPIRATE_POS["Y"], z=ASPIRATE_POS["Z"], feed=3000
        )
        print(f"  Position at source well: {pos}")

        # Lower the tip to the aspiration depth using A axis for height
        print(f"  Position at aspiration depth: {pos}")

        # Perform aspiration
        pipette.aspirate(amount=TRANSFER_VOLUME)

        # 5. Protocol Step 3: Dispense Liquid
        print(f"\nProtocol Step 3: Dispensing {TRANSFER_VOLUME} uL")

        # Move to safe Z-height again
        pos = qubot.move_absolute(z=SAFE_Z_POS)
        print(f"  Position after safe Z move: {pos}")

        # Move to the destination well (DISPENSE_POS)
        pos = qubot.move_absolute(
            x=DISPENSE_POS["X"], y=DISPENSE_POS["Y"], z=DISPENSE_POS["Z"], feed=3000
        )
        print(f"  Position at destination well: {pos}")

        # Lower the tip to the dispensing depth using A axis for height
        pos = qubot.move_absolute(a=DISPENSE_POS["A"])
        print(f"  Position at dispensing depth: {pos}")

        # Perform dispensing
        pipette.dispense(amount=TRANSFER_VOLUME)

        # 6. Protocol Step 4: Finalization
        print("\nProtocol Step 4: Finalizing")

        # Simulate moving to a trash bin and ejecting the tip
        pos = qubot.move_absolute(x=10.0, y=-10.0)
        print(f"  Position at trash bin: {pos}")
        pipette.eject_tip()

    except Exception as e:
        print(f"\n--- ❌ PROTOCOL FAILURE: {e} ---")

    finally:
        # 7. Disconnect Devices
        print("\n--- 🛑 Disconnecting Devices ---")
        pipette.disconnect()
        qubot.disconnect()
        print("--- Automated Pipetting Routine Complete ---")


# Execute the combined routine
if __name__ == "__main__":
    if qubot is None or pipette is None:
        print("\n--- ❌ Devices Not Properly Initialized ---\n")
        exit(1)
    print("\n--- ✅ Devices Initialized and Connected ---\n")
    run_automation()
