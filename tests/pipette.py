import logging
from puda_drivers.transfer.liquid.sartorius import SartoriusController
from puda_drivers.core.logging import setup_logging

# Optional: finding ports
# import serial.tools.list_ports
# for port, desc, hwid in serial.tools.list_ports.comports():
#     print(f"{port}: {desc} [{hwid}]")

# --- LOGGING CONFIGURATION ---
# Set ENABLE_FILE_LOGGING to True to save logs to logs/ folder, False to only output to console
ENABLE_FILE_LOGGING = True  # Change to False to disable file logging

# Configure logging
# All loggers in imported modules (SerialController, SartoriusController) will inherit this setup.
setup_logging(
    enable_file_logging=ENABLE_FILE_LOGGING,
    log_level=logging.DEBUG,  # Use logging.DEBUG to see all (DEBUG, INFO, WARNING, ERROR, CRITICAL) logs
)

# OPTIONAL: If you only want specific loggers at specific level, you can specifically set it here
# logging.getLogger('puda_drivers.transfer.liquid.sartorius').setLevel(logging.INFO)


# --- CONFIGURATION ---
SARTORIUS_PORT = "/dev/ttyUSB0"
TRANSFER_VOLUME = 20  # uL
TIP_LENGTH = 70  # mm


# --- TEST FUNCTION ---
def test_pipette_operations():
    """
    Tests the initialization and core liquid handling functions
    of the SartoriusController.
    """
    print("--- 🔬 Starting Pipette Controller Test ---")
    pipette = SartoriusController(port_name=SARTORIUS_PORT)

    try:
        # 1. Initialize and Connect
        print("\n[STEP 1] Connecting to pipette...")
        # SartoriusController connects automatically in __init__, no need to call connect()
        
        # Always start with initializing
        pipette.initialize()

        # pipette.get_inward_speed()
        # print("\n set inward speed to 3")
        # pipette.set_inward_speed(3)

        # 3. Eject Tip (if any)
        print("\n[STEP 3] Ejecting Tip (if any)...")
        pipette.eject_tip(return_position=30)
        print(f"\n[STEP 3] Aspirate {TRANSFER_VOLUME} uL...")
        pipette.aspirate(amount=TRANSFER_VOLUME)

        # 4. Dispense
        print(f"\n[STEP 4] Dispensing {TRANSFER_VOLUME} uL...")
        pipette.dispense(amount=TRANSFER_VOLUME)

        # # 5. Eject Tip
        # print("\n[STEP 5] Ejecting Tip...")
        # pipette.eject()
        # if not pipette.is_tip_on():
        #     print("✅ Tip check: Tip is ejected.")
        # else:
        #     raise Exception("Tip ejection failed.")
        #
        # print("\n--- 🎉 All Pipette operations passed! ---")

    except Exception as e:
        print(f"\n--- ❌ TEST FAILURE: {e} ---")

    finally:
        # 6. Disconnect
        if pipette and pipette.is_connected:
            print("\n[FINAL] Disconnecting...")
            pipette.disconnect()
        print("--- 🧪 Pipette Controller Test Complete ---")


if __name__ == "__main__":
    test_pipette_operations()
