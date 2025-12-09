import logging
from puda_drivers.transfer.liquid.sartorius import SartoriusController

# Optinal: finding ports
# import serial.tools.list_ports
# for port, desc, hwid in serial.tools.list_ports.comports():
#     print(f"{port}: {desc} [{hwid}]")

# 1. Configure the root logger
# All loggers in imported modules (SerialController, GCodeController) will inherit this setup.
logging.basicConfig(
    # Use logging.DEBUG to see all (DEBUG, INFO, WARNING, ERROR, CRITICAL) logs
    level=logging.DEBUG,
    # Recommended format: includes time, logger name, level, and message
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

# 2. OPTIONAL: If you only want GCodeController's logs at specific level, you can specifically set it here
# logging.getLogger('drivers.gcodecontroller').setLevel(logging.INFO)


# --- CONFIGURATION ---
SARTORIUS_PORT = "/dev/ttyUSB0"
TRANSFER_VOLUME = 20  # uL
TIP_LENGTH = 70  # mm


# --- TEST FUNCTION ---
def test_sartorius_operations():
    """
    Tests the initialization and core liquid handling functions
    of the SartoriusController mock class.
    """
    print("--- 🔬 Starting Sartorius Controller Test ---")
    sartorius = SartoriusController(port_name=SARTORIUS_PORT)

    try:
        # 1. Initialize and Connect
        print("\n[STEP 1] Connecting to pipette...")
        sartorius.connect()

        # # 2. Attach Tip
        # print("\n[STEP 2] Initialize...")
        # sartorius.initialize()

        sartorius.get_inward_speed()
        # print("\n set inward speed to 3")
        # sartorius.set_inward_speed(3)

        # 3. Eject Tip (if any)
        # print("\n[STEP 3] Ejecting Tip (if any)...")
        # sartorius.eject_tip(return_position=30)
        # print(f"\n[STEP 3] Aspirate {TRANSFER_VOLUME} uL...")
        # sartorius.aspirate(amount=TRANSFER_VOLUME)

        # 4. Dispense
        # print(f"\n[STEP 4] Dispensing {TRANSFER_VOLUME} uL...")
        # sartorius.dispense(amount=TRANSFER_VOLUME)

        # # 5. Eject Tip
        # print("\n[STEP 5] Ejecting Tip...")
        # sartorius.eject()
        # if not sartorius.is_tip_on():
        #     print("✅ Tip check: Tip is ejected.")
        # else:
        #     raise Exception("Tip ejection failed.")
        #
        # print("\n--- 🎉 All Sartorius operations passed! ---")

    except Exception as e:
        print(f"\n--- ❌ TEST FAILURE: {e} ---")

    finally:
        # 6. Disconnect
        if sartorius and sartorius.is_connected:
            print("\n[FINAL] Disconnecting...")
            sartorius.disconnect()
        print("--- 🧪 Sartorius Controller Test Complete ---")


if __name__ == "__main__":
    test_sartorius_operations()
