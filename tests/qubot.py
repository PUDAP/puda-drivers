import logging
from puda_drivers.move import GCodeController

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

PORT_NAME = "/dev/ttyACM1"


def main():
    print("--- Starting GCode Controller Application ---")

    try:
        # Instantiate the qubot controller
        qubot = GCodeController(port_name=PORT_NAME)

        # Example: Get current axis limits
        print("\n--- Current Axis Limits ---")
        all_limits = qubot.get_axis_limits()
        for axis, limits in all_limits.items():
            print(f"{axis}: [{limits.min}, {limits.max}]")

        # Example: Get limits for a specific axis
        x_limits = qubot.get_axis_limits("X")
        print(f"\nX axis limits: [{x_limits.min}, {x_limits.max}]")

        # Example: Set custom axis limits
        qubot.set_axis_limits("X", -200, 200)
        qubot.set_axis_limits("Y", -200, 200)
        qubot.set_axis_limits("Z", -100, 100)
        qubot.set_axis_limits("A", -180, 180)

        qubot.query_position()
        # Always start with homing
        qubot.home()
        qubot.sync_position()

        # Should generate WARNING due to exceeding MAX_FEEDRATE (3000)
        # qubot.feed = 5000

        # Relative moves are converted to absolute internally, but works the same
        # qubot.move_relative(x=20.0, y=-10.0)
        #
        # qubot.move_absolute(x=50.0, y=-50.0, z=-10.0)

        # Example of an ERROR - invalid axis
        # try:
        #     qubot.home(axis="B")  # Generates ERROR
        # except ValueError:
        #     pass

        # Example of an ERROR - position outside limits
        # try:
        #     qubot.move_absolute(x=150.0)  # May raise ValueError if outside limits
        # except ValueError as e:
        #     print(f"Position validation error: {e}")

    except Exception as e:
        logging.getLogger(__name__).error(f"An unrecoverable error occurred: {e}")


if __name__ == "__main__":
    main()
