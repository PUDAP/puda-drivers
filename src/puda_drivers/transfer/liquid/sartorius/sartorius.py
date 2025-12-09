"""
Sartorius rLINE pipette controller.

This module provides a Python interface for controlling Sartorius rLINE® electronic
pipettes and robotic dispensers via serial communication.

Reference: https://api.sartorius.com/document-hub/dam/download/34901/Sartorius-rLine-technical-user-manual-v1.1.pdf
"""

import logging
from typing import Optional

from puda_drivers.core.serialcontroller import SerialController

from .constants import STATUS_CODES


class SartoriusDeviceError(Exception):
    """Custom exception raised when the Sartorius device reports an error."""


class SartoriusController(SerialController):
    """
    Controller for Sartorius rLINE® pipettes and robotic dispensers.

    This class provides methods for controlling pipette operations including
    aspiration, dispensing, tip ejection, and speed control via serial communication.

    Attributes:
        DEFAULT_BAUDRATE: Default baud rate for serial communication (9600)
        DEFAULT_TIMEOUT: Default timeout for operations (10 seconds)
        MICROLITER_PER_STEP: Conversion factor from steps to microliters (0.5 µL/step)
        MIN_SPEED: Minimum speed setting (1)
        MAX_SPEED: Maximum speed setting (6)
    """

    # Protocol Constants
    DEFAULT_BAUDRATE = 9600
    DEFAULT_TIMEOUT = 10

    PROTOCOL_SOH = "\x01"
    SLAVE_ADDRESS = "1"
    PROTOCOL_TERMINATOR = "º\r"

    # Sartorius rLine Settings
    MICROLITER_PER_STEP = 0.5
    SUCCESS_RESPONSE = "ok"
    ERROR_RESPONSE = "err"
    MIN_SPEED = 1
    MAX_SPEED = 6

    def __init__(
        self,
        port_name: Optional[str] = None,
        baudrate: int = DEFAULT_BAUDRATE,
        timeout: int = DEFAULT_TIMEOUT,
    ):
        """
        Initialize the Sartorius controller.

        Args:
            port_name: Serial port name (e.g., '/dev/ttyUSB0' or 'COM3')
            baudrate: Baud rate for serial communication. Defaults to 9600.
            timeout: Timeout in seconds for operations. Defaults to 10.
        """
        super().__init__(port_name, baudrate, timeout)
        self._logger = logging.getLogger(__name__)
        self._logger.info(
            "Sartorius Controller initialized with port='%s', baudrate=%s, timeout=%s",
            port_name,
            baudrate,
            timeout,
        )

    def _build_command(self, command_code: str, value: str = "") -> str:
        """
        Build a command string according to the Sartorius protocol.

        Command format: <SOH><SLAVE_ADDRESS>R<COMMAND_CODE><VALUE><TERMINATOR>

        Args:
            command_code: Single character command code
            value: Optional value string to append to the command

        Returns:
            Complete command string ready to send
        """
        return (
            self.PROTOCOL_SOH
            + self.SLAVE_ADDRESS
            + "R"
            + command_code
            + value
            + self.PROTOCOL_TERMINATOR
        )

    def _check_response_error(self, response: str, operation: str) -> None:
        """
        Check if a response contains an error and raise an exception if so.

        Args:
            response: Response string from the device
            operation: Description of the operation being performed (for error message)

        Raises:
            SartoriusDeviceError: If the response contains an error
        """
        if self.ERROR_RESPONSE in response.lower():
            raise SartoriusDeviceError(
                f"{operation} failed. Device returned error: {response}"
            )

    def _validate_speed(self, speed: int, direction: str = "speed") -> None:
        """
        Validate that a speed value is within the allowed range.

        Args:
            speed: Speed value to validate
            direction: Direction description for error message (e.g., "Inward", "Outward")

        Raises:
            ValueError: If speed is outside the valid range
        """
        if not self.MIN_SPEED <= speed <= self.MAX_SPEED:
            raise ValueError(
                f"{direction} speed must be between {self.MIN_SPEED} and {self.MAX_SPEED}, "
                f"got {speed}"
            )

    def _validate_no_leading_zeros(self, value: int, command_name: str) -> str:
        """
        Validate that a numeric value has no leading zeros when converted to string.

        Args:
            value: Numeric value to validate
            command_name: Command name for error message (e.g., "RP", "RE")

        Returns:
            String representation of the value

        Raises:
            ValueError: If the value has leading zeros
        """
        value_str = str(value)
        if len(value_str) > 1 and value_str.startswith("0"):
            raise ValueError(
                f"{command_name} command value must not have leading zeros. "
                f"Got: {value_str}"
            )
        return value_str

    def _execute_command(
        self, command_code: str, value: str = "", operation: str = ""
    ) -> str:
        """
        Execute a command and return the response.

        Args:
            command_code: Command code to execute
            value: Optional value for the command
            operation: Description of the operation (for logging and error messages)

        Returns:
            Response string from the device

        Raises:
            SartoriusDeviceError: If the device returns an error
        """
        command = self._build_command(command_code, value)
        self._send_command(command)
        response = self._read_response()
        self._check_response_error(response, operation or f"Command {command_code}")
        return response

    def initialize(self) -> None:
        """
        Initialize the pipette unit (RZ command).

        This command resets the pipette to its initial state and should be called
        before performing other operations.

        Raises:
            SartoriusDeviceError: If initialization fails
        """
        self._logger.info("** Initializing Pipette Head (RZ) **")
        self._execute_command("Z", operation="Pipette initialization")
        self._logger.info("** Pipette Initialization Complete **")

    def get_inward_speed(self) -> int:
        """
        Query the current aspirating speed (DI command).

        Returns:
            Current inward speed setting (1-6)

        Raises:
            SartoriusDeviceError: If the query fails
        """
        self._logger.info("** Querying Inward Speed (DI) **")
        response = self._execute_command("DI", operation="Inward speed query")

        if len(response) < 2:
            raise SartoriusDeviceError(
                f"Invalid response format for inward speed query: {response}"
            )

        speed = int(response[1])
        self._logger.info("** Current Inward Speed: %s **", speed)
        return speed

    def set_inward_speed(self, speed: int) -> None:
        """
        Set the aspirating speed (SI command).

        Args:
            speed: Speed setting (1-6, where 1 is slowest and 6 is fastest)

        Raises:
            ValueError: If speed is outside the valid range
            SartoriusDeviceError: If setting the speed fails
        """
        self._validate_speed(speed, "Inward")
        self._logger.info("** Setting Inward Speed (SI, Speed: %s) **", speed)
        self._execute_command("I", value=str(speed), operation="Setting inward speed")
        self._logger.info("** Inward Speed Set to %s Successfully **", speed)

    def get_outward_speed(self) -> int:
        """
        Query the current dispensing speed (DO command).

        Returns:
            Current outward speed setting (1-6)

        Raises:
            SartoriusDeviceError: If the query fails
        """
        self._logger.info("** Querying Outward Speed (DO) **")
        response = self._execute_command("DO", operation="Outward speed query")

        if len(response) < 2:
            raise SartoriusDeviceError(
                f"Invalid response format for outward speed query: {response}"
            )

        speed = int(response[1])
        self._logger.info("** Current Outward Speed: %s **", speed)
        return speed

    def set_outward_speed(self, speed: int) -> None:
        """
        Set the dispensing speed (SO command).

        Args:
            speed: Speed setting (1-6, where 1 is slowest and 6 is fastest)

        Raises:
            ValueError: If speed is outside the valid range
            SartoriusDeviceError: If setting the speed fails
        """
        self._validate_speed(speed, "Outward")
        self._logger.info("** Setting Outward Speed (SO, Speed: %s) **", speed)
        self._execute_command("O", value=str(speed), operation="Setting outward speed")
        self._logger.info("** Outward Speed Set to %s Successfully **", speed)

    def run_to_position(self, position: int) -> None:
        """
        Drive the piston to an absolute step position (RP command).

        Args:
            position: Target position in steps (must not have leading zeros)

        Raises:
            ValueError: If position has leading zeros
            SartoriusDeviceError: If the command fails
        """
        position_str = self._validate_no_leading_zeros(position, "RP")
        self._logger.info("** Run to absolute Position (RP, Position: %s) **", position)
        self._execute_command("P", value=position_str, operation="Run to position")
        self._logger.info("** Reached Position %s Successfully **", position)

    def aspirate(self, amount: float) -> None:
        """
        Aspirate fluid from the current location.

        Args:
            amount: Volume to aspirate in microliters (µL)

        Raises:
            ValueError: If amount is negative or zero
            SartoriusDeviceError: If aspiration fails
        """
        if amount <= 0:
            raise ValueError(f"Aspiration amount must be positive, got {amount}")

        steps = int(amount / self.MICROLITER_PER_STEP)
        self._logger.info("** Aspirating %s uL (RI%s steps) **", amount, steps)
        self._execute_command("I", value=str(steps), operation="Aspirate")
        self._logger.info("** Aspirated %s uL Successfully **", amount)

    def dispense(self, amount: float) -> None:
        """
        Dispense fluid at the current location.

        Args:
            amount: Volume to dispense in microliters (µL)

        Raises:
            ValueError: If amount is negative or zero
            SartoriusDeviceError: If dispensing fails
        """
        if amount <= 0:
            raise ValueError(f"Dispense amount must be positive, got {amount}")

        steps = int(amount / self.MICROLITER_PER_STEP)
        self._logger.info("** Dispensing %s uL (RO%s steps) **", amount, steps)
        self._execute_command("O", value=str(steps), operation="Dispense")
        self._logger.info("** Dispensed %s uL Successfully **", amount)

    def eject_tip(self, return_position: int = 30) -> None:
        """
        Eject the pipette tip (RE command).

        Args:
            return_position: Position to return to after ejection. Defaults to 30.

        Raises:
            ValueError: If return_position has leading zeros
            SartoriusDeviceError: If tip ejection fails
        """
        position_str = self._validate_no_leading_zeros(return_position, "RE")
        self._logger.info(
            "** Ejecting Tip and returning to position %s (RE %s) **",
            return_position,
            return_position,
        )
        self._execute_command(
            "E", value=position_str, operation="Eject tip with return position"
        )
        self._logger.info("** Tip Ejection Complete **")

    def run_blowout(self, return_position: Optional[int] = None) -> None:
        """
        Run the blowout cycle to clear residual liquid (RB command).

        Args:
            return_position: Optional position to return to after blowout.
                           If None, completes blowout without returning.

        Raises:
            ValueError: If return_position has leading zeros
            SartoriusDeviceError: If blowout fails
        """
        if return_position is not None:
            position_str = self._validate_no_leading_zeros(
                return_position, "RB"
            )
            self._logger.info(
                "** Running Blowout and returning to position %s (RB %s) **",
                return_position,
                return_position,
            )
            self._execute_command(
                "B", value=position_str, operation="Blowout with return position"
            )
        else:
            self._logger.info("** Running Blowout (RB) **")
            self._execute_command("B", operation="Blowout")

        self._logger.info("** Blowout Complete **")

    def get_status(self) -> str:
        """
        Query the current status of the pipette (DS command).

        Returns:
            Status code character (single character string)

        Raises:
            SartoriusDeviceError: If the status query fails
        """
        self._logger.info("** Querying Pipette Status (DS) **")
        response = self._execute_command("DS", operation="Status query")

        if len(response) < 2:
            raise SartoriusDeviceError(
                f"Invalid response format for status query: {response}"
            )

        status_code = response[1]
        if status_code in STATUS_CODES:
            status_message = STATUS_CODES[status_code]
            self._logger.info("Pipette Status Code [%s]: %s", status_code, status_message)
        else:
            self._logger.warning(
                "Pipette Status Code [%s]: Unknown Status Code", status_code
            )

        return status_code
