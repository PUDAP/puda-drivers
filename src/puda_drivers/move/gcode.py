"""
G-code controller for motion systems.

This module provides a Python interface for controlling G-code compatible motion
systems (e.g., QuBot) via serial communication. All movements are executed in
absolute coordinates, with relative moves converted to absolute internally.
Supports homing and position synchronization.
"""

import re
import logging
from dataclasses import dataclass
from typing import Optional, Dict, Tuple, Union

from puda_drivers.core.serialcontroller import SerialController


@dataclass
class AxisLimits:
    """Holds min/max limits for an axis."""

    min: float
    max: float

    def validate(self, value: float) -> None:
        """
        Validate that a value is within the axis limits.

        Args:
            value: Value to validate

        Raises:
            ValueError: If value is outside the limits
        """
        if not (self.min <= value <= self.max):
            raise ValueError(
                f"Value {value} outside axis limits [{self.min}, {self.max}]"
            )


class GCodeController(SerialController):
    """
    Controller for G-code compatible motion systems.

    This class provides methods for controlling multi-axis motion systems that
    understand G-code commands. All movements are executed in absolute coordinates,
    with relative moves converted to absolute internally. Supports homing and
    position synchronization.

    Attributes:
        DEFAULT_FEEDRATE: Default feed rate in mm/min (3000)
        MAX_FEEDRATE: Maximum allowed feed rate in mm/min (3000)
        TOLERANCE: Position synchronization tolerance in mm (0.01)
    """

    DEFAULT_FEEDRATE = 3000  # mm/min
    MAX_FEEDRATE = 3000  # mm/min
    TOLERANCE = 0.01  # tolerance for position sync in mm

    PROTOCOL_TERMINATOR = "\r"
    VALID_AXES = "XYZA"

    def __init__(
        self,
        port_name: Optional[str] = None,
        baudrate: int = SerialController.DEFAULT_BAUDRATE,
        timeout: int = SerialController.DEFAULT_TIMEOUT,
        feed: int = DEFAULT_FEEDRATE,
    ):
        """
        Initialize the G-code controller.

        Args:
            port_name: Serial port name (e.g., '/dev/ttyACM0' or 'COM3')
            baudrate: Baud rate for serial communication. Defaults to 9600.
            timeout: Timeout in seconds for operations. Defaults to 20.
            feed: Initial feed rate in mm/min. Defaults to 3000.
        """
        super().__init__(port_name, baudrate, timeout)

        self._logger = logging.getLogger(__name__)
        self._logger.info(
            "GCodeController initialized with port='%s', baudrate=%s, timeout=%s",
            port_name,
            baudrate,
            timeout,
        )

        # Tracks internal position state
        self.current_position: Dict[str, float] = {
            "X": 0.0,
            "Y": 0.0,
            "Z": 0.0,
            "A": 0.0,
        }
        self._feed: int = feed

        # Initialize axis limits with default values
        self._axis_limits: Dict[str, AxisLimits] = {
            "X": AxisLimits(0, 0),
            "Y": AxisLimits(0, 0),
            "Z": AxisLimits(0, 0),
            "A": AxisLimits(0, 0),
        }

    @property
    def feed(self) -> int:
        """Get the current feed rate in mm/min."""
        return self._feed

    @feed.setter
    def feed(self, new_feed: int) -> None:
        """
        Set the movement feed rate, enforcing the maximum limit.

        Args:
            new_feed: New feed rate in mm/min (must be > 0)

        Raises:
            ValueError: If feed rate is not positive
        """
        if new_feed <= 0:
            error_msg = (
                f"Attempted to set invalid feed rate: {new_feed}. Must be > 0."
            )
            self._logger.error(error_msg)
            raise ValueError(error_msg)

        if new_feed > self.MAX_FEEDRATE:
            self._logger.warning(
                "Requested feed rate (%s) exceeds maximum (%s). "
                "Setting feed rate to maximum: %s.",
                new_feed,
                self.MAX_FEEDRATE,
                self.MAX_FEEDRATE,
            )
            self._feed = self.MAX_FEEDRATE
        else:
            self._feed = new_feed
            self._logger.debug("Feed rate set to: %s mm/min.", self._feed)

    def _build_command(self, command: str) -> str:
        """
        Build a G-code command with terminator.

        Args:
            command: G-code command string (without terminator)

        Returns:
            Complete command string with terminator
        """
        return f"{command}{self.PROTOCOL_TERMINATOR}"

    def _validate_axis(self, axis: str) -> str:
        """
        Validate and normalize an axis name.

        Args:
            axis: Axis name to validate

        Returns:
            Uppercase axis name

        Raises:
            ValueError: If axis is not valid
        """
        axis_upper = axis.upper()
        if axis_upper not in self.VALID_AXES:
            self._logger.error(
                "Invalid axis '%s' provided. Must be one of: %s.",
                axis_upper,
                ", ".join(self.VALID_AXES),
            )
            raise ValueError(
                f"Invalid axis. Must be one of: {', '.join(self.VALID_AXES)}."
            )
        return axis_upper

    def _validate_move_positions(
        self,
        x: Optional[float] = None,
        y: Optional[float] = None,
        z: Optional[float] = None,
        a: Optional[float] = None,
    ) -> None:
        """
        Validate that move positions are within axis limits.

        Only validates axes that are being moved (not None). Raises ValueError
        if any position is outside the configured limits.

        Args:
            x: Target X position (optional)
            y: Target Y position (optional)
            z: Target Z position (optional)
            a: Target A position (optional)

        Raises:
            ValueError: If any position is outside the axis limits
        """
        if x is not None:
            if "X" in self._axis_limits:
                try:
                    self._axis_limits["X"].validate(x)
                except ValueError as e:
                    self._logger.error("Move validation failed for X axis: %s", e)
                    raise
        if y is not None:
            if "Y" in self._axis_limits:
                try:
                    self._axis_limits["Y"].validate(y)
                except ValueError as e:
                    self._logger.error("Move validation failed for Y axis: %s", e)
                    raise
        if z is not None:
            if "Z" in self._axis_limits:
                try:
                    self._axis_limits["Z"].validate(z)
                except ValueError as e:
                    self._logger.error("Move validation failed for Z axis: %s", e)
                    raise
        if a is not None:
            if "A" in self._axis_limits:
                try:
                    self._axis_limits["A"].validate(a)
                except ValueError as e:
                    self._logger.error("Move validation failed for A axis: %s", e)
                    raise

    def set_axis_limits(
        self, axis: str, min_val: float, max_val: float
    ) -> None:
        """
        Set the min/max limits for an axis.

        Args:
            axis: Axis name ('X', 'Y', 'Z', 'A')
            min_val: Minimum allowed value
            max_val: Maximum allowed value

        Raises:
            ValueError: If axis is unknown or min >= max
        """
        axis = self._validate_axis(axis)

        if min_val >= max_val:
            raise ValueError("min must be < max")

        self._axis_limits[axis] = AxisLimits(min_val, max_val)
        self._logger.info(
            "Set limits for axis %s: [%s, %s]", axis, min_val, max_val
        )

    def get_axis_limits(
        self, axis: Optional[str] = None
    ) -> Union[AxisLimits, Dict[str, AxisLimits]]:
        """
        Get the current limits for an axis or all axes.

        Args:
            axis: Optional axis name ('X', 'Y', 'Z', 'A'). If None, returns all limits.

        Returns:
            If axis is specified: AxisLimits object with min and max values.
            If axis is None: Dictionary of all axis limits.

        Raises:
            ValueError: If axis is unknown (only when axis is provided)
        """
        if axis is None:
            return self._axis_limits.copy()
        axis = self._validate_axis(axis)
        return self._axis_limits[axis]

    def home(self, axis: Optional[str] = None) -> None:
        """
        Home one or all axes (G28 command).

        Args:
            axis: Optional axis to home ('X', 'Y', 'Z', 'A').
                  If None, homes all axes.

        Raises:
            ValueError: If an invalid axis is provided
        """
        if axis:
            axis = self._validate_axis(axis)
            cmd = f"G28 {axis}"
            home_target = axis
        else:
            cmd = "G28"
            home_target = "All"

        self._logger.info("[%s] homing axis/axes: %s **", cmd, home_target)
        self._send_command(self._build_command(cmd))
        self._logger.info("Homing of %s completed.", home_target)

        # Update internal position (optimistic zeroing)
        if axis:
            self.current_position[axis] = 0.0
        else:
            for key in self.current_position:
                self.current_position[key] = 0.0

        self._logger.debug(
            "Internal position updated (optimistically zeroed) to %s",
            self.current_position,
        )

    def move_absolute(
        self,
        x: Optional[float] = None,
        y: Optional[float] = None,
        z: Optional[float] = None,
        a: Optional[float] = None,
        feed: Optional[int] = None,
    ) -> None:
        """
        Move to an absolute position (G90 + G1 command).

        Args:
            x: Target X position (optional)
            y: Target Y position (optional)
            z: Target Z position (optional)
            a: Target A position (optional)
            feed: Feed rate for this move (optional, uses current feed if not specified)

        Raises:
            ValueError: If any position is outside the axis limits
        """
        # Validate positions before executing move
        self._validate_move_positions(x=x, y=y, z=z, a=a)

        feed_rate = feed if feed is not None else self._feed
        self._logger.info(
            "Preparing absolute move to X:%s, Y:%s, Z:%s, A:%s at F:%s",
            x,
            y,
            z,
            a,
            feed_rate,
        )

        self._execute_move(x=x, y=y, z=z, a=a, feed=feed)

    def move_relative(
        self,
        x: Optional[float] = None,
        y: Optional[float] = None,
        z: Optional[float] = None,
        a: Optional[float] = None,
        feed: Optional[int] = None,
    ) -> None:
        """
        Move relative to the current position (converted to absolute move internally).

        Args:
            x: Relative X movement (optional)
            y: Relative Y movement (optional)
            z: Relative Z movement (optional)
            a: Relative A movement (optional)
            feed: Feed rate for this move (optional, uses current feed if not specified)

        Raises:
            ValueError: If any resulting absolute position is outside the axis limits
        """
        feed_rate = feed if feed is not None else self._feed
        self._logger.info(
            "Preparing relative move by dX:%s, dY:%s, dZ:%s, dA:%s at F:%s",
            x,
            y,
            z,
            a,
            feed_rate,
        )

        # Convert relative movements to absolute positions
        abs_x = (self.current_position["X"] + x) if x is not None else None
        abs_y = (self.current_position["Y"] + y) if y is not None else None
        abs_z = (self.current_position["Z"] + z) if z is not None else None
        abs_a = (self.current_position["A"] + a) if a is not None else None

        # Validate absolute positions before executing move
        self._validate_move_positions(x=abs_x, y=abs_y, z=abs_z, a=abs_a)

        self._execute_move(x=abs_x, y=abs_y, z=abs_z, a=abs_a, feed=feed)

    def _execute_move(
        self,
        x: Optional[float] = None,
        y: Optional[float] = None,
        z: Optional[float] = None,
        a: Optional[float] = None,
        feed: Optional[int] = None,
    ) -> None:
        """
        Internal helper for executing G1 move commands with safe movement pattern.
        All coordinates are treated as absolute positions.

        Safe move pattern:
        1. If X or Y movement is needed, first move Z to 0 (safe height)
        2. Then move X, Y (and optionally A) to target
        3. Finally move Z to target position (if specified)

        Args:
            x: Absolute X position (optional)
            y: Absolute Y position (optional)
            z: Absolute Z position (optional)
            a: Absolute A position (optional)
            feed: Feed rate (optional)
        """
        # Calculate target positions (all absolute)
        target_pos = self.current_position.copy()
        has_x = x is not None
        has_y = y is not None
        has_z = z is not None
        has_a = a is not None

        if not (has_x or has_y or has_z or has_a):
            self._logger.warning(
                "Move command issued without any axis movement. Skipping transmission."
            )
            return

        # Set target positions (all absolute)
        if has_x:
            target_pos["X"] = x
        if has_y:
            target_pos["Y"] = y
        if has_z:
            target_pos["Z"] = z
        if has_a:
            target_pos["A"] = a

        feed_rate = feed if feed is not None else self._feed
        if feed_rate > self.MAX_FEEDRATE:
            feed_rate = self.MAX_FEEDRATE

        # Ensure absolute mode is active
        self._send_command(self._build_command("G90"))

        # Safe move pattern: Z to 0, then XY, then Z to target
        needs_xy_move = has_x or has_y
        current_z = self.current_position["Z"]
        target_z = target_pos["Z"] if has_z else current_z

        # Step 1: Move Z to safe height (0) if XY movement is needed and Z is not already at 0
        if needs_xy_move and abs(current_z) > 0.001:  # Small tolerance for floating point
            # Validate safe height (Z=0) is within limits
            self._validate_move_positions(z=0.0)
            self._logger.info(
                "Safe move: Raising Z to safe height (0) before XY movement"
            )
            move_cmd = f"G1 Z0 F{feed_rate}"
            self._send_command(self._build_command(move_cmd))
            self.current_position["Z"] = 0.0
            self._logger.debug("Z moved to safe height (0)")

        # Step 2: Move X, Y (and optionally A) to target
        if needs_xy_move or has_a:
            move_cmd = "G1"
            if has_x:
                move_cmd += f" X{target_pos['X']}"
            if has_y:
                move_cmd += f" Y{target_pos['Y']}"
            if has_a:
                move_cmd += f" A{target_pos['A']}"
            move_cmd += f" F{feed_rate}"

            self._logger.info("Executing XY move command: %s", move_cmd)
            self._send_command(self._build_command(move_cmd))

            # Update position for moved axes
            if has_x:
                self.current_position["X"] = target_pos["X"]
            if has_y:
                self.current_position["Y"] = target_pos["Y"]
            if has_a:
                self.current_position["A"] = target_pos["A"]

        # Step 3: Move Z to target position (if Z movement was requested)
        if has_z:
            z_needs_move = abs(target_z - (0.0 if needs_xy_move else current_z)) > 0.001
            if z_needs_move:
                move_cmd = f"G1 Z{target_z} F{feed_rate}"
                
                if needs_xy_move:
                    self._logger.info(
                        "Safe move: Lowering Z to target position: %s", target_z
                    )
                else:
                    self._logger.info("Executing Z move command: %s", move_cmd)
                
                self._send_command(self._build_command(move_cmd))
                self.current_position["Z"] = target_z

        self._logger.info(
            "Move complete. Final position: %s", self.current_position
        )
        self._logger.debug("New internal position: %s", self.current_position)

        # Post-move position synchronization check
        self.sync_position()

    def query_position(self) -> Dict[str, float]:
        """
        Query the current machine position (M114 command).

        Returns:
            Dictionary containing X, Y, Z, and A positions

        Note:
            Returns an empty dictionary if the query fails or no positions are found.
        """
        self._logger.info("Querying current machine position (M114).")
        res: str = self.execute(self._build_command("M114"))

        # Extract position values using regex
        pattern = re.compile(r"([XYZA]):(-?\d+\.\d+)")
        matches = pattern.findall(res)

        position_data: Dict[str, float] = {}

        for axis, value_str in matches:
            try:
                position_data[axis] = float(value_str)
            except ValueError:
                self._logger.error(
                    "Failed to convert position value '%s' for axis %s to float.",
                    value_str,
                    axis,
                )
                continue

        return position_data

    def sync_position(self) -> Tuple[bool, Dict[str, float]]:
        """
        Synchronize internal position with actual machine position.

        Queries the machine position and compares it with the internal position.
        If a discrepancy greater than the tolerance is found, attempts to correct
        it by moving to the internal position.

        Returns:
            Tuple of (adjustment_occurred: bool, final_position: Dict[str, float])
            where adjustment_occurred is True if a correction move was made.

        Note:
            This method may recursively call itself if a correction move is made.
        """
        self._logger.info("Starting position synchronization check (M114).")

        # Query the actual machine position
        queried_position = self.query_position()

        if not queried_position:
            self._logger.warning("Query position failed. Cannot synchronize.")
            return False, self.current_position

        # Compare internal vs. queried position
        axis_keys = ["X", "Y", "Z", "A"]
        adjustment_needed = False

        for axis in axis_keys:
            if (
                axis in self.current_position
                and axis in queried_position
                and abs(self.current_position[axis] - queried_position[axis])
                > self.TOLERANCE
            ):
                self._logger.warning(
                    "Position mismatch found on %s axis: Internal=%.3f, Queried=%.3f",
                    axis,
                    self.current_position[axis],
                    queried_position[axis],
                )
                adjustment_needed = True
            elif axis in queried_position:
                # Update internal position with queried position if it differs slightly
                self.current_position[axis] = queried_position[axis]

        # Perform re-synchronization move if needed
        if adjustment_needed:
            self._logger.info(
                "** DISCREPANCY DETECTED. Moving robot back to internal position: %s **",
                self.current_position,
            )

            try:
                target_x = self.current_position.get("X")
                target_y = self.current_position.get("Y")
                target_z = self.current_position.get("Z")
                target_a = self.current_position.get("A")

                self.move_absolute(x=target_x, y=target_y, z=target_z, a=target_a)
                self._logger.info("Synchronization move successfully completed.")

                # Recursive call to verify position after move
                return self.sync_position()
            except (ValueError, RuntimeError, OSError) as e:
                self._logger.error("Synchronization move failed: %s", e)
                adjustment_needed = False

        if adjustment_needed:
            self._logger.info(
                "Position check complete. Internal position is synchronized with machine."
            )
        else:
            self._logger.info("No adjustment was made.")

        return adjustment_needed, self.current_position.copy()

    def get_info(self) -> str:
        """
        Query machine information (M115 command).

        Returns:
            Machine information string from the device
        """
        self._logger.info("Querying machine information (M115).")
        return self.execute(self._build_command("M115"))

    def get_internal_position(self) -> Dict[str, float]:
        """
        Get the internally tracked position.

        Returns:
            Dictionary containing the current internal position for all axes
        """
        self._logger.debug("Returning internal position: %s", self.current_position)
        return self.current_position.copy()
