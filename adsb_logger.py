import json
import time
import mysql.connector
from mysql.connector import errorcode, pooling
from datetime import datetime
import smtplib
import ssl
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import signal
import threading
import logging
import logging.handlers
import os
import csv
from collections import OrderedDict
import contextlib
from typing import Dict, Any, Optional

# --- CONFIGURATION MANAGEMENT ---
class Config:
    def __init__(self):
        # Default configuration
        self.AIRCRAFT_JSON_PATH = os.getenv('AIRCRAFT_JSON_PATH', 'your-path/aircraft.json')
        self.AIRCRAFT_CSV_PATH = os.getenv('AIRCRAFT_CSV_PATH', 'your-path/aircraft.csv')
        self.SUMMARY_UPLOAD_INTERVAL = int(os.getenv('SUMMARY_UPLOAD_INTERVAL', '300'))
        self.MAX_CACHE_SIZE = int(os.getenv('MAX_CACHE_SIZE', '200'))
        self.MAX_RETRY_ATTEMPTS = int(os.getenv('MAX_RETRY_ATTEMPTS', '3'))
        self.RETRY_DELAY = int(os.getenv('RETRY_DELAY', '10'))
        self.CACHE_TTL_SECONDS = int(os.getenv('CACHE_TTL_SECONDS', '3600'))  # 1 hour TTL
        self.AIRCRAFT_CSV_TTL_SECONDS = int(os.getenv('AIRCRAFT_CSV_TTL_SECONDS', '86400'))  # 24 hour TTL for CSV cache
        self.DB_POOL_SIZE = int(os.getenv('DB_POOL_SIZE', '5'))
        self.CIRCUIT_BREAKER_THRESHOLD = int(os.getenv('CIRCUIT_BREAKER_THRESHOLD', '5'))
        self.CIRCUIT_BREAKER_TIMEOUT = int(os.getenv('CIRCUIT_BREAKER_TIMEOUT', '60'))
        self.LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
        self.LOG_FILE = os.getenv('LOG_FILE', 'adsb_logger.log')
        self.LOG_MAX_BYTES = int(os.getenv('LOG_MAX_BYTES', '10485760'))  # 10MB
        self.LOG_BACKUP_COUNT = int(os.getenv('LOG_BACKUP_COUNT', '5'))
        self.LOG_CLEANUP_INTERVAL_HOURS = int(os.getenv('LOG_CLEANUP_INTERVAL_HOURS', '48'))  # 48 hours

config = Config()

# --- VISUAL FORMATTING CLASSES ---
class Colors:
    """ANSI color codes for terminal formatting"""
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    MAGENTA = '\033[95m'
    PURPLE = '\033[95m'
    WHITE = '\033[97m'
    BOLD = '\033[1m'
    DIM = '\033[2m'
    UNDERLINE = '\033[4m'
    RESET = '\033[0m'

    # Background colors
    BG_BLACK = '\033[40m'
    BG_GREEN = '\033[42m'
    BG_YELLOW = '\033[43m'
    BG_RED = '\033[41m'

class DynamicConsoleHandler(logging.StreamHandler):
    """Custom handler that manages the dynamic dashboard display"""

    def __init__(self):
        super().__init__()
        self.formatter_instance = None
        self.dashboard_active = False

    def emit(self, record):
        try:
            msg = self.format(record)
            if msg:  # Only emit if there's content
                self.stream.write(msg + '\n')
                self.stream.flush()
        except Exception:
            self.handleError(record)

class ApplicationFormatter(logging.Formatter):
    """Custom formatter for application-like console output with dynamic updates"""

    def __init__(self):
        super().__init__()
        self.startup_shown = False
        self.dashboard_lines = 4  # Fixed dashboard size
        self.dashboard_initialized = False
        self.last_refresh_time = 0
        self.status_change_timer = None
        self._main_lock = threading.RLock()  # Single main lock to prevent deadlocks
        self._dashboard_update_pending = False
        self._cursor_position_saved = False
        self._consolidation_timer = None
        self._timer_lock = threading.Lock()  # Separate lock only for timer management
        self._terminal_initialized = False

    def format(self, record):
        # Get timestamp
        timestamp = datetime.fromtimestamp(record.created).strftime('%H:%M:%S')

        # Message content
        msg = record.getMessage()

        # Application header (show once) - now just initializes without showing the big header
        if not self.startup_shown and "initialized with production features" in msg:
            self.startup_shown = True
            self.dashboard_initialized = True
            # Don't reserve lines here - let _refresh_dashboard handle exact positioning
            return ""  # Return empty to prevent initial spacing issues

        # Skip certain messages that shouldn't be displayed in the dynamic interface
        if any(skip in msg for skip in [
            "Configuration:",
            "Startup stats:",
            "Processing",
            "Finished processing"
        ]):
            return ""  # Return empty string to suppress these messages

        # Handle dynamic updates by directly printing to stdout and returning empty string
        # Only trigger refresh on key status changes, not every message
        if any(dynamic in msg for dynamic in [
            "Data collection:",
            "Cache status:",
            "Initiating summary",  # Catches both "Initiating summary data upload" and "Initiating scheduled summary"
            "SUCCESS: Committed",
            "Health check:",
            "Summary upload complete",
            "Finished processing",
            "Processing"
        ]):
            self._handle_dynamic_message(timestamp, msg)
            return ""  # Return empty to prevent double printing
        elif "Sleeping for" in msg:
            # For sleep messages, update status and force a refresh to show STANDBY
            with self._main_lock:
                ApplicationFormatter.current_status = "STANDBY"
                self._dashboard_update_pending = True
            self._schedule_consolidated_refresh()
            return ""

        # Handle errors and warnings normally
        elif "ERROR" in record.levelname:
            return self._format_error(timestamp, msg)
        elif "WARNING" in record.levelname:
            return self._format_warning(timestamp, msg)
        else:
            return ""  # Suppress other INFO messages for cleaner display

    def _format_startup_header(self):
        # Calculate exact character counts - the top border is 79 chars, so content should be 79 chars
        line1 = " " * 79

        # Line 2: "      AUTOMATED DEPENDENT SURVEILLANCE-BROADCAST MONITORING SYSTEM"
        text2 = "AUTOMATED DEPENDENT SURVEILLANCE-BROADCAST MONITORING SYSTEM"
        line2 = (" " * 6) + text2 + (" " * (79 - 6 - len(text2)))

        # Line 3: "                           VERSION 2.0 - OPERATIONAL"
        text3 = "VERSION 2.0 - OPERATIONAL"
        line3 = (" " * 27) + text3 + (" " * (79 - 27 - len(text3)))

        line4 = " " * 79

        # Line 5: " [INIT] DATABASE_POOL........ONLINE  [5_CONNECTIONS]"
        text5 = " [INIT] DATABASE_POOL........ONLINE  [5_CONNECTIONS]"
        line5 = text5 + (" " * (79 - len(text5)))

        # Line 6: " [INIT] CIRCUIT_BREAKERS.....ARMED   [FAIL_SAFE_MODE]"
        text6 = " [INIT] CIRCUIT_BREAKERS.....ARMED   [FAIL_SAFE_MODE]"
        line6 = text6 + (" " * (79 - len(text6)))

        # Line 7: " [INIT] TTL_CACHE............READY   [3600S_TIMEOUT]"
        text7 = " [INIT] TTL_CACHE............READY   [3600S_TIMEOUT]"
        line7 = text7 + (" " * (79 - len(text7)))

        # Line 8: " [INIT] UPLINK_PROTOCOL......ACTIVE  [300S_INTERVAL]"
        text8 = " [INIT] UPLINK_PROTOCOL......ACTIVE  [300S_INTERVAL]"
        line8 = text8 + (" " * (79 - len(text8)))

        # Line 9: "                            SYSTEM STATUS MONITOR"
        text9 = "SYSTEM STATUS MONITOR"
        line9 = (" " * 29) + text9 + (" " * (79 - 29 - len(text9)))

        header = f"""
{Colors.BOLD}{Colors.GREEN}╔═══════════════════════════════════════════════════════════════════════════════╗{Colors.RESET}
{Colors.BOLD}{Colors.GREEN}║{Colors.RESET}{line1}{Colors.BOLD}{Colors.GREEN}║{Colors.RESET}
{Colors.BOLD}{Colors.GREEN}║{Colors.RESET}      {Colors.BOLD}{Colors.CYAN}{text2}{Colors.RESET}{' ' * (79 - 6 - len(text2))}{Colors.BOLD}{Colors.GREEN}║{Colors.RESET}
{Colors.BOLD}{Colors.GREEN}║{Colors.RESET}{' ' * 27}{Colors.BOLD}{text3}{Colors.RESET}{' ' * (79 - 27 - len(text3))}{Colors.BOLD}{Colors.GREEN}║{Colors.RESET}
{Colors.BOLD}{Colors.GREEN}║{Colors.RESET}{line4}{Colors.BOLD}{Colors.GREEN}║{Colors.RESET}
{Colors.BOLD}{Colors.GREEN}╠═══════════════════════════════════════════════════════════════════════════════╣{Colors.RESET}
{Colors.BOLD}{Colors.GREEN}║{Colors.RESET}{Colors.GREEN}{text5}{Colors.RESET}{' ' * (79 - len(text5))}{Colors.BOLD}{Colors.GREEN}║{Colors.RESET}
{Colors.BOLD}{Colors.GREEN}║{Colors.RESET}{Colors.GREEN}{text6}{Colors.RESET}{' ' * (79 - len(text6))}{Colors.BOLD}{Colors.GREEN}║{Colors.RESET}
{Colors.BOLD}{Colors.GREEN}║{Colors.RESET}{Colors.GREEN}{text7}{Colors.RESET}{' ' * (79 - len(text7))}{Colors.BOLD}{Colors.GREEN}║{Colors.RESET}
{Colors.BOLD}{Colors.GREEN}║{Colors.RESET}{Colors.GREEN}{text8}{Colors.RESET}{' ' * (79 - len(text8))}{Colors.BOLD}{Colors.GREEN}║{Colors.RESET}
{Colors.BOLD}{Colors.GREEN}╠═══════════════════════════════════════════════════════════════════════════════╣{Colors.RESET}
{Colors.BOLD}{Colors.GREEN}║{Colors.RESET}{' ' * 29}{Colors.BOLD}{Colors.YELLOW}{text9}{Colors.RESET}{' ' * (79 - 29 - len(text9))}{Colors.BOLD}{Colors.GREEN}║{Colors.RESET}
{Colors.BOLD}{Colors.GREEN}╚═══════════════════════════════════════════════════════════════════════════════╝{Colors.RESET}

"""
        # Reserve space for the mainframe-style dashboard (5 lines)
        self.dashboard_lines = 5
        return header + "\n" * self.dashboard_lines

    def _handle_dynamic_message(self, timestamp, msg):
        """Handle dynamic dashboard updates with atomic state management"""
        if not self.dashboard_initialized:
            return

        # Determine message type and only process significant changes
        msg_type = self._determine_message_type(msg)

        # Debug logging for critical upload messages
        if msg_type in ["upload_success", "upload_complete"]:
            import logging
            logger = logging.getLogger('adsb_logger.formatter')
            logger.debug(f"Handling critical message type '{msg_type}': {msg[:50]}...")

        # Use single main lock to prevent deadlocks
        with self._main_lock:
            self._build_dashboard(timestamp, msg, msg_type)
            # Mark that an update is pending instead of immediately refreshing
            self._dashboard_update_pending = True

        # Schedule a single consolidated refresh
        self._schedule_consolidated_refresh()

    def _determine_message_type(self, msg):
        """Determine the message type for dashboard updates"""
        if "Cache status:" in msg:
            return "cache"
        elif "Initiating summary" in msg:
            return "upload_start"
        elif "SUCCESS: Committed" in msg:
            return "upload_success"
        elif "Summary upload complete" in msg:
            return "upload_complete"
        elif "Health check:" in msg:
            return "health"
        elif "Data collection:" in msg:
            return "data"
        elif "Processing" in msg:
            return "processing"
        elif "Finished processing" in msg:
            return "processing_complete"
        else:
            return "other"

    @classmethod
    def update_aircraft_data(cls, aircraft_list):
        """Update the current aircraft data for display"""
        cls.current_aircraft_data = aircraft_list

    def _schedule_status_change(self, new_status, delay_seconds):
        """Schedule a status change after a delay with consolidated refresh"""
        with self._timer_lock:
            # Cancel any existing timer
            if hasattr(self, 'status_change_timer') and self.status_change_timer:
                self.status_change_timer.cancel()

            def change_status():
                with self._main_lock:
                    ApplicationFormatter.current_status = new_status
                    self._dashboard_update_pending = True
                # Use consolidated refresh instead of direct refresh
                self._schedule_consolidated_refresh()

            # Schedule the status change
            self.status_change_timer = threading.Timer(delay_seconds, change_status)
            self.status_change_timer.daemon = True
            self.status_change_timer.start()

    def _schedule_consolidated_refresh(self):
        """Schedule a single consolidated dashboard refresh to prevent duplicates"""
        import time
        import threading

        with self._timer_lock:
            # Cancel any existing consolidation timer
            if self._consolidation_timer and self._consolidation_timer.is_alive():
                self._consolidation_timer.cancel()

            def consolidated_refresh():
                time.sleep(0.05)  # Reduced delay for more responsive updates
                # Use try-finally to ensure timer cleanup
                try:
                    with self._main_lock:
                        if self._dashboard_update_pending:
                            self._dashboard_update_pending = False
                            self._refresh_dashboard()
                finally:
                    with self._timer_lock:
                        self._consolidation_timer = None

            self._consolidation_timer = threading.Timer(0.05, consolidated_refresh)
            self._consolidation_timer.daemon = True
            self._consolidation_timer.start()

    def _refresh_dashboard(self):
        """Refresh the dashboard display with proper terminal control and synchronization"""
        import sys
        import time

        # This method should only be called when _main_lock is already held
        current_time = time.time()
        # Reduced throttling time and more reliable refresh
        if current_time - self.last_refresh_time < 0.5:
            return
        self.last_refresh_time = current_time

        try:
            # Initialize terminal state properly on first use
            if not self._terminal_initialized:
                # Simple terminal initialization without alternate screen buffer
                sys.stdout.write("\033[s")     # Save initial cursor position
                self._terminal_initialized = True
                self._cursor_position_saved = True

            # Restore to saved position and clear from cursor down
            if self._cursor_position_saved:
                sys.stdout.write("\033[u")  # Restore to saved position
                sys.stdout.write("\033[0J")  # Clear from cursor to end of screen
            else:
                # Fallback: just clear from current position
                sys.stdout.write("\033[0J")

            # Get the current dashboard content
            dashboard = self._get_current_dashboard()

            # Write the complete dashboard atomically
            sys.stdout.write(dashboard)
            sys.stdout.flush()

            # Save position after successful update only if we don't have one saved already
            if not self._cursor_position_saved:
                sys.stdout.write("\033[s")
                self._cursor_position_saved = True

            # Debug logging for successful refreshes during critical periods
            import logging
            logger = logging.getLogger('adsb_logger.formatter')
            if hasattr(ApplicationFormatter, 'current_status') and "UPLOAD" in ApplicationFormatter.current_status:
                logger.debug(f"Dashboard refresh successful during: {ApplicationFormatter.current_status}")

        except Exception as e:
            # Log error but don't crash the application
            import logging
            logger = logging.getLogger('adsb_logger.formatter')
            logger.debug(f"Dashboard refresh error: {e}")
            # Minimal terminal recovery - don't reset everything
            try:
                sys.stdout.write("\033[0J")  # Clear from cursor down
                sys.stdout.flush()
                logger.debug("Terminal display cleared after refresh error")
            except Exception as reset_error:
                logger.debug(f"Failed to clear terminal display: {reset_error}")

    def _get_current_dashboard(self):
        """Get the current mainframe-style technical dashboard display"""
        # Create gradient progress bar for cached aircraft
        cached_num = int(ApplicationFormatter.last_cached_count)
        cache_percent = min(int((cached_num / 200) * 100), 100) if cached_num > 0 else 0  # Updated for 200 max cache
        progress = min(cached_num // 10, 20) if cached_num > 0 else 0  # Scale adjusted for 200 max

        # Create gradient colored bar: green (empty) -> red (medium) -> purple (full)
        bar_chars = []
        for i in range(20):
            if i < progress:
                # Calculate color based on fill percentage for gradient effect
                if cache_percent <= 33:  # Green zone (0-33% - low usage)
                    bar_chars.append(f"{Colors.GREEN}█{Colors.RESET}")
                elif cache_percent <= 70:  # Red zone (34-70% - medium usage)
                    bar_chars.append(f"{Colors.RED}█{Colors.RESET}")
                else:  # Purple zone (71-100% - high usage, approaching limit)
                    bar_chars.append(f"{Colors.PURPLE}█{Colors.RESET}")
            else:
                bar_chars.append("░")

        bar = "".join(bar_chars)

        # Status indicators
        data_status = "ONLINE" if int(ApplicationFormatter.last_aircraft_count) > 0 else "OFFLINE"
        data_color = Colors.GREEN if data_status == "ONLINE" else Colors.RED

        # Current operation status
        operation_status = ApplicationFormatter.current_status
        if operation_status == "UPLOADING":
            op_color = Colors.YELLOW
            op_indicator = "[XMIT]"
        elif "SUCCESS" in operation_status:
            op_color = Colors.GREEN
            op_indicator = "[SYNC]"
        elif operation_status == "STANDBY":
            op_color = Colors.CYAN
            op_indicator = "[IDLE]"
        else:
            op_color = Colors.GREEN
            op_indicator = "[PROC]"

        # Create base text without colors first (exactly 74 characters)
        line1_base = f" SENSORS: {data_status.ljust(7)} │ TARGETS: {ApplicationFormatter.last_aircraft_count.rjust(4)} │ STATUS: {op_indicator} {operation_status.ljust(16)[:16]} "
        line2_base = f" BUFFER: [{bar}] {cache_percent:3d}% │ STORED: {ApplicationFormatter.last_cached_count.rjust(4)} │ ACTIVE: {ApplicationFormatter.last_active_count.rjust(4)} "
        line3_base = f" LAST SCAN: {ApplicationFormatter.last_data_collection_time.ljust(8)} │ LAST UPLINK: {ApplicationFormatter.last_database_upload_time.ljust(8)} "

        # Calculate visible text length (exclude ANSI color codes from length calculations)
        import re

        # Strip ANSI codes from bar to get visible length
        bar_visible = re.sub(r'\033\[[0-9;]*m', '', bar)

        line1_visible_len = len(f" SENSORS: {data_status.ljust(7)} │ TARGETS: {ApplicationFormatter.last_aircraft_count.rjust(4)} │ STATUS: {op_indicator} {operation_status.ljust(16)[:16]} ")
        line2_visible_len = len(f" BUFFER: [{bar_visible}] {cache_percent:3d}% │ STORED: {ApplicationFormatter.last_cached_count.rjust(4)} │ ACTIVE: {ApplicationFormatter.last_active_count.rjust(4)} ")
        line3_visible_len = len(f" LAST SCAN: {ApplicationFormatter.last_data_collection_time.ljust(8)} │ LAST UPLINK: {ApplicationFormatter.last_database_upload_time.ljust(8)} ")

        # Pad base text to exactly 74 visible characters
        line1_padded = line1_base + (" " * max(0, (74 - line1_visible_len)))
        line2_padded = line2_base + (" " * max(0, (74 - line2_visible_len)))
        line3_padded = line3_base + (" " * max(0, (74 - line3_visible_len)))

        # Now apply colors to the padded text by replacing specific parts
        line1 = line1_padded.replace(data_status.ljust(7), f"{data_color}{data_status.ljust(7)}{Colors.RESET}")
        line1 = line1.replace(ApplicationFormatter.last_aircraft_count.rjust(4), f"{Colors.BOLD}{ApplicationFormatter.last_aircraft_count.rjust(4)}{Colors.RESET}")
        line1 = line1.replace(op_indicator, f"{op_color}{op_indicator}{Colors.RESET}")

        line2 = line2_padded.replace(f"[{bar}]", f"[{Colors.CYAN}{bar}{Colors.RESET}]")
        line2 = line2.replace(ApplicationFormatter.last_cached_count.rjust(4), f"{Colors.BOLD}{ApplicationFormatter.last_cached_count.rjust(4)}{Colors.RESET}")
        line2 = line2.replace(ApplicationFormatter.last_active_count.rjust(4), f"{Colors.BOLD}{ApplicationFormatter.last_active_count.rjust(4)}{Colors.RESET}")

        line3 = line3_padded.replace(ApplicationFormatter.last_data_collection_time.ljust(8), f"{Colors.YELLOW}{ApplicationFormatter.last_data_collection_time.ljust(8)}{Colors.RESET}")
        line3 = line3.replace(ApplicationFormatter.last_database_upload_time.ljust(8), f"{Colors.YELLOW}{ApplicationFormatter.last_database_upload_time.ljust(8)}{Colors.RESET}")

        # Get aircraft list for display
        aircraft_display = self._get_aircraft_display()

        # Build dashboard string with proper formatting
        dashboard = f"{Colors.BOLD}{Colors.GREEN}┌─ SURVEILLANCE RADAR DATA ACQUISITION SYSTEM ─────────────────────────────┐{Colors.RESET}\n"
        dashboard += f"{Colors.BOLD}{Colors.GREEN}│{Colors.RESET}{line1}{Colors.BOLD}{Colors.GREEN}│{Colors.RESET}\n"
        dashboard += f"{Colors.BOLD}{Colors.GREEN}│{Colors.RESET}{line2}{Colors.BOLD}{Colors.GREEN}│{Colors.RESET}\n"
        dashboard += f"{Colors.BOLD}{Colors.GREEN}│{Colors.RESET}{line3}{Colors.BOLD}{Colors.GREEN}│{Colors.RESET}\n"
        dashboard += f"{Colors.BOLD}{Colors.GREEN}└──────────────────────────────────────────────────────────────────────────┘{Colors.RESET}{aircraft_display}\n"
        return dashboard

    def _get_aircraft_display(self):
        """Generate compact aircraft list display with formatted header"""
        if not ApplicationFormatter.current_aircraft_data:
            no_aircraft_text = "No aircraft currently tracked".center(74)
            return f"\n{Colors.BOLD}{Colors.GREEN}┌─ ACTIVE AIRCRAFT (0) " + "─" * 51 + "┐{Colors.RESET}\n{Colors.BOLD}{Colors.GREEN}│{Colors.RESET}{Colors.DIM}{no_aircraft_text}{Colors.RESET}{Colors.BOLD}{Colors.GREEN}│{Colors.RESET}\n{Colors.BOLD}{Colors.GREEN}└" + "─" * 74 + "┘{Colors.RESET}\n"

        # Sort aircraft by hex code for consistent display
        sorted_aircraft = sorted(ApplicationFormatter.current_aircraft_data, key=lambda x: x.get('hex', ''))
        display_count = min(len(sorted_aircraft), 15)

        lines = []

        # Header box with exact spacing (74 chars total width)
        header_text = f"ACTIVE AIRCRAFT ({len(sorted_aircraft)})"
        header_line = f"─ {header_text} " + "─" * (74 - len(header_text) - 3)
        lines.append(f"{Colors.BOLD}{Colors.GREEN}┌{header_line}┐{Colors.RESET}")

        # Column headers with proper alignment and spacing - updated for new fields
        col_header_text = " HEX     REGISTRATION  TYPE  ALTITUDE   SPEED   FLIGHT  "
        col_header_padded = col_header_text.ljust(74)
        lines.append(f"{Colors.BOLD}{Colors.GREEN}│{Colors.RESET}{Colors.BOLD}{Colors.CYAN}{col_header_padded}{Colors.RESET}{Colors.BOLD}{Colors.GREEN}│{Colors.RESET}")
        lines.append(f"{Colors.BOLD}{Colors.GREEN}├{'─' * 74}┤{Colors.RESET}")

        for aircraft in sorted_aircraft[:15]:  # Show max 15 aircraft to avoid screen clutter
            hex_code = aircraft.get('hex', 'UNKNOWN')
            registration = aircraft.get('registration', 'N/A') or 'N/A'
            type_code = aircraft.get('type_code', 'N/A') or 'N/A'
            flight = aircraft.get('flight', '').strip() or 'N/A'
            alt = aircraft.get('alt_baro', aircraft.get('alt_geom', 'N/A'))
            speed = aircraft.get('gs', 'N/A')

            # Format altitude
            if isinstance(alt, (int, float)) and alt != 'N/A':
                alt_str = f"{int(alt)}ft"
            else:
                alt_str = "N/A"

            # Format speed
            if isinstance(speed, (int, float)) and speed != 'N/A':
                speed_str = f"{int(speed)}kt"
            else:
                speed_str = "N/A"

            # Create formatted line with proper column widths to match headers exactly
            # HEX(6) + space(2) + REG(12) + space(2) + TYPE(4) + space(2) + ALT(9) + space(2) + SPD(6) + space(2) + FLIGHT(8) + padding
            aircraft_text = f" {hex_code:<6}  {registration:<12}  {type_code:<4}  {alt_str:<9}  {speed_str:<6}  {flight:<8}"
            aircraft_padded = aircraft_text.ljust(74)

            # Apply colors to the formatted text
            aircraft_colored = f" {Colors.YELLOW}{hex_code:<6}{Colors.RESET}  {Colors.BLUE}{registration:<12}{Colors.RESET}  {Colors.MAGENTA}{type_code:<4}{Colors.RESET}  {Colors.CYAN}{alt_str:<9}{Colors.RESET}  {Colors.GREEN}{speed_str:<6}{Colors.RESET}  {Colors.WHITE}{flight:<8}{Colors.RESET}"
            # Calculate padding to reach exactly 74 characters
            visible_length = len(f" {hex_code:<6}  {registration:<12}  {type_code:<4}  {alt_str:<9}  {speed_str:<6}  {flight:<8}")
            padding_needed = max(0, 74 - visible_length)
            aircraft_colored += " " * padding_needed

            lines.append(f"{Colors.BOLD}{Colors.GREEN}│{Colors.RESET}{aircraft_colored}{Colors.BOLD}{Colors.GREEN}│{Colors.RESET}")

        # Footer
        if len(sorted_aircraft) > 15:
            more_text = f"... and {len(sorted_aircraft) - 15} more aircraft"
            more_text_padded = more_text.center(74)
            lines.append(f"{Colors.BOLD}{Colors.GREEN}├{'─' * 74}┤{Colors.RESET}")
            lines.append(f"{Colors.BOLD}{Colors.GREEN}│{Colors.RESET}{Colors.DIM}{more_text_padded}{Colors.RESET}{Colors.BOLD}{Colors.GREEN}│{Colors.RESET}")

        lines.append(f"{Colors.BOLD}{Colors.GREEN}└{'─' * 74}┘{Colors.RESET}")
        lines.append("")  # Empty line at end

        return "\n" + "\n".join(lines)

    # Global state for the dashboard
    last_aircraft_count = "0"
    last_cached_count = "0"
    last_active_count = "0"
    current_status = "STARTING"
    last_update_time = "00:00:00"
    last_data_collection_time = "Never"
    last_database_upload_time = "Never"
    current_aircraft_data = []

    def _build_dashboard(self, timestamp, msg, msg_type):
        """Build the complete dashboard display"""
        import re

        # Update global state based on message type with delayed transitions for visibility
        if msg_type == "data":
            match = re.search(r'Read (\d+) aircraft', msg)
            ApplicationFormatter.last_aircraft_count = match.group(1) if match else "0"
            ApplicationFormatter.current_status = "DATA COLLECTION"
            ApplicationFormatter.last_update_time = timestamp
            ApplicationFormatter.last_data_collection_time = timestamp
            # Keep this status visible for 3 seconds by scheduling a delayed change
            self._schedule_status_change("STANDBY", 3.0)

        elif msg_type == "upload_start":
            ApplicationFormatter.current_status = "UPLOADING"
            # Keep this status visible for 3 seconds
            self._schedule_status_change("STANDBY", 3.0)

        elif msg_type == "upload_success":
            match = re.search(r'Committed (\d+)', msg)
            commit_count = match.group(1) if match else "0"
            ApplicationFormatter.current_status = f"UPLOAD SUCCESS ({commit_count} records)"
            ApplicationFormatter.last_database_upload_time = timestamp
            # Keep success status visible for 3 seconds, then transition to upload complete
            self._schedule_status_change("UPLOAD COMPLETE", 3.0)

        elif msg_type == "upload_complete":
            ApplicationFormatter.current_status = "UPLOAD COMPLETE"
            # Keep this status visible for 2 seconds before going to standby
            self._schedule_status_change("STANDBY", 2.0)

        elif msg_type == "processing":
            ApplicationFormatter.current_status = "PROCESSING DATA"
            # Keep this status visible for 2 seconds
            self._schedule_status_change("STANDBY", 2.0)

        elif msg_type == "processing_complete":
            ApplicationFormatter.current_status = "PROCESSING COMPLETE"
            # Keep this status visible for 2 seconds
            self._schedule_status_change("STANDBY", 2.0)

        elif msg_type == "health":
            ApplicationFormatter.current_status = "HEALTH CHECK"
            # Keep this status visible for 3 seconds
            self._schedule_status_change("STANDBY", 3.0)

        elif msg_type == "sleep":
            ApplicationFormatter.current_status = "STANDBY"

        elif msg_type == "cache":
            # This is a cache status update - just update cache numbers
            pass

        # Check if this is a cache status message
        if "Cache status:" in msg or msg_type == "cache":
            cache_match = re.search(r'(\d+) total aircraft cached', msg)
            active_match = re.search(r'(\d+) currently active', msg)
            if cache_match:
                ApplicationFormatter.last_cached_count = cache_match.group(1)
            if active_match:
                ApplicationFormatter.last_active_count = active_match.group(1)

        # Build the dashboard
        status_icon = "🟢" if int(ApplicationFormatter.last_aircraft_count) > 0 else "🔴"

        # Create progress bar for cached aircraft
        cached_num = int(ApplicationFormatter.last_cached_count)
        progress = min(cached_num // 50, 20)  # Scale to 20 chars max
        bar = "█" * progress + "░" * (20 - progress)

        # Status color based on current activity
        status_color = Colors.GREEN
        if ApplicationFormatter.current_status == "UPLOADING":
            status_color = Colors.YELLOW
        elif "SUCCESS" in ApplicationFormatter.current_status:
            status_color = Colors.GREEN
        elif ApplicationFormatter.current_status == "STANDBY":
            status_color = Colors.DIM

        # This method is no longer used - replaced by _get_current_dashboard
        return ""

    def _format_error(self, timestamp, msg):
        return f"\n{Colors.BOLD}[{timestamp}]{Colors.RESET} ❌ {Colors.BG_RED}{Colors.BOLD} ERROR {Colors.RESET}     │ {Colors.RED}{msg}{Colors.RESET}"

    def _format_warning(self, timestamp, msg):
        return f"{Colors.BOLD}[{timestamp}]{Colors.RESET} ⚠️  {Colors.BG_YELLOW} WARNING {Colors.RESET}  │ {Colors.YELLOW}{msg}{Colors.RESET}"

    def _format_info(self, timestamp, msg):
        return f"{Colors.BOLD}[{timestamp}]{Colors.RESET} ℹ️  {Colors.BLUE}INFO{Colors.RESET}        │ {msg}"

# --- DATABASE CONFIGURATION ---
DB_CONFIG = {
    'user': os.getenv('DB_USER', 'jcellaco_mom-z14'),
    'password': os.getenv('DB_PASSWORD', 'ReUHA6QTyW2yjcQ'),
    'host': os.getenv('DB_HOST', 'rs2-va.serverhostgroup.com'),
    'database': os.getenv('DB_NAME', 'jcellaco_adsb-prtr'),
    'raise_on_warnings': False,
    'connection_timeout': 30,
    'autocommit': False,
    'charset': 'utf8mb4',
    'use_unicode': True,
    'sql_mode': ''
}

# --- EMAIL NOTIFICATION CONFIGURATION ---
SMTP_SERVER = os.getenv('SMTP_SERVER', 'mail.coldwararchive.com')
SMTP_PORT = int(os.getenv('SMTP_PORT', '465'))
SENDER_EMAIL = os.getenv('SENDER_EMAIL', 'atomic@coldwararchive.com')
SENDER_PASSWORD = os.getenv('SENDER_PASSWORD', 'D0]f]7NqVN#wmBfO')
RECEIVER_EMAIL = os.getenv('RECEIVER_EMAIL', 'altair.dracolich@gmail.com')

# --- LOG FILE MANAGEMENT ---
class LogFileManager:
    """Manages log file cleanup operations."""

    def __init__(self, log_file_path: str, cleanup_interval_hours: int = 48):
        self.log_file_path = log_file_path
        self.cleanup_interval_seconds = cleanup_interval_hours * 3600
        self.last_cleanup_time = time.time()
        self.logger = logging.getLogger('adsb_logger.logmanager')
        self._cleanup_lock = threading.Lock()

    def should_cleanup(self) -> bool:
        """Check if it's time for log cleanup."""
        current_time = time.time()
        return (current_time - self.last_cleanup_time) >= self.cleanup_interval_seconds

    def cleanup_log_file(self) -> bool:
        """Empty the log file and reset the cleanup timer."""
        with self._cleanup_lock:
            try:
                # Get current log file size before cleanup
                try:
                    file_size = os.path.getsize(self.log_file_path)
                    file_size_mb = file_size / (1024 * 1024)
                except (OSError, FileNotFoundError):
                    file_size_mb = 0

                # Create backup timestamp
                cleanup_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                # Write cleanup marker before emptying
                cleanup_marker = f"\n{'='*80}\n"
                cleanup_marker += f"LOG FILE CLEANUP PERFORMED: {cleanup_timestamp}\n"
                cleanup_marker += f"Previous log size: {file_size_mb:.2f} MB\n"
                cleanup_marker += f"Cleanup interval: {self.cleanup_interval_seconds/3600:.0f} hours\n"
                cleanup_marker += f"{'='*80}\n\n"

                # Empty the log file by opening in write mode
                with open(self.log_file_path, 'w', encoding='utf-8') as f:
                    f.write(cleanup_marker)

                # Update cleanup time
                self.last_cleanup_time = time.time()

                # Log the cleanup action (this will go to the newly emptied file)
                self.logger.debug(f"Log file cleanup completed - freed {file_size_mb:.2f} MB")

                return True

            except Exception as e:
                self.logger.error(f"Failed to cleanup log file: {e}")
                return False

    def get_cleanup_status(self) -> dict:
        """Get information about cleanup status."""
        current_time = time.time()
        time_since_cleanup = current_time - self.last_cleanup_time
        time_until_cleanup = self.cleanup_interval_seconds - time_since_cleanup

        return {
            'cleanup_interval_hours': self.cleanup_interval_seconds / 3600,
            'hours_since_cleanup': time_since_cleanup / 3600,
            'hours_until_cleanup': max(0, time_until_cleanup / 3600),
            'cleanup_needed': self.should_cleanup()
        }

# --- LOGGING SETUP ---
def setup_logging() -> logging.Logger:
    """Set up structured logging with rotating file handlers."""
    logger = logging.getLogger('adsb_logger')
    logger.setLevel(getattr(logging, config.LOG_LEVEL.upper()))

    # Clear existing handlers
    logger.handlers.clear()

    # File handler with rotation
    file_handler = logging.handlers.RotatingFileHandler(
        config.LOG_FILE,
        maxBytes=config.LOG_MAX_BYTES,
        backupCount=config.LOG_BACKUP_COUNT
    )

    # Console handler with custom formatting for application-like display
    # Only show INFO and above on console, DEBUG only in file
    console_handler = DynamicConsoleHandler()
    console_handler.setLevel(logging.INFO)  # Filter out DEBUG messages from console

    # File formatter (detailed)
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Console formatter (application-like)
    console_formatter = ApplicationFormatter()

    file_handler.setFormatter(file_formatter)
    console_handler.setFormatter(console_formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

# --- TTL CACHE MANAGEMENT ---
class TTLCache:
    """Time-To-Live cache with automatic cleanup of expired entries."""

    def __init__(self, max_size: int = None, ttl_seconds: int = None):
        self.cache = OrderedDict()
        self.timestamps = {}
        self.max_size = max_size or config.MAX_CACHE_SIZE
        self.ttl = ttl_seconds or config.CACHE_TTL_SECONDS
        self.lock = threading.Lock()

    def cleanup_expired(self) -> int:
        """Remove expired entries and return count of removed items."""
        with self.lock:
            current_time = time.time()
            expired_keys = [
                key for key, timestamp in self.timestamps.items()
                if current_time - timestamp > self.ttl
            ]

            for key in expired_keys:
                self.cache.pop(key, None)
                self.timestamps.pop(key, None)

            return len(expired_keys)

    def put(self, key: str, value: Any) -> None:
        """Add or update an item in the cache."""
        with self.lock:
            # Clean up expired items first (internal cleanup without lock)
            current_time = time.time()
            expired_keys = [
                k for k, timestamp in self.timestamps.items()
                if current_time - timestamp > self.ttl
            ]

            for k in expired_keys:
                self.cache.pop(k, None)
                self.timestamps.pop(k, None)

            # Remove oldest items if at capacity
            while len(self.cache) >= self.max_size:
                oldest_key = next(iter(self.cache))
                self.cache.pop(oldest_key)
                self.timestamps.pop(oldest_key)

            self.cache[key] = value
            self.timestamps[key] = time.time()

    def get(self, key: str) -> Optional[Any]:
        """Get an item from the cache if it exists and hasn't expired."""
        with self.lock:
            if key not in self.cache:
                return None

            # Check if expired
            if time.time() - self.timestamps[key] > self.ttl:
                self.cache.pop(key, None)
                self.timestamps.pop(key, None)
                return None

            return self.cache[key]

    def __contains__(self, key: str) -> bool:
        """Check if key exists and hasn't expired."""
        return self.get(key) is not None

    def __len__(self) -> int:
        """Return current cache size after cleanup."""
        with self.lock:
            # Inline cleanup to avoid nested lock acquisition
            current_time = time.time()
            expired_keys = [
                key for key, timestamp in self.timestamps.items()
                if current_time - timestamp > self.ttl
            ]
            for key in expired_keys:
                self.cache.pop(key, None)
                self.timestamps.pop(key, None)
            return len(self.cache)

    def items(self):
        """Return cache items after cleanup."""
        with self.lock:
            # Inline cleanup to avoid nested lock acquisition
            current_time = time.time()
            expired_keys = [
                key for key, timestamp in self.timestamps.items()
                if current_time - timestamp > self.ttl
            ]
            for key in expired_keys:
                self.cache.pop(key, None)
                self.timestamps.pop(key, None)
            return dict(self.cache).items()

    def clear(self) -> None:
        """Clear all cache entries."""
        with self.lock:
            self.cache.clear()
            self.timestamps.clear()

# --- AIRCRAFT REGISTRY CACHE ---
class AircraftRegistryCache:
    """Aircraft registry cache that loads aircraft data from CSV."""

    def __init__(self):
        self.aircraft_data = {}
        self.last_update_time = 0
        self.lock = threading.Lock()
        self.logger = logging.getLogger('adsb_logger.registry')
        self._loading = False  # Flag to prevent concurrent loading
        self.csv_circuit_breaker = CircuitBreaker(failure_threshold=3, timeout=300)  # 5-minute timeout

    def _load_aircraft_csv(self, csv_url: str) -> Dict[str, Dict[str, str]]:
        """Load aircraft data from CSV URL with circuit breaker protection."""
        def fetch_csv():
            response = requests.get(csv_url, timeout=30)
            response.raise_for_status()
            return response.text.strip()

        try:
            csv_content = self.csv_circuit_breaker.call(fetch_csv)
            aircraft_dict = {}

            if not csv_content:
                self.logger.warning(f"Empty CSV response from {csv_url}")
                return aircraft_dict

            # Parse CSV content
            csv_reader = csv.reader(csv_content.split('\n'), delimiter=';')
            processed_rows = 0
            for row_num, row in enumerate(csv_reader, 1):
                try:
                    if len(row) >= 5 and row[0].strip():  # Ensure we have at least 5 columns and hex is not empty
                        hex_code = row[0].strip().upper()  # Normalize hex to uppercase

                        # Validate hex code format (should be 6 hex characters)
                        if len(hex_code) == 6 and all(c in '0123456789ABCDEF' for c in hex_code):
                            aircraft_dict[hex_code] = {
                                'registration': row[1].strip() if len(row) > 1 and row[1].strip() else None,
                                'type_code': row[2].strip() if len(row) > 2 and row[2].strip() else None,
                                'long_type_name': row[4].strip() if len(row) > 4 and row[4].strip() else None
                            }
                            processed_rows += 1
                        else:
                            self.logger.debug(f"Invalid hex code format on row {row_num}: {hex_code}")
                except (IndexError, AttributeError) as e:
                    self.logger.debug(f"Skipping malformed row {row_num}: {e}")
                    continue

            self.logger.info(f"Successfully loaded {processed_rows} aircraft records from {row_num} CSV rows")
            return aircraft_dict

        except Exception as e:
            self.logger.error(f"Failed to load aircraft CSV from {csv_url}: {e}")
            return {}

    def get_aircraft_info(self, hex_code: str) -> Dict[str, Optional[str]]:
        """Get aircraft information by hex code, refreshing cache if needed."""
        if not hex_code:
            return {'registration': None, 'type_code': None, 'long_type_name': None}

        with self.lock:
            current_time = time.time()

            # Check if we need to refresh the cache
            if (current_time - self.last_update_time > config.AIRCRAFT_CSV_TTL_SECONDS or
                not self.aircraft_data):

                # Prevent concurrent loading attempts
                if self._loading:
                    self.logger.debug("Cache refresh already in progress, using existing data")
                else:
                    self._loading = True
                    try:
                        self.logger.debug("Refreshing aircraft registry cache...")
                        new_data = self._load_aircraft_csv(config.AIRCRAFT_CSV_PATH)
                        if new_data:  # Only update if we got data
                            self.aircraft_data = new_data
                            self.last_update_time = current_time
                            self.logger.debug("Aircraft registry cache refreshed successfully")
                        else:
                            self.logger.warning("Failed to refresh aircraft registry cache, using existing data")
                    finally:
                        self._loading = False

            # Look up aircraft information (must stay within lock)
            hex_upper = hex_code.upper()
            aircraft_info = self.aircraft_data.get(hex_upper, {})

            # Return a copy to avoid external mutation
            return {
                'registration': aircraft_info.get('registration'),
                'type_code': aircraft_info.get('type_code'),
                'long_type_name': aircraft_info.get('long_type_name')
            }

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self.lock:
            return {
                'total_aircraft': len(self.aircraft_data),
                'last_update': self.last_update_time,
                'cache_age_seconds': time.time() - self.last_update_time
            }

# --- CIRCUIT BREAKER PATTERN ---
class CircuitBreaker:
    """Circuit breaker pattern for external service calls."""

    def __init__(self, failure_threshold: int = None, timeout: int = None):
        self.failure_threshold = failure_threshold or config.CIRCUIT_BREAKER_THRESHOLD
        self.timeout = timeout or config.CIRCUIT_BREAKER_TIMEOUT
        self.failure_count = 0
        self.last_failure_time = None
        self.state = 'CLOSED'  # CLOSED, OPEN, HALF_OPEN
        self.lock = threading.Lock()

    def call(self, func, *args, **kwargs):
        """Execute function with circuit breaker protection."""
        with self.lock:
            if self.state == 'OPEN':
                if self.last_failure_time and time.time() - self.last_failure_time > self.timeout:
                    self.state = 'HALF_OPEN'
                else:
                    raise Exception(f"Circuit breaker is OPEN. Service unavailable for {self.timeout}s after {self.failure_count} failures.")

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise

    def _on_success(self):
        """Handle successful call."""
        with self.lock:
            self.failure_count = 0
            self.state = 'CLOSED'

    def _on_failure(self):
        """Handle failed call."""
        with self.lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.failure_count >= self.failure_threshold:
                self.state = 'OPEN'

    def get_state(self) -> Dict[str, Any]:
        """Get current circuit breaker state."""
        with self.lock:
            return {
                'state': self.state,
                'failure_count': self.failure_count,
                'failure_threshold': self.failure_threshold,
                'last_failure_time': self.last_failure_time
            }

# --- DATABASE CONNECTION MANAGEMENT ---
class DatabaseManager:
    """Database connection pool manager with automatic cleanup."""

    def __init__(self, config_dict: Dict[str, Any], pool_size: int = None):
        self.pool_config = config_dict.copy()
        self.pool_size = pool_size or config.DB_POOL_SIZE
        self.pool = None
        self.logger = logging.getLogger('adsb_logger.db')
        self._initialize_pool()

    def _initialize_pool(self):
        """Initialize the connection pool."""
        try:
            self.pool = pooling.MySQLConnectionPool(
                pool_name="adsb_pool",
                pool_size=self.pool_size,
                pool_reset_session=True,
                **self.pool_config
            )
            self.logger.info(f"Database connection pool initialized with {self.pool_size} connections")
        except mysql.connector.Error as e:
            self.logger.error(f"Failed to initialize database pool: {e}")
            raise

    @contextlib.contextmanager
    def get_connection(self):
        """Get a database connection from the pool with automatic cleanup."""
        conn = None
        try:
            conn = self.pool.get_connection()
            self.logger.debug("Database connection acquired from pool")
            yield conn
        except Exception as e:
            if conn:
                try:
                    conn.rollback()
                    self.logger.debug("Database transaction rolled back due to error")
                except:
                    pass
            self.logger.error(f"Database operation failed: {e}")
            raise
        finally:
            if conn:
                try:
                    conn.close()
                    self.logger.debug("Database connection returned to pool")
                except:
                    pass

# --- ADSB LOGGER CLASS ---
class ADSBLogger:
    """Main ADSB Logger class with graceful shutdown and resource management."""

    def __init__(self):
        # Clear screen and show startup
        self._clear_screen()

        self.logger = setup_logging()
        self.shutdown_event = threading.Event()
        self.local_aircraft_cache = TTLCache()
        self.aircraft_registry = AircraftRegistryCache()
        self.previously_seen_hexes = set()
        self.last_summary_upload_time = time.time()
        self.first_scan_complete = False
        self.db_manager = DatabaseManager(DB_CONFIG)
        self.api_circuit_breaker = CircuitBreaker()
        self.db_circuit_breaker = CircuitBreaker()
        self.log_manager = LogFileManager(config.LOG_FILE, config.LOG_CLEANUP_INTERVAL_HOURS)
        self.setup_signal_handlers()

        self.logger.info("ADSB Logger initialized with production features")
        self.logger.info(f"Configuration: Upload interval={config.SUMMARY_UPLOAD_INTERVAL}s, Cache size={config.MAX_CACHE_SIZE}, TTL={config.CACHE_TTL_SECONDS}s")
        self.logger.info(f"Aircraft registry CSV TTL: {config.AIRCRAFT_CSV_TTL_SECONDS}s")
        self.logger.info(f"Log cleanup interval: {config.LOG_CLEANUP_INTERVAL_HOURS} hours")

    def setup_signal_handlers(self):
        """Set up graceful shutdown signal handlers."""
        def signal_handler(signum, frame):
            signal_name = signal.Signals(signum).name
            self.logger.info(f"Received signal {signal_name} ({signum}), initiating graceful shutdown...")
            self.shutdown_event.set()

        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

        # On Windows, also handle CTRL+C
        if hasattr(signal, 'SIGBREAK'):
            signal.signal(signal.SIGBREAK, signal_handler)

    def is_shutdown_requested(self) -> bool:
        """Check if shutdown has been requested."""
        return self.shutdown_event.is_set()

    def _clear_screen(self):
        """Clear the terminal screen for a clean startup."""
        import os
        # Clear screen command for Windows and Unix-like systems
        os.system('cls' if os.name == 'nt' else 'clear')


    def send_failure_email(self, error_message: str):
        """Sends an email notification when a database error occurs."""
        if not SENDER_EMAIL or "your_email" in SENDER_EMAIL:
            self.logger.warning("Email not configured. Skipping notification.")
            return

        message = MIMEMultipart("alternative")
        message["Subject"] = "ADSB Logger: Database Operation FAILED"
        message["From"] = SENDER_EMAIL
        message["To"] = RECEIVER_EMAIL
        text = f"The ADSB logger script failed a database operation.\n\nError details:\n{error_message}"
        html = f"""
        <html><body>
            <h2>ADSB Logger: Database Operation Failure</h2>
            <p>The script encountered an error. Please check the server and script logs.</p>
            <h3>Error Details:</h3>
            <pre style="background-color:#f0f0f0; border:1px solid #ddd; padding:10px; border-radius:5px;"><code>{error_message}</code></pre>
        </body></html>
        """
        part1 = MIMEText(text, "plain")
        part2 = MIMEText(html, "html")
        message.attach(part1)
        message.attach(part2)

        context = ssl.create_default_context()
        try:
            with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT, context=context) as server:
                server.login(SENDER_EMAIL, SENDER_PASSWORD)
                server.sendmail(SENDER_EMAIL, RECEIVER_EMAIL, message.as_string())
                self.logger.info("Successfully sent failure notification email.")
        except Exception as e:
            self.logger.error(f"Failed to send email notification: {e}")

    def validate_aircraft_data(self, aircraft: Dict[str, Any]) -> bool:
        """Validates aircraft data before processing."""
        if not isinstance(aircraft, dict):
            return False
        hex_code = aircraft.get('hex')
        if not hex_code or not isinstance(hex_code, str):
            return False

        # Validate numeric fields
        numeric_fields = ['alt_baro', 'gs', 'track', 'baro_rate', 'messages', 'seen', 'lat', 'lon']
        for field in numeric_fields:
            if field in aircraft and aircraft[field] is not None and not isinstance(aircraft[field], (int, float)):
                self.logger.debug(f"Invalid numeric field {field} in aircraft {hex_code}: {aircraft[field]}")
                return False
        return True

    def read_aircraft_json(self, path: str) -> list:
        """Reads and parses the aircraft.json file or URL with validation."""
        try:
            def fetch_data():
                if path.startswith('http://') or path.startswith('https://'):
                    response = requests.get(path, timeout=10)
                    response.raise_for_status()
                    return response.json()
                else:
                    with open(path, 'r') as f:
                        return json.load(f)

            # Use circuit breaker for API calls
            data = self.api_circuit_breaker.call(fetch_data)
            aircraft_list = data.get('aircraft', [])
            valid_aircraft = [ac for ac in aircraft_list if self.validate_aircraft_data(ac)]

            self.logger.debug(f"Read {len(aircraft_list)} aircraft, {len(valid_aircraft)} valid")
            return valid_aircraft

        except Exception as e:
            self.logger.error(f"Error reading/parsing aircraft data from {path}: {e}")
            # Also log at INFO level for visibility
            self.logger.info(f"Failed to fetch aircraft data - API might be unavailable: {e}")
            return []

    def upload_summary_to_database(self, aircraft_data: Dict[str, Any], retry_count: int = 0) -> bool:
        """Connects to the database and uploads the cached aircraft summary data."""
        if not aircraft_data:
            self.logger.info("No aircraft summary data to upload.")
            return True

        self.logger.info(f"Attempting to upload summary for {len(aircraft_data)} unique aircraft...")
        self.logger.info("Building records for database upload...")
        records = []

        for hex_code, data in aircraft_data.items():
            first_seen_ts = data.get('first_seen', time.time())
            records.append({
                'hex': hex_code, 'flight': data.get('flight'), 'alt_baro': data.get('alt_baro'),
                'gs': data.get('gs'), 'track': data.get('track'),
                'baro_rate': data.get('baro_rate'), 'squawk': data.get('squawk'), 'category': data.get('category'),
                'messages': data.get('messages'), 'seen': data.get('seen'),
                'lat': data.get('lat'), 'lon': data.get('lon'),
                'registration': data.get('registration'), 'type_code': data.get('type_code'), 'long_type_name': data.get('long_type_name'),
                'first_seen': datetime.fromtimestamp(first_seen_ts).strftime('%Y-%m-%d %H:%M:%S'),
                'seen_count_increment': 1 if data.get('is_new_sighting', False) else 0
            })

        def database_operation():
            self.logger.info("Starting database operation...")
            with self.db_manager.get_connection() as conn:
                self.logger.info("Database connection acquired successfully")
                cursor = conn.cursor()

                # Set query timeout to prevent hanging
                self.logger.info("Setting database session timeouts...")
                cursor.execute("SET SESSION wait_timeout = 30")
                cursor.execute("SET SESSION interactive_timeout = 30")
                self.logger.info("Database session timeouts configured")

                # Process new sightings and existing aircraft separately
                new_sighting_query = """
                INSERT INTO tracked_aircraft (
                    hex, flight, alt_baro, gs, track, baro_rate, squawk,
                    category, messages, seen, lat, lon, registration, type_code, long_type_name,
                    seen_count, first_seen, last_seen
                ) VALUES (
                    %(hex)s, %(flight)s, %(alt_baro)s, %(gs)s, %(track)s, %(baro_rate)s, %(squawk)s,
                    %(category)s, %(messages)s, %(seen)s, %(lat)s, %(lon)s, %(registration)s, %(type_code)s, %(long_type_name)s,
                    1, %(first_seen)s, NOW()
                ) ON DUPLICATE KEY UPDATE
                    flight=VALUES(flight), alt_baro=VALUES(alt_baro), gs=VALUES(gs),
                    track=VALUES(track), baro_rate=VALUES(baro_rate), squawk=VALUES(squawk), category=VALUES(category),
                    messages=VALUES(messages), seen=VALUES(seen),
                    lat=VALUES(lat), lon=VALUES(lon),
                    registration=VALUES(registration), type_code=VALUES(type_code), long_type_name=VALUES(long_type_name),
                    seen_count = seen_count + 1,
                    last_seen=NOW()
                """

                existing_query = """
                INSERT INTO tracked_aircraft (
                    hex, flight, alt_baro, gs, track, baro_rate, squawk,
                    category, messages, seen, lat, lon, registration, type_code, long_type_name,
                    seen_count, first_seen, last_seen
                ) VALUES (
                    %(hex)s, %(flight)s, %(alt_baro)s, %(gs)s, %(track)s, %(baro_rate)s, %(squawk)s,
                    %(category)s, %(messages)s, %(seen)s, %(lat)s, %(lon)s, %(registration)s, %(type_code)s, %(long_type_name)s,
                    1, %(first_seen)s, NOW()
                ) ON DUPLICATE KEY UPDATE
                    flight=VALUES(flight), alt_baro=VALUES(alt_baro), gs=VALUES(gs),
                    track=VALUES(track), baro_rate=VALUES(baro_rate), squawk=VALUES(squawk), category=VALUES(category),
                    messages=VALUES(messages), seen=VALUES(seen),
                    lat=VALUES(lat), lon=VALUES(lon),
                    registration=VALUES(registration), type_code=VALUES(type_code), long_type_name=VALUES(long_type_name),
                    last_seen=NOW()
                """

                # Separate records into new sightings and existing aircraft
                self.logger.info("Separating records into new sightings and existing aircraft...")
                new_sightings = []
                existing_aircraft = []

                for record in records:
                    clean_record = {k: v for k, v in record.items() if k != 'seen_count_increment'}
                    if record['seen_count_increment'] == 1:
                        new_sightings.append(clean_record)
                    else:
                        existing_aircraft.append(clean_record)

                self.logger.info(f"Prepared {len(new_sightings)} new sightings and {len(existing_aircraft)} existing aircraft updates")
                total_rows = 0

                # Process in batches for better performance with large datasets
                batch_size = 100  # Process in smaller batches

                # Execute new sightings (increment seen_count)
                if new_sightings:
                    self.logger.info(f"Executing {len(new_sightings)} new sighting queries in batches of {batch_size}...")
                    for i in range(0, len(new_sightings), batch_size):
                        batch = new_sightings[i:i + batch_size]
                        cursor.executemany(new_sighting_query, batch)
                        total_rows += cursor.rowcount
                    self.logger.info(f"Processed {len(new_sightings)} new sightings (increment seen_count)")

                # Execute existing aircraft updates (no increment)
                if existing_aircraft:
                    self.logger.info(f"Executing {len(existing_aircraft)} existing aircraft queries in batches of {batch_size}...")
                    for i in range(0, len(existing_aircraft), batch_size):
                        batch = existing_aircraft[i:i + batch_size]
                        cursor.executemany(existing_query, batch)
                        total_rows += cursor.rowcount
                    self.logger.info(f"Updated {len(existing_aircraft)} existing aircraft (no increment)")

                self.logger.info("Committing transaction...")
                conn.commit()
                self.logger.info(f"SUCCESS: Committed {total_rows} summary changes to the database.")
                return True

        try:
            return database_operation()
        except Exception as e:
            self.logger.error(f"Database operation failed: {e}")
            if retry_count < config.MAX_RETRY_ATTEMPTS:
                self.logger.info(f"Retrying summary upload in {config.RETRY_DELAY}s... ({retry_count + 1}/{config.MAX_RETRY_ATTEMPTS})")
                time.sleep(config.RETRY_DELAY)
                return self.upload_summary_to_database(aircraft_data, retry_count + 1)
            else:
                self.logger.error(f"Failed summary upload after {config.MAX_RETRY_ATTEMPTS} attempts.")
                self.send_failure_email(f"Failed to upload summary: {e}")
                return False

    def run(self):
        """Main execution loop with graceful shutdown support."""
        self.logger.info(f"Starting ADSB Logger - Summary upload interval: {config.SUMMARY_UPLOAD_INTERVAL/60:.1f} minutes")

        startup_stats = {
            'cache_size': len(self.local_aircraft_cache),
            'api_circuit_state': self.api_circuit_breaker.get_state(),
            'db_circuit_state': self.db_circuit_breaker.get_state()
        }
        self.logger.info(f"Startup stats: {startup_stats}")

        while not self.is_shutdown_requested():
            try:
                current_time = time.time()

                # Read all aircraft data with circuit breaker protection
                all_aircraft = self.read_aircraft_json(config.AIRCRAFT_JSON_PATH)

                # Log data collection activity at INFO level
                self.logger.info(f"Data collection: Read {len(all_aircraft)} aircraft from source")

                # Update aircraft data for display
                ApplicationFormatter.update_aircraft_data(all_aircraft)

                # Update the summary cache
                current_hexes = {ac['hex'] for ac in all_aircraft if 'hex' in ac}
                self.logger.info(f"Processing {len(current_hexes)} aircraft with valid hex codes")

                if all_aircraft:
                    now = time.time()
                    for ac in all_aircraft:
                        hex_code = ac.get('hex')
                        if not hex_code:
                            continue

                        # Enrich aircraft data with registry information
                        registry_info = self.aircraft_registry.get_aircraft_info(hex_code)
                        ac.update(registry_info)  # Add registration, type_code, long_type_name

                        # Validate and sanitize data lengths for database constraints
                        if ac.get('registration') and len(ac['registration']) > 15:
                            ac['registration'] = ac['registration'][:15]
                        if ac.get('type_code') and len(ac['type_code']) > 10:
                            ac['type_code'] = ac['type_code'][:10]
                        if ac.get('long_type_name') and len(ac['long_type_name']) > 100:
                            ac['long_type_name'] = ac['long_type_name'][:100]

                        # Check if aircraft exists in cache
                        cached_aircraft = self.local_aircraft_cache.get(hex_code)
                        if cached_aircraft:
                            # Update existing entry but preserve first_seen
                            first_seen = cached_aircraft.get('first_seen')
                            ac['first_seen'] = first_seen
                        else:
                            # Add new entry
                            ac['first_seen'] = now

                        # Mark if it's a new sighting in this cycle for seen_count increment
                        ac['is_new_sighting'] = hex_code not in self.previously_seen_hexes

                        # Update cache
                        self.local_aircraft_cache.put(hex_code, ac)

                    self.logger.info(f"Finished processing aircraft data, updating cache")

                self.previously_seen_hexes = current_hexes

                # Log cache statistics
                expired_count = self.local_aircraft_cache.cleanup_expired()
                cache_size = len(self.local_aircraft_cache)

                if expired_count > 0:
                    self.logger.info(f"Cleaned up {expired_count} expired cache entries")

                # Always show cache status at INFO level for monitoring
                self.logger.info(f"Cache status: {cache_size} total aircraft cached, {len(current_hexes)} currently active")

                # Check for log file cleanup (every cycle, but only acts if needed)
                if self.log_manager.should_cleanup():
                    self.logger.debug("Log file cleanup is due - performing 48-hour log file maintenance...")
                    cleanup_success = self.log_manager.cleanup_log_file()
                    if cleanup_success:
                        self.logger.debug("Log file cleanup completed successfully")
                    else:
                        self.logger.debug("Log file cleanup failed - will retry in next cycle")

                # Check if it's time to upload the summary
                should_upload = (current_time - self.last_summary_upload_time >= config.SUMMARY_UPLOAD_INTERVAL or
                                cache_size >= config.MAX_CACHE_SIZE or
                                (not self.first_scan_complete and cache_size > 0))

                if should_upload and cache_size > 0:
                    # Determine reason for upload
                    if not self.first_scan_complete:
                        self.logger.info("Initiating initial data upload after first scan...")
                    elif cache_size >= config.MAX_CACHE_SIZE:
                        self.logger.warning(f"Cache size limit reached ({cache_size}), forcing upload.")
                    else:
                        self.logger.info("Initiating scheduled summary data upload...")

                    # Convert cache to dictionary for upload
                    try:
                        upload_data = dict(self.local_aircraft_cache.items())
                        self.logger.debug(f"Successfully converted cache to dict with {len(upload_data)} items")
                    except Exception as cache_error:
                        self.logger.error(f"Cache conversion failed: {cache_error}")
                        continue  # Skip upload if cache conversion fails and continue with next cycle

                    try:
                        upload_result = self.upload_summary_to_database(upload_data)
                        if upload_result:
                            self.last_summary_upload_time = current_time
                            # Mark first scan as complete after successful initial upload
                            if not self.first_scan_complete:
                                self.first_scan_complete = True
                                self.logger.info("Initial upload complete. Switching to timer/cache-based upload cycle.")
                            # Clear the 'is_new_sighting' flag for all cached items after upload
                            for hex_code, aircraft_data in self.local_aircraft_cache.items():
                                aircraft_data['is_new_sighting'] = False
                                self.local_aircraft_cache.put(hex_code, aircraft_data)
                            self.logger.info("Summary upload complete.")
                        else:
                            self.logger.warning("Summary upload failed, keeping cache for next attempt.")
                    except Exception as upload_error:
                        self.logger.error(f"Upload error: {upload_error}", exc_info=True)

                # Log system health periodically
                if int(current_time) % 300 == 0:  # Every 5 minutes
                    registry_stats = self.aircraft_registry.get_cache_stats()
                    log_cleanup_stats = self.log_manager.get_cleanup_status()
                    health_stats = {
                        'cache_size': cache_size,
                        'api_circuit': self.api_circuit_breaker.get_state(),
                        'db_circuit': self.db_circuit_breaker.get_state(),
                        'registry_aircraft_count': registry_stats['total_aircraft'],
                        'registry_cache_age_hours': round(registry_stats['cache_age_seconds'] / 3600, 1),
                        'uptime_minutes': int((current_time - self.last_summary_upload_time) / 60),
                        'log_cleanup_hours_until': round(log_cleanup_stats['hours_until_cleanup'], 1)
                    }
                    self.logger.info(f"Health check: {health_stats}")

                if not self.is_shutdown_requested():
                    self.logger.info("Sleeping for 60 seconds until next data collection cycle...")
                    # Sleep in small intervals to allow for responsive shutdown
                    for i in range(60):
                        if self.is_shutdown_requested():
                            break
                        time.sleep(1)

            except KeyboardInterrupt:
                self.logger.info("Keyboard interrupt received, initiating shutdown...")
                self.shutdown_event.set()
            except Exception as e:
                self.logger.error(f"An unexpected error occurred in the main loop: {e}", exc_info=True)
                if not self.is_shutdown_requested():
                    time.sleep(60)

        # Graceful shutdown - attempt final upload
        self.logger.info("Shutdown signal received. Attempting final summary upload...")
        final_data = dict(self.local_aircraft_cache.items())
        if final_data:
            try:
                self.upload_summary_to_database(final_data)
                self.logger.info(f"Final upload completed for {len(final_data)} aircraft.")
            except Exception as e:
                self.logger.error(f"Final upload failed: {e}")

        # Final shutdown message with visual formatting
        print(f"\n{Colors.BOLD}{Colors.CYAN}{'='*80}{Colors.RESET}")
        print(f"{Colors.BOLD}{Colors.CYAN}║{Colors.RESET}  {Colors.BOLD}🛩️  ADSB Aircraft Tracking System - Shutdown Complete{' '*24}{Colors.CYAN}║{Colors.RESET}")
        print(f"{Colors.BOLD}{Colors.CYAN}║{' '*78}║{Colors.RESET}")
        print(f"{Colors.BOLD}{Colors.CYAN}║{Colors.RESET}  {Colors.GREEN}✓ Final upload completed{Colors.RESET}{' '*46}{Colors.CYAN}║{Colors.RESET}")
        print(f"{Colors.BOLD}{Colors.CYAN}║{Colors.RESET}  {Colors.GREEN}✓ Database connections closed{Colors.RESET}{' '*41}{Colors.CYAN}║{Colors.RESET}")
        print(f"{Colors.BOLD}{Colors.CYAN}║{Colors.RESET}  {Colors.GREEN}✓ System shutdown gracefully{Colors.RESET}{' '*40}{Colors.CYAN}║{Colors.RESET}")
        print(f"{Colors.BOLD}{Colors.CYAN}║{' '*78}║{Colors.RESET}")
        print(f"{Colors.BOLD}{Colors.CYAN}{'='*80}{Colors.RESET}")
        print(f"{Colors.DIM}Thank you for using ADSB Aircraft Tracking System{Colors.RESET}\n")

        self.logger.info("ADSB Logger shutdown complete.")
        logging.shutdown()

# Legacy function kept for backward compatibility
def send_failure_email(error_message):
    """Legacy function - use ADSBLogger.send_failure_email instead."""
    logger = logging.getLogger('adsb_logger')
    logger.warning("Using legacy send_failure_email function - consider updating to use ADSBLogger class")

    if not SENDER_EMAIL or "your_email" in SENDER_EMAIL:
        logger.warning("Email not configured. Skipping notification.")
        return

    message = MIMEMultipart("alternative")
    message["Subject"] = "ADSB Logger: Database Operation FAILED"
    message["From"] = SENDER_EMAIL
    message["To"] = RECEIVER_EMAIL
    text = f"The ADSB logger script failed a database operation.\n\nError details:\n{error_message}"
    html = f"""
    <html><body>
        <h2>ADSB Logger: Database Operation Failure</h2>
        <p>The script encountered an error. Please check the server and script logs.</p>
        <h3>Error Details:</h3>
        <pre style="background-color:#f0f0f0; border:1px solid #ddd; padding:10px; border-radius:5px;"><code>{error_message}</code></pre>
    </body></html>
    """
    part1 = MIMEText(text, "plain")
    part2 = MIMEText(html, "html")
    message.attach(part1)
    message.attach(part2)

    context = ssl.create_default_context()
    try:
        with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT, context=context) as server:
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.sendmail(SENDER_EMAIL, RECEIVER_EMAIL, message.as_string())
            logger.info("Successfully sent failure notification email.")
    except Exception as e:
        logger.error(f"Failed to send email notification: {e}")

# Legacy function kept for backward compatibility
def validate_aircraft_data(aircraft):
    """Legacy function - use ADSBLogger.validate_aircraft_data instead."""
    if not isinstance(aircraft, dict): return False
    hex_code = aircraft.get('hex')
    if not hex_code or not isinstance(hex_code, str): return False
    # Validate numeric fields
    numeric_fields = ['alt_baro', 'gs', 'track', 'baro_rate', 'messages', 'seen', 'lat', 'lon']
    for field in numeric_fields:
        if field in aircraft and aircraft[field] is not None and not isinstance(aircraft[field], (int, float)):
            return False
    return True

# Legacy function kept for backward compatibility
def read_aircraft_json(path):
    """Legacy function - use ADSBLogger.read_aircraft_json instead."""
    try:
        if path.startswith('http://') or path.startswith('https://'):
            response = requests.get(path, timeout=10)
            response.raise_for_status()
            data = response.json()
        else:
            with open(path, 'r') as f: data = json.load(f)
        aircraft_list = data.get('aircraft', [])
        valid_aircraft = [ac for ac in aircraft_list if validate_aircraft_data(ac)]
        return valid_aircraft
    except Exception as e:
        logger = logging.getLogger('adsb_logger')
        logger.error(f"Error reading/parsing aircraft data from {path}: {e}")
        return []


# Legacy function kept for backward compatibility
def upload_summary_to_database(aircraft_data, retry_count=0):
    """Legacy function - use ADSBLogger.upload_summary_to_database instead."""
    logger = logging.getLogger('adsb_logger')
    logger.warning("Using legacy upload_summary_to_database function - consider updating to use ADSBLogger class")

    if not aircraft_data:
        logger.info("No aircraft summary data to upload.")
        return True

    logger.info(f"Attempting to upload summary for {len(aircraft_data)} unique aircraft...")
    cnx = None
    records = []
    for hex_code, data in aircraft_data.items():
        first_seen_ts = data.get('first_seen', time.time())
        records.append({
            'hex': hex_code, 'flight': data.get('flight'), 'alt_baro': data.get('alt_baro'),
            'gs': data.get('gs'), 'track': data.get('track'),
            'baro_rate': data.get('baro_rate'), 'squawk': data.get('squawk'), 'category': data.get('category'),
            'messages': data.get('messages'), 'seen': data.get('seen'),
            'lat': data.get('lat'), 'lon': data.get('lon'),
            'registration': data.get('registration'), 'type_code': data.get('type_code'), 'long_type_name': data.get('long_type_name'),
            'first_seen': datetime.fromtimestamp(first_seen_ts).strftime('%Y-%m-%d %H:%M:%S'),
            'seen_count_increment': 1 if data.get('is_new_sighting', False) else 0
        })

    try:
        cnx = mysql.connector.connect(**DB_CONFIG)
        cursor = cnx.cursor()
        # Process new sightings and existing aircraft separately
        new_sighting_query = """
        INSERT INTO tracked_aircraft (
            hex, flight, alt_baro, gs, track, baro_rate, squawk,
            category, messages, seen, lat, lon, registration, type_code, long_type_name,
            seen_count, first_seen, last_seen
        ) VALUES (
            %(hex)s, %(flight)s, %(alt_baro)s, %(gs)s, %(track)s, %(baro_rate)s, %(squawk)s,
            %(category)s, %(messages)s, %(seen)s, %(lat)s, %(lon)s, %(registration)s, %(type_code)s, %(long_type_name)s,
            1, %(first_seen)s, NOW()
        ) ON DUPLICATE KEY UPDATE
            flight=VALUES(flight), alt_baro=VALUES(alt_baro), gs=VALUES(gs),
            track=VALUES(track), baro_rate=VALUES(baro_rate), squawk=VALUES(squawk), category=VALUES(category),
            messages=VALUES(messages), seen=VALUES(seen),
            lat=VALUES(lat), lon=VALUES(lon),
            registration=VALUES(registration), type_code=VALUES(type_code), long_type_name=VALUES(long_type_name),
            seen_count = seen_count + 1,
            last_seen=NOW()
        """

        existing_query = """
        INSERT INTO tracked_aircraft (
            hex, flight, alt_baro, gs, track, baro_rate, squawk,
            category, messages, seen, lat, lon, registration, type_code, long_type_name,
            seen_count, first_seen, last_seen
        ) VALUES (
            %(hex)s, %(flight)s, %(alt_baro)s, %(gs)s, %(track)s, %(baro_rate)s, %(squawk)s,
            %(category)s, %(messages)s, %(seen)s, %(lat)s, %(lon)s, %(registration)s, %(type_code)s, %(long_type_name)s,
            1, %(first_seen)s, NOW()
        ) ON DUPLICATE KEY UPDATE
            flight=VALUES(flight), alt_baro=VALUES(alt_baro), gs=VALUES(gs),
            track=VALUES(track), baro_rate=VALUES(baro_rate), squawk=VALUES(squawk), category=VALUES(category),
            messages=VALUES(messages), seen=VALUES(seen),
            lat=VALUES(lat), lon=VALUES(lon),
            registration=VALUES(registration), type_code=VALUES(type_code), long_type_name=VALUES(long_type_name),
            last_seen=NOW()
        """

        # Separate records into new sightings and existing aircraft
        new_sightings = []
        existing_aircraft = []

        for record in records:
            if record['seen_count_increment'] == 1:
                # Remove the seen_count_increment field for the query
                clean_record = {k: v for k, v in record.items() if k != 'seen_count_increment'}
                new_sightings.append(clean_record)
            else:
                # Remove the seen_count_increment field for the query
                clean_record = {k: v for k, v in record.items() if k != 'seen_count_increment'}
                existing_aircraft.append(clean_record)

        total_rows = 0

        # Execute new sightings (increment seen_count)
        if new_sightings:
            cursor.executemany(new_sighting_query, new_sightings)
            total_rows += cursor.rowcount
            logger.info(f"Processed {len(new_sightings)} new sightings (increment seen_count)")

        # Execute existing aircraft updates (no increment)
        if existing_aircraft:
            cursor.executemany(existing_query, existing_aircraft)
            total_rows += cursor.rowcount
            logger.info(f"Updated {len(existing_aircraft)} existing aircraft (no increment)")

        cnx.commit()
        logger.info(f"SUCCESS: Committed {total_rows} summary changes to the database.")
        return True
    except mysql.connector.Error as err:
        logger.error(f"DATABASE ERROR (summary): {err}")
        if retry_count < config.MAX_RETRY_ATTEMPTS:
            logger.info(f"Retrying summary upload in {config.RETRY_DELAY}s... ({retry_count + 1}/{config.MAX_RETRY_ATTEMPTS})")
            time.sleep(config.RETRY_DELAY)
            return upload_summary_to_database(aircraft_data, retry_count + 1)
        else:
            logger.error(f"Failed summary upload after {config.MAX_RETRY_ATTEMPTS} attempts.")
            send_failure_email(f"Failed to upload summary: {err}")
            return False
    finally:
        if cnx and cnx.is_connected(): cnx.close()


def main():
    """Legacy main function - use ADSBLogger().run() instead."""
    logger = logging.getLogger('adsb_logger')
    logger.warning("Using legacy main function - consider updating to use ADSBLogger().run()")

    previously_seen_hexes = set()
    local_aircraft_cache = {}
    last_summary_upload_time = time.time()

    logger.info(f"Starting ADSB Logger - Summary upload interval: {config.SUMMARY_UPLOAD_INTERVAL/60:.1f} minutes")

    while True:
        try:
            current_time = time.time()

            # Read all aircraft data
            all_aircraft = read_aircraft_json(config.AIRCRAFT_JSON_PATH)

            # Update the summary cache
            current_hexes = {ac['hex'] for ac in all_aircraft if 'hex' in ac}
            if all_aircraft:
                now = time.time()
                for ac in all_aircraft:
                    hex_code = ac.get('hex')
                    if not hex_code: continue

                    if hex_code in local_aircraft_cache:
                        # Update existing entry but preserve first_seen
                        first_seen = local_aircraft_cache[hex_code].get('first_seen')
                        local_aircraft_cache[hex_code] = ac
                        if first_seen: local_aircraft_cache[hex_code]['first_seen'] = first_seen
                    else:
                        # Add new entry
                        local_aircraft_cache[hex_code] = ac
                        local_aircraft_cache[hex_code]['first_seen'] = now

                    # Mark if it's a new sighting in this cycle for seen_count increment
                    if hex_code not in previously_seen_hexes:
                        local_aircraft_cache[hex_code]['is_new_sighting'] = True
                    else:
                        # Ensure flag is not carried over from a previous upload cycle
                        local_aircraft_cache[hex_code]['is_new_sighting'] = False

            previously_seen_hexes = current_hexes
            logger.debug(f"Cache status: {len(local_aircraft_cache)} total aircraft cached.")

            # Check if it's time to upload the summary
            should_upload = (current_time - last_summary_upload_time >= config.SUMMARY_UPLOAD_INTERVAL or
                             len(local_aircraft_cache) >= config.MAX_CACHE_SIZE)

            if should_upload and local_aircraft_cache:
                logger.info("Initiating summary data upload...")
                if len(local_aircraft_cache) >= config.MAX_CACHE_SIZE:
                    logger.warning(f"Cache size limit reached ({len(local_aircraft_cache)}), forcing upload.")

                if upload_summary_to_database(local_aircraft_cache):
                    last_summary_upload_time = current_time
                    # Clear the 'is_new_sighting' flag for all cached items after upload
                    for hex_code in local_aircraft_cache:
                        local_aircraft_cache[hex_code]['is_new_sighting'] = False
                    logger.info("Summary upload complete.")
                else:
                    logger.warning("Summary upload failed, keeping cache for next attempt.")

            logger.debug("Sleeping for 60 seconds...")
            time.sleep(60)

        except KeyboardInterrupt:
            logger.info("Shutdown signal received. Attempting final summary upload...")
            if local_aircraft_cache:
                upload_summary_to_database(local_aircraft_cache)
            logger.info("Shutdown complete.")
            break
        except Exception as e:
            logger.error(f"An unexpected error occurred in the main loop: {e}", exc_info=True)
            time.sleep(60)


if __name__ == "__main__":
    # Use the new production-ready ADSBLogger class
    try:
        adsb_logger = ADSBLogger()
        adsb_logger.run()
    except KeyboardInterrupt:
        print("\nKeyboard interrupt received. Shutting down gracefully...")
    except Exception as e:
        print(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
