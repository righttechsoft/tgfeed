#!/usr/bin/env python3
"""TUI Orchestrator for TGFeed scripts.

Provides a simple terminal interface to manage all TGFeed scripts:
- Start/stop scripts
- View status and logs
- Handle dependencies between scripts

Usage:
    uv run python orchestrator.py
"""

import os
import subprocess
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path

try:
    from rich.console import Console
    from rich.layout import Layout
    from rich.live import Live
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text
except ImportError:
    print("This tool requires 'rich'. Install with: uv add rich")
    sys.exit(1)

# Cross-platform keyboard input
IS_WINDOWS = os.name == 'nt'

if IS_WINDOWS:
    import msvcrt
else:
    import select
    import tty
    import termios

# Maximum log lines to keep per script
MAX_LOG_LINES = 100

# Directory for error logs
ERROR_LOG_DIR = Path(__file__).parent / "data" / "logs"


class ScriptType(Enum):
    DAEMON = "daemon"      # Long-running service
    SYNC = "sync"          # Sync scripts (run once or in loop)
    MAINTENANCE = "maint"  # Maintenance tasks


@dataclass
class Chain:
    """A sequence of scripts that run one after another in a loop."""
    name: str
    description: str
    scripts: list[str]  # Script names in order
    active: bool = False
    current_index: int = 0
    iteration: int = 0

    @property
    def current_script(self) -> str | None:
        if not self.active or not self.scripts:
            return None
        return self.scripts[self.current_index]

    @property
    def status(self) -> str:
        if self.active:
            return f"[green]Running[/green] ({self.current_index + 1}/{len(self.scripts)})"
        return "[dim]Stopped[/dim]"

    def advance(self) -> str:
        """Move to next script in chain. Returns next script name."""
        self.current_index = (self.current_index + 1) % len(self.scripts)
        if self.current_index == 0:
            self.iteration += 1
        return self.scripts[self.current_index]

    def reset(self):
        """Reset chain state."""
        self.active = False
        self.current_index = 0
        self.iteration = 0


# Define chains - sequences of scripts that run in a loop
CHAINS = [
    Chain(
        name="sync",
        description="Main sync loop: read status, channels, messages, telegraph",
        scripts=["read-sync", "channels", "messages", "telegraph"],
    ),
    Chain(
        name="maintenance",
        description="Maintenance tasks: thumbnails, hashes, search, cleanup",
        scripts=["thumbnails", "hashes", "search", "cleanup"],
    ),
]


@dataclass
class Script:
    name: str
    path: str
    description: str
    script_type: ScriptType
    depends_on: list[str] = field(default_factory=list)
    process: subprocess.Popen | None = None
    logs: deque = field(default_factory=lambda: deque(maxlen=MAX_LOG_LINES))
    exit_code: int | None = None
    last_error_log: str | None = None  # Path to last error log file

    @property
    def is_running(self) -> bool:
        if self.process is None:
            return False
        return self.process.poll() is None

    @property
    def status(self) -> str:
        if self.is_running:
            return "[green]Running[/green]"
        elif self.exit_code is not None:
            if self.exit_code == 0:
                return "[cyan]Completed[/cyan]"
            else:
                return f"[red]Failed ({self.exit_code})[/red]"
        return "[dim]Stopped[/dim]"


# Define all scripts with their dependencies
SCRIPTS = [
    Script(
        name="daemon",
        path="tg_daemon.py",
        description="Telegram connection daemon",
        script_type=ScriptType.DAEMON,
    ),
    Script(
        name="web",
        path="web.py",
        description="Web UI server",
        script_type=ScriptType.DAEMON,
    ),
    Script(
        name="channels",
        path="sync_channels.py",
        description="Sync channel list from Telegram",
        script_type=ScriptType.SYNC,
        depends_on=["daemon"],
    ),
    Script(
        name="messages",
        path="sync_messages.py",
        description="Sync new messages from channels",
        script_type=ScriptType.SYNC,
        depends_on=["daemon", "channels"],
    ),
    Script(
        name="history",
        path="sync_history.py",
        description="Download historical messages",
        script_type=ScriptType.SYNC,
        depends_on=["daemon"],
    ),
    Script(
        name="read-sync",
        path="sync_read_to_tg.py",
        description="Sync read status to Telegram",
        script_type=ScriptType.SYNC,
        depends_on=["daemon"],
    ),
    Script(
        name="telegraph",
        path="download_telegraph.py",
        description="Download telegra.ph pages",
        script_type=ScriptType.MAINTENANCE,
    ),
    Script(
        name="thumbnails",
        path="generate_thumbnails.py",
        description="Generate video thumbnails",
        script_type=ScriptType.MAINTENANCE,
    ),
    Script(
        name="hashes",
        path="generate_content_hashes.py",
        description="Generate content hashes for dedup",
        script_type=ScriptType.MAINTENANCE,
    ),
    Script(
        name="search",
        path="index_search.py",
        description="Index messages for search",
        script_type=ScriptType.MAINTENANCE,
    ),
    Script(
        name="cleanup",
        path="cleanup.py",
        description="Clean up old messages",
        script_type=ScriptType.MAINTENANCE,
    ),
]


class Orchestrator:
    def __init__(self):
        self.console = Console()
        self.scripts = {s.name: s for s in SCRIPTS}
        self.chains = {c.name: c for c in CHAINS}
        self.selected_index = 0
        self.show_logs_for: str | None = None
        self.running = True
        self.base_path = Path(__file__).parent
        self.message = ""
        self.message_time = 0
        self._chain_lock = threading.Lock()

    def get_script_list(self) -> list[Script]:
        return list(self.scripts.values())

    def get_chain_list(self) -> list[Chain]:
        return list(self.chains.values())

    def start_script(self, name: str) -> bool:
        """Start a script. Returns True if started successfully."""
        script = self.scripts.get(name)
        if not script:
            self.set_message(f"Unknown script: {name}", error=True)
            return False

        if script.is_running:
            self.set_message(f"{name} is already running")
            return False

        # Start the process
        script_path = self.base_path / script.path
        if not script_path.exists():
            self.set_message(f"Script not found: {script.path}", error=True)
            return False

        try:
            script.logs.clear()
            script.exit_code = None
            script.process = subprocess.Popen(
                [sys.executable, str(script_path)],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding='utf-8',
                errors='replace',
                bufsize=1,
                cwd=str(self.base_path),
            )

            # Start log reader thread
            thread = threading.Thread(
                target=self._read_logs,
                args=(script,),
                daemon=True
            )
            thread.start()

            self.set_message(f"Started {name}")
            return True
        except Exception as e:
            self.set_message(f"Failed to start {name}: {e}", error=True)
            return False

    def stop_script(self, name: str) -> bool:
        """Stop a running script."""
        script = self.scripts.get(name)
        if not script:
            self.set_message(f"Unknown script: {name}", error=True)
            return False

        if not script.is_running:
            self.set_message(f"{name} is not running")
            return False

        try:
            script.process.terminate()
            script.process.wait(timeout=5)
            self.set_message(f"Stopped {name}")
            return True
        except subprocess.TimeoutExpired:
            script.process.kill()
            self.set_message(f"Killed {name} (didn't respond to terminate)")
            return True
        except Exception as e:
            self.set_message(f"Failed to stop {name}: {e}", error=True)
            return False

    def _read_logs(self, script: Script):
        """Read logs from a running script (runs in thread)."""
        try:
            while script.process and script.process.poll() is None:
                line = script.process.stdout.readline()
                if line:
                    script.logs.append(line.rstrip())
            # Read remaining output
            if script.process:
                for line in script.process.stdout:
                    script.logs.append(line.rstrip())
                script.exit_code = script.process.returncode

                # Save logs to file if script failed
                if script.exit_code != 0:
                    self._save_error_log(script)
        except Exception as e:
            script.logs.append(f"[Error reading logs: {e}]")

    def _save_error_log(self, script: Script):
        """Save script logs to a file when it fails."""
        try:
            ERROR_LOG_DIR.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_file = ERROR_LOG_DIR / f"{script.name}_{timestamp}.log"

            with open(log_file, "w", encoding="utf-8") as f:
                f.write(f"Script: {script.name} ({script.path})\n")
                f.write(f"Exit code: {script.exit_code}\n")
                f.write(f"Time: {datetime.now().isoformat()}\n")
                f.write("=" * 50 + "\n\n")
                for line in script.logs:
                    f.write(line + "\n")

            script.last_error_log = str(log_file)
            self.set_message(f"{script.name} failed - log saved to {log_file.name}", error=True)
        except Exception as e:
            self.set_message(f"Failed to save error log: {e}", error=True)

    def start_chain(self, name: str) -> bool:
        """Start a chain running in loop mode."""
        chain = self.chains.get(name)
        if not chain:
            self.set_message(f"Unknown chain: {name}", error=True)
            return False

        if chain.active:
            self.set_message(f"Chain '{name}' is already running")
            return False

        # Check that all scripts in chain exist
        for script_name in chain.scripts:
            if script_name not in self.scripts:
                self.set_message(f"Chain references unknown script: {script_name}", error=True)
                return False

        # Reset and activate chain
        chain.reset()
        chain.active = True

        # Start the chain monitor thread
        thread = threading.Thread(
            target=self._run_chain,
            args=(chain,),
            daemon=True
        )
        thread.start()

        self.set_message(f"Started chain '{name}'")
        return True

    def stop_chain(self, name: str) -> bool:
        """Stop a running chain."""
        chain = self.chains.get(name)
        if not chain:
            self.set_message(f"Unknown chain: {name}", error=True)
            return False

        if not chain.active:
            self.set_message(f"Chain '{name}' is not running")
            return False

        with self._chain_lock:
            chain.active = False

        # Stop the currently running script from the chain
        current_script = chain.current_script
        if current_script:
            script = self.scripts.get(current_script)
            if script and script.is_running:
                self.stop_script(current_script)

        chain.reset()
        self.set_message(f"Stopped chain '{name}'")
        return True

    def _run_chain(self, chain: Chain):
        """Run a chain in a loop (runs in thread)."""
        while chain.active and self.running:
            script_name = chain.scripts[chain.current_index]
            script = self.scripts.get(script_name)

            if not script:
                chain.active = False
                break

            # Check daemon dependencies before starting
            # (sync script dependencies are handled by chain ordering)
            deps_ok = True
            for dep_name in script.depends_on:
                dep = self.scripts.get(dep_name)
                if dep and dep.script_type == ScriptType.DAEMON and not dep.is_running:
                    # Try to auto-start daemon dependencies
                    self.start_script(dep_name)
                    time.sleep(1)  # Give it time to start
                    if not dep.is_running:
                        deps_ok = False
                        break

            if not deps_ok:
                # Wait and retry
                time.sleep(2)
                continue

            # Start the script if not already running
            if not script.is_running:
                self.start_script(script_name)

            # Wait for script to complete
            while script.is_running and chain.active and self.running:
                time.sleep(0.5)

            if not chain.active or not self.running:
                break

            # Move to next script
            with self._chain_lock:
                if chain.active:
                    chain.advance()

            # Small delay between scripts
            time.sleep(1)

    def get_script_chain(self, script_name: str) -> Chain | None:
        """Check if a script is part of an active chain."""
        for chain in self.chains.values():
            if chain.active and script_name in chain.scripts:
                return chain
        return None

    def set_message(self, msg: str, error: bool = False):
        """Set a status message to display."""
        self.message = f"[red]{msg}[/red]" if error else f"[green]{msg}[/green]"
        self.message_time = time.time()

    def render_script_table(self) -> Table:
        """Render the script list table."""
        table = Table(
            title="TGFeed Scripts",
            show_header=True,
            header_style="bold magenta",
            border_style="bright_white",
        )
        table.add_column("#", style="dim", width=3)
        table.add_column("Name", width=12)
        table.add_column("Type", width=8)
        table.add_column("Status", width=16)
        table.add_column("Description", width=35)
        table.add_column("Chain", width=16)

        for i, script in enumerate(self.get_script_list()):
            selected = ">" if i == self.selected_index else " "
            row_style = "reverse" if i == self.selected_index else None

            # Check if script is part of active chain
            chain = self.get_script_chain(script.name)
            chain_info = ""
            if chain:
                if chain.current_script == script.name:
                    chain_info = f"[cyan]{chain.name}[/cyan] <-"
                else:
                    chain_info = f"[dim]{chain.name}[/dim]"

            table.add_row(
                f"{selected}{i+1}",
                script.name,
                script.script_type.value,
                script.status,
                script.description,
                chain_info,
                style=row_style,
            )

        return table

    def render_chain_table(self) -> Table:
        """Render the chains table."""
        table = Table(
            title="Chains (Sequential Loops)",
            show_header=True,
            header_style="bold cyan",
            border_style="cyan",
        )
        table.add_column("Key", style="dim", width=4)
        table.add_column("Name", width=12)
        table.add_column("Status", width=18)
        table.add_column("Scripts", width=40)
        table.add_column("Iteration", width=10)

        for i, chain in enumerate(self.get_chain_list()):
            key = f"F{i+1}"
            scripts_display = " → ".join(chain.scripts)
            iteration = f"#{chain.iteration + 1}" if chain.active else "-"

            table.add_row(
                key,
                chain.name,
                chain.status,
                scripts_display,
                iteration,
            )

        return table

    def render_logs(self) -> Panel:
        """Render the log panel."""
        # Calculate available lines based on terminal height
        # Log panel gets 50% of height (ratio 1:1), minus borders/padding
        terminal_height = self.console.height or 40
        log_panel_height = int(terminal_height * 0.5) - 4  # Account for borders, title, padding
        max_lines = max(5, log_panel_height)

        if self.show_logs_for:
            script = self.scripts.get(self.show_logs_for)
            if script:
                # Get the last N lines that will fit
                log_lines = list(script.logs) if script.logs else []
                lines = log_lines[-max_lines:] if log_lines else ["(no logs yet)"]

                # Add error log path if script failed
                if script.exit_code and script.exit_code != 0 and script.last_error_log:
                    lines.append("")
                    lines.append(f"[yellow]Full log saved to: {script.last_error_log}[/yellow]")

                # Pad to fixed height to prevent jumping
                while len(lines) < max_lines:
                    lines.insert(0, "")

                # Determine border style
                if script.is_running:
                    border_style = "green"
                elif script.exit_code and script.exit_code != 0:
                    border_style = "red"
                else:
                    border_style = "dim"

                return Panel(
                    "\n".join(lines),
                    title=f"Logs: {script.name}",
                    border_style=border_style,
                )

        return Panel(
            "[dim]Select a script and press [L] to view logs[/dim]",
            title="Logs",
            border_style="dim",
        )

    def render_help(self) -> Panel:
        """Render the help panel."""
        help_text = """[bold]Scripts:[/bold]
  [cyan]↑/↓[/cyan] [cyan]j/k[/cyan]  Navigate
  [cyan]Enter[/cyan] [cyan]s[/cyan]  Start script
  [cyan]x[/cyan]       Stop script
  [cyan]l[/cyan]       View logs
  [cyan]a[/cyan]       Start daemons

[bold]Chains:[/bold]
  [cyan]F1[/cyan]      Toggle sync chain
  [cyan]F2[/cyan]      Toggle maint chain

  [cyan]q[/cyan]       Quit"""

        # Add message if recent
        if self.message and time.time() - self.message_time < 5:
            help_text += f"\n\n{self.message}"

        return Panel(help_text, title="Help", border_style="yellow")

    def render(self) -> Layout:
        """Render the full layout."""
        from rich.console import Group

        layout = Layout()

        layout.split_column(
            Layout(name="tables", ratio=1),
            Layout(name="bottom", ratio=1),
        )

        # Combine script table and chain table
        tables_group = Group(
            self.render_script_table(),
            self.render_chain_table(),
        )

        layout["tables"].split_row(
            Layout(tables_group, name="scripts", ratio=3),
            Layout(self.render_help(), name="help", ratio=1),
        )

        layout["bottom"].update(self.render_logs())

        return layout

    def handle_input(self, key: str):
        """Handle keyboard input."""
        scripts_list = self.get_script_list()
        chains_list = self.get_chain_list()
        num_scripts = len(scripts_list)

        if key in ("up", "k"):
            self.selected_index = (self.selected_index - 1) % num_scripts
            self.show_logs_for = scripts_list[self.selected_index].name
        elif key in ("down", "j"):
            self.selected_index = (self.selected_index + 1) % num_scripts
            self.show_logs_for = scripts_list[self.selected_index].name
        elif key in ("enter", "s"):
            script = scripts_list[self.selected_index]
            self.start_script(script.name)
        elif key == "x":
            script = scripts_list[self.selected_index]
            self.stop_script(script.name)
        elif key == "l":
            script = scripts_list[self.selected_index]
            if self.show_logs_for == script.name:
                self.show_logs_for = None
            else:
                self.show_logs_for = script.name
        elif key == "a":
            # Start all daemons
            for script in scripts_list:
                if script.script_type == ScriptType.DAEMON and not script.is_running:
                    self.start_script(script.name)
        elif key == "q":
            self.running = False
        elif key.isdigit():
            idx = int(key) - 1
            if 0 <= idx < num_scripts:
                self.selected_index = idx
        elif key.startswith("f") and len(key) > 1:
            # Handle function keys for chains (F1, F2, etc.)
            try:
                chain_idx = int(key[1:]) - 1
                if 0 <= chain_idx < len(chains_list):
                    chain = chains_list[chain_idx]
                    if chain.active:
                        self.stop_chain(chain.name)
                    else:
                        self.start_chain(chain.name)
            except ValueError:
                pass

    def stop_all(self):
        """Stop all running chains and scripts."""
        # First stop all chains
        for chain in self.get_chain_list():
            if chain.active:
                chain.active = False
                chain.reset()

        # Then stop scripts in reverse dependency order
        for script in reversed(self.get_script_list()):
            if script.is_running:
                self.stop_script(script.name)

    def start_all(self):
        """Start all daemons, chains, and the history script."""
        # Start both daemons first
        for script in self.get_script_list():
            if script.script_type == ScriptType.DAEMON and not script.is_running:
                self.start_script(script.name)

        # Give daemons time to start
        time.sleep(1)

        # Start both chains
        for chain in self.get_chain_list():
            if not chain.active:
                self.start_chain(chain.name)

        # Start history script
        history_script = self.scripts.get("history")
        if history_script and not history_script.is_running:
            self.start_script("history")

    def _get_key_windows(self) -> str | None:
        """Get keyboard input on Windows."""
        if msvcrt.kbhit():
            key = msvcrt.getch()
            # Handle special keys (arrow keys and function keys)
            if key in (b'\x00', b'\xe0'):
                key2 = msvcrt.getch()
                if key2 == b'H':
                    return "up"
                elif key2 == b'P':
                    return "down"
                # Function keys: F1=0x3B, F2=0x3C, F3=0x3D, etc.
                elif key2 == b';':  # F1
                    return "f1"
                elif key2 == b'<':  # F2
                    return "f2"
                elif key2 == b'=':  # F3
                    return "f3"
                elif key2 == b'>':  # F4
                    return "f4"
                return None
            # Handle enter
            elif key == b'\r':
                return "enter"
            # Regular key
            try:
                return key.decode('utf-8')
            except UnicodeDecodeError:
                return None
        return None

    def _get_key_unix(self) -> str | None:
        """Get keyboard input on Unix."""
        if select.select([sys.stdin], [], [], 0.25)[0]:
            key = sys.stdin.read(1)

            # Handle escape sequences (arrows, function keys)
            if key == "\x1b":
                # Read more characters for the sequence
                if select.select([sys.stdin], [], [], 0.05)[0]:
                    key += sys.stdin.read(1)
                    if key == "\x1b[":
                        # CSI sequence - read until we get the final character
                        while select.select([sys.stdin], [], [], 0.05)[0]:
                            c = sys.stdin.read(1)
                            key += c
                            if c.isalpha() or c == "~":
                                break
                    elif key == "\x1bO":
                        # SS3 sequence (function keys)
                        if select.select([sys.stdin], [], [], 0.05)[0]:
                            key += sys.stdin.read(1)

                # Parse the sequence
                if key == "\x1b[A":
                    return "up"
                elif key == "\x1b[B":
                    return "down"
                # Function keys: \x1bOP=F1, \x1bOQ=F2, \x1bOR=F3, \x1bOS=F4
                elif key == "\x1bOP" or key == "\x1b[11~":
                    return "f1"
                elif key == "\x1bOQ" or key == "\x1b[12~":
                    return "f2"
                elif key == "\x1bOR" or key == "\x1b[13~":
                    return "f3"
                elif key == "\x1bOS" or key == "\x1b[14~":
                    return "f4"
                return None

            # Handle enter
            elif key == "\r" or key == "\n":
                return "enter"

            return key
        return None

    def run(self):
        """Run the orchestrator TUI."""
        if IS_WINDOWS:
            self._run_windows()
        else:
            self._run_unix()

    def _run_windows(self):
        """Run on Windows using msvcrt."""
        # Auto-start everything on launch
        self.start_all()

        try:
            with Live(self.render(), console=self.console, refresh_per_second=4, screen=True) as live:
                while self.running:
                    key = self._get_key_windows()
                    if key:
                        self.handle_input(key)
                    else:
                        time.sleep(0.05)  # Small delay to prevent busy-waiting
                    live.update(self.render())

        except KeyboardInterrupt:
            pass
        finally:
            # Stop all scripts
            self.console.print("\n[yellow]Stopping all scripts...[/yellow]")
            self.stop_all()
            self.console.print("[green]Done.[/green]")

    def _run_unix(self):
        """Run on Unix using termios."""
        # Auto-start everything on launch
        self.start_all()

        # Save terminal settings
        old_settings = termios.tcgetattr(sys.stdin)

        try:
            # Set terminal to raw mode for key input
            tty.setcbreak(sys.stdin.fileno())

            with Live(self.render(), console=self.console, refresh_per_second=4, screen=True) as live:
                while self.running:
                    key = self._get_key_unix()
                    if key:
                        self.handle_input(key)
                    live.update(self.render())

        except KeyboardInterrupt:
            pass
        finally:
            # Restore terminal settings
            termios.tcsetattr(sys.stdin, termios.TCSADRAIN, old_settings)

            # Stop all scripts
            self.console.print("\n[yellow]Stopping all scripts...[/yellow]")
            self.stop_all()
            self.console.print("[green]Done.[/green]")


def main():
    orchestrator = Orchestrator()
    orchestrator.run()


if __name__ == "__main__":
    main()
