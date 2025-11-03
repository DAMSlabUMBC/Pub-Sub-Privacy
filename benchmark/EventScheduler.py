import time
import heapq
from typing import Callable, Dict, List, Optional, Any
from dataclasses import dataclass, field


@dataclass(order=True)
class ScheduledEvent:
    """Represents an event scheduled at a specific time"""
    time_ms: float = field(compare=True)
    event_type: str = field(compare=False)
    params: Dict[str, Any] = field(default_factory=dict, compare=False)
    description: str = field(default="", compare=False)


class EventScheduler:
    """Deterministic event scheduler - events fire at exact times"""

    def __init__(self):
        self.events: List[ScheduledEvent] = []  # Min heap
        self.handlers: Dict[str, Callable] = {}
        self.start_time: Optional[float] = None
        self.is_running: bool = False

    def schedule_event(self, time_ms: float, event_type: str,
                      params: Optional[Dict[str, Any]] = None,
                      description: str = ""):
        """Schedule an event at a specific time (ms from test start)

        Parameters
        ----------
        time_ms : float
            Time when event should fire
        event_type : str
            Type of event
        params : dict, optional
            Parameters for the handler
        description : str, optional
            Description of the event
        """
        if params is None:
            params = {}

        event = ScheduledEvent(
            time_ms=time_ms,
            event_type=event_type,
            params=params,
            description=description
        )
        heapq.heappush(self.events, event)
        print(f"[EventScheduler] Scheduled '{event_type}' at {time_ms}ms: {description}")

    def register_handler(self, event_type: str, handler: Callable):
        """Register a handler function for an event type

        Parameters
        ----------
        event_type : str
            Event type to handle
        handler : callable
            Function to call when event fires
        """
        self.handlers[event_type] = handler
        print(f"[EventScheduler] Registered handler for '{event_type}'")

    def start(self):
        """Start the scheduler (record start time)"""
        self.start_time = time.monotonic()
        self.is_running = True
        print(f"[EventScheduler] Started at {time.strftime('%Y-%m-%d %H:%M:%S')}")

    def stop(self):
        """Stop the scheduler"""
        self.is_running = False
        print(f"[EventScheduler] Stopped")

    def get_elapsed_ms(self) -> Optional[float]:
        """Get elapsed time in milliseconds since start"""
        if self.start_time is None:
            return None
        return (time.monotonic() - self.start_time) * 1000.0

    def process_due_events(self):
        """Process events that are due to fire based on elapsed time"""
        if not self.is_running or self.start_time is None:
            return

        elapsed_ms = self.get_elapsed_ms()

        # Process all events that are due
        while self.events and self.events[0].time_ms <= elapsed_ms:
            event = heapq.heappop(self.events)
            self._fire_event(event)

    def get_next_event_time_ms(self) -> Optional[float]:
        """Get time (ms from start) when next event fires, or None if no events"""
        if not self.events:
            return None
        return self.events[0].time_ms

    def get_time_until_next_event_ms(self) -> Optional[float]:
        """Get time remaining until next event, or None"""
        if not self.events or self.start_time is None:
            return None

        elapsed_ms = self.get_elapsed_ms()
        next_event_ms = self.events[0].time_ms
        remaining_ms = max(0, next_event_ms - elapsed_ms)

        return remaining_ms

    def has_pending_events(self) -> bool:
        """Check if there are any pending events"""
        return len(self.events) > 0

    def clear(self):
        """Clear all scheduled events"""
        self.events = []
        self.start_time = None
        self.is_running = False
        print(f"[EventScheduler] Cleared all events")

    def _fire_event(self, event: ScheduledEvent):
        """Fire an event by calling its registered handler"""
        print(f"[EventScheduler] Firing event '{event.event_type}' at "
              f"{self.get_elapsed_ms():.0f}ms: {event.description}")

        handler = self.handlers.get(event.event_type)
        if handler:
            try:
                handler(event.params)
            except Exception as e:
                print(f"[EventScheduler] ERROR in handler for '{event.event_type}': {e}")
                raise
        else:
            print(f"[EventScheduler] WARNING: No handler registered for event type '{event.event_type}'")

    def get_scheduled_events(self) -> List[ScheduledEvent]:
        """Get a list of all scheduled events (sorted by time)"""
        return sorted(self.events)

    def print_schedule(self):
        """Print all scheduled events for debugging"""
        print("\n[EventScheduler] Scheduled Events:")
        print("-" * 80)
        for event in self.get_scheduled_events():
            print(f"  {event.time_ms:>8.0f}ms | {event.event_type:20s} | {event.description}")
        print("-" * 80)
