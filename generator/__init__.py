"""marketplace-streaming generator package.

Public surface:
  MarketplaceGenerator  — main generator class
  run_generator         — convenience function
  InMemorySink          — test-friendly recording sink
  KafkaSink             — runtime Kafka/Redpanda sink
  FaultConfig           — fault injection configuration dataclass
  FaultHarness          — fault application engine
  SimClock              — real-time accelerated simulation clock
  FixedClock            — deterministic fixed clock (for tests)
"""

from generator.clock import FixedClock, SimClock
from generator.fault_injection import FaultConfig, FaultHarness
from generator.generator import MarketplaceGenerator, run_generator
from generator.sink import InMemorySink, KafkaSink, Sink

__all__ = [
    "FaultConfig",
    "FaultHarness",
    "FixedClock",
    "InMemorySink",
    "KafkaSink",
    "MarketplaceGenerator",
    "SimClock",
    "Sink",
    "run_generator",
]
