from dataclasses import dataclass


@dataclass(frozen=True)
class DemoConfig:
    total_items: int = 600
    page_size: int = 30
    payload_size: int = 128
    api_latency: float = 0.010
    api_jitter: float = 0.0008
    db_latency: float = 0.0010


config = DemoConfig()