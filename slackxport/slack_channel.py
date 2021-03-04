from dataclasses import dataclass

__all__ = ['SlackChannel']


@dataclass
class SlackChannel:
    channel_id: str
    name: str
