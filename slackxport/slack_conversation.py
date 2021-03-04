from dataclasses import dataclass

__all__ = ['SlackConversation']


@dataclass
class SlackConversation:
    conversation_id: str
    name: str
