from dataclasses import dataclass

__all__ = ["SlackConversation", "SlackMessage"]


@dataclass
class SlackConversation:
    conversation_id: str
    name: str


@dataclass
class SlackMessage:
    kind: str
    ts: str
    reply_count: str

    @property
    def is_thread_root(self):
        return self.reply_count > 0
