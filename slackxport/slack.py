from dataclasses import dataclass

__all__ = ["SlackConversation", "SlackFile", "SlackMessage"]


@dataclass
class SlackConversation:
    conversation_id: str
    name: str


@dataclass
class SlackFile:
    file_id: str
    filetype: str
    filename: str
    url_private: str


@dataclass
class SlackMessage:
    kind: str
    ts: str
    reply_count: int

    @property
    def is_thread_root(self):
        return self.reply_count > 0
