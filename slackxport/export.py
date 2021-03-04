import json
import logging
import os
import re
from functools import wraps
from pathlib import Path
from re import Pattern
from typing import Iterator, Union, List

import slack_sdk

from slackxport.slack_conversation import SlackConversation

__all__ = ["JsonSlackExport"]
logger = logging.getLogger(__name__)


def skip_if_exists(func):
    """Skip decorated execution if argument :into: exists."""
    @wraps(func)
    def f(*args, into, **kwargs):
        if not os.path.exists(into):
            return func(*args, into=into, **kwargs)
        else:
            logger.info(f'skipping {func.__name__}({into=}) because the file exists.')

    return f


def list_page_endpoint(callback, extract, has_more=None, get_cursor=None, args=None, kwargs=None) -> List:
    has_more = has_more if has_more else lambda d: "" != d.get("response_metadata", {}).get("next_cursor", "")
    get_cursor = get_cursor if get_cursor else lambda d: d["response_metadata"]["next_cursor"]

    args = args or tuple()
    kwargs = kwargs or dict()

    data = callback(*args, **kwargs).data
    lst = extract(data)
    while has_more(data):
        cursor = get_cursor(data)
        data = callback(*args, cursor=cursor, **kwargs).data
        lst.append(extract(data))
    return lst


class JsonSlackExport:
    def __init__(self, root: Path, token: str, *, pull_if_not_found: bool = True):
        self.pull_if_not_found = pull_if_not_found
        self.root = root
        self.wc = slack_sdk.web.WebClient(
            token=token,
        )

    def conversations_file(self):
        return self.root / "conversations.json"

    def users_file(self):
        return self.root / "users.json"

    def conversation_history_dir(self):
        return self.root / "conversations"

    def conversation(self) -> Iterator[SlackConversation]:
        if not self.conversations_file().exists() and self.pull_if_not_found:
            self.pull_conversations(self.conversations_file)

        with open(self.conversations_file(), 'r') as fp:
            j = json.load(fp)

        for jc in j:
            yield SlackConversation(
                conversation_id=jc["id"],
                name=jc["name"]
            )

    def process(self):
        self.pull_conversations(into=self.conversations_file())
        self.pull_users(into=self.conversations_file())

        history_dir = self.conversation_history_dir()
        history_dir.mkdir(exist_ok=True)
        self.pull_conversation_history_by(pattern=re.compile(".*"), into=history_dir)

    @skip_if_exists
    def pull_conversations(self, *, into: Path):
        """Pull a JSON file of all conversation data the user belongs to."""
        logger.info(f"pulling conversations metadata.")
        conversations = list_page_endpoint(
            callback=self.wc.conversations_list,
            extract=lambda d: d["channels"]
        )
        JsonSlackExport._json_dump(into, conversations)

    def pull_conversation_history_by(self, pattern: Union[Pattern, str], into: Path, meta=True, members=True):
        """Pull history for all conversations that match the pattern into a directory with conversation-name files."""
        logger.info(f"pulling conversation history that matches {pattern.pattern!r}.")

        if isinstance(pattern, str):
            pattern = re.compile(pattern)

        if not into.is_dir():
            raise ValueError("into must be dir.")
        meta_dir = into / "meta"
        members_dir = into / "members"
        # replies_dir = into / "replies"

        if meta:
            meta_dir.mkdir(exist_ok=True)
        if members:
            members_dir.mkdir(exist_ok=True)
        # if replies:
        #     replies_dir.mkdir(exist_ok=True)

        for conversation in self.conversation():
            if pattern.match(conversation.name):
                self.pull_conversation_history(conversation=conversation, into=into / f"{conversation.name}.json")
                if meta:
                    self.pull_conversation_metadata(
                        conversation=conversation,
                        into=meta_dir / f"{conversation.name}.meta.json"
                    )
                if members:
                    self.pull_conversation_members(
                        conversation=conversation,
                        into=members_dir / f"{conversation.name}.members.json"
                    )
                # if replies:
                #     self.pull_conversation_replies(
                #         channel=conversation,
                #         into=replies_dir / f"{conversation.name}.replies.json"
                #     )

    @skip_if_exists
    def pull_conversation_history(self, conversation: Union[SlackConversation, str], *, into: Path):
        """Pull history for a specific conversation into a file."""
        logger.info(f"pulling conversation history for {conversation}.")

        if isinstance(conversation, SlackConversation):
            conversation = conversation.conversation_id

        messages = list_page_endpoint(
            callback=self.wc.conversations_history,
            kwargs={'channel': conversation},
            extract=lambda d: d["messages"],
        )

        JsonSlackExport._json_dump(into, messages)

    @skip_if_exists
    def pull_conversation_metadata(self, conversation: Union[SlackConversation, str], *, into: Path):
        """Pull metadata for a specific conversation into a file."""
        logger.info(f"pulling conversation metadata for {conversation}.")

        if isinstance(conversation, SlackConversation):
            conversation = conversation.conversation_id

        metadata = list_page_endpoint(
            callback=self.wc.conversations_info,
            kwargs={'channel': conversation},
            extract=lambda d: d["channel"],
        )

        JsonSlackExport._json_dump(into, metadata)

    @skip_if_exists
    def pull_conversation_members(self, conversation: Union[SlackConversation, str], *, into: Path):
        """Pull members for a specific conversation into a file."""
        logger.info(f"pulling conversation members for {conversation}.")

        if isinstance(conversation, SlackConversation):
            conversation = conversation.conversation_id

        members = list_page_endpoint(
            callback=self.wc.conversations_members,
            kwargs={'channel': conversation},
            extract=lambda d: d["members"],
        )

        JsonSlackExport._json_dump(into, members)

    # @skip_if_exists
    # def pull_conversation_replies(self, conversation: Union[SlackConversation, str], *, into: Path):
    #     logger.info(f"pulling replies for {conversation}.")
    #
    #     if isinstance(conversation, SlackConversation):
    #         conversation = conversation.conversation_id
    #
    #
    #
    #     JsonSlackExport._json_dump(into, replies)

    @skip_if_exists
    def pull_users(self, *, into: Path):
        """Pull user metadata into file."""
        logger.info("pulling users metadata.")
        users = list_page_endpoint(
            callback=self.wc.users_list,
            extract=lambda d: d["members"]
        )

        JsonSlackExport._json_dump(into, users)

    @staticmethod
    def _json_dump(into, j):
        with open(into, 'x') as fp:
            json.dump(j, fp, indent=2)
