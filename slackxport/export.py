import json
import logging
import os
import re
from functools import wraps
from pathlib import Path
from re import Pattern
from typing import Iterator, Union, List

import requests
import slack_sdk
from ratelimit import limits, sleep_and_retry

from slackxport.exceptions import SlackXportException
from slackxport.slack import SlackConversation, SlackMessage, SlackFile

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


def list_page_endpoint(callback, extract, has_more=None, get_page=None, args=None, kwargs=None) -> List:
    has_more = has_more if has_more else lambda d: "" != d.get("response_metadata", {}).get("next_cursor", "")
    get_page = get_page if get_page else lambda d: ("cursor", d["response_metadata"]["next_cursor"])

    args = args or tuple()
    kwargs = kwargs or dict()

    data = callback(*args, **kwargs).data

    lst = extract(data)
    while has_more(data):
        key, page = get_page(data)
        kwargs[key] = page
        data = callback(*args, **kwargs).data
        lst.extend(extract(data))
    return lst


class ExportedSlackConversation(SlackConversation):
    def __init__(self, conversation_id: str, name: str, root: Path):
        super(ExportedSlackConversation, self).__init__(conversation_id=conversation_id, name=name)
        self.root = root

    @classmethod
    def of(cls, conversation: SlackConversation, root: Path) -> "ExportedSlackConversation":
        return ExportedSlackConversation(conversation.conversation_id, conversation.name, root)

    def history_file(self) -> Path:
        return self.root / "history.json"

    def meta_file(self) -> Path:
        return self.root / "meta.json"

    def members_file(self) -> Path:
        return self.root / "members.json"

    def replies_file_for(self, message: SlackMessage) -> Path:
        return self.root / "replies" / f"{message.ts}.json"

    def pins_file(self) -> Path:
        return self.root / "pins.json"


class JsonSlackExport:
    def __init__(self, root: Path, token: str, *, pull_if_not_found: bool = True):
        self.pull_if_not_found = pull_if_not_found
        self.root = root
        self.token = token
        self.wc = slack_sdk.web.WebClient(
            token=token,
        )

    def conversations_file(self) -> Path:
        return self.root / "conversations.json"

    def users_file(self) -> Path:
        return self.root / "users.json"

    def conversations_dir(self) -> Path:
        return self.root / "conversations"

    def conversation_dir(self, conversation: SlackConversation):
        return self.conversations_dir() / conversation.name

    def emojis_file(self) -> Path:
        return self.root / "emojis.json"

    def files_file(self) -> Path:
        return self.root / "files.json"

    def files_dir(self) -> Path:
        return self.root / "files"

    def conversations(self) -> Iterator[ExportedSlackConversation]:
        conversations_file = self.conversations_file()
        if not conversations_file.exists() and self.pull_if_not_found:
            self.pull_conversations(conversations_file)

        with open(conversations_file, 'r') as fp:
            j = json.load(fp)

        for jc in j:
            conversation = SlackConversation(conversation_id=jc["id"], name=jc.get("name") or jc["user"])
            yield ExportedSlackConversation.of(
                conversation,
                root=self.conversation_dir(conversation)
            )

    def files(self) -> Iterator[SlackFile]:
        files_file = self.files_file()
        if not files_file.exists() and self.pull_if_not_found:
            self.pull_files_meta(files_file)

        with open(files_file, 'r') as fp:
            fj = json.load(fp)

        for f in fj:
            # yield f
            yield SlackFile(
                file_id=f["id"],
                filetype=os.path.splitext(f["name"])[1][1:] or f["filetype"] or f["mimetype"].split("/")[-1],
                filename=f["name"],
                url_private=f["url_private"]
            )

    def get_conversation_by_id(self, conversation_id: str) -> ExportedSlackConversation:
        for c in self.conversations():
            if conversation_id == c.conversation_id:
                return c
        else:
            raise KeyError(conversation_id)

    def messages(self, conversation: ExportedSlackConversation) -> Iterator[SlackMessage]:
        history_file = conversation.history_file()
        if not history_file.exists() and self.pull_if_not_found:
            self.pull_conversation_history(conversation, into=history_file)

        with open(history_file, 'r') as fp:
            mj = json.load(fp)

        m = None
        try:
            for m in mj:
                yield SlackMessage(
                    kind=m["type"],
                    ts=m["ts"],
                    reply_count=m.get('reply_count', 0)
                )
        except KeyError:
            raise SlackXportException(m)

    def process(self):
        self.pull_conversations(into=self.conversations_file())
        self.pull_users(into=self.users_file())
        self.pull_emojis(into=self.emojis_file())
        self.pull_files_meta(into=self.files_file())

        conversations_dir = self.conversations_dir()
        conversations_dir.mkdir(exist_ok=True)
        self.pull_conversation_history_by(pattern=re.compile(".*"))

        files_dir = self.files_dir()
        files_dir.mkdir(exist_ok=True)
        self.pull_all_files(into=files_dir)

    @skip_if_exists
    def pull_conversations(self, *, into: Path):
        """Pull a JSON file of all conversation data the user belongs to."""
        logger.info(f"pulling conversations metadata.")
        conversations = list_page_endpoint(
            callback=self.wc.conversations_list,
            kwargs={"types": "public_channel,private_channel,mpim,im"},
            extract=lambda d: d["channels"]
        )
        JsonSlackExport._json_dump(into, conversations)

    def pull_conversation_history_by(
            self,
            pattern: Union[Pattern, str],
            meta=True,
            members=True,
            replies=True,
            pins=True,
    ):
        """Pull history for all conversations that match the pattern into a directory with conversation-name files."""
        logger.info(f"pulling conversation history that matches {pattern.pattern!r}.")

        if isinstance(pattern, str):
            pattern = re.compile(pattern)

        for conversation in self.conversations():
            if pattern.match(conversation.name):
                c_dir = self.conversation_dir(conversation)
                c_dir.mkdir(exist_ok=True)
                ec = ExportedSlackConversation.of(conversation, root=c_dir)

                self.pull_conversation_history(conversation=conversation, into=ec.history_file())
                if meta:
                    self.pull_conversation_metadata(
                        conversation=conversation,
                        into=ec.meta_file()
                    )
                if members:
                    self.pull_conversation_members(
                        conversation=conversation,
                        into=ec.members_file()
                    )
                if replies:
                    self.pull_conversation_replies(conversation=conversation)
                if pins:
                    self.pull_conversation_pins(conversation=conversation, into=ec.pins_file())

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

    def pull_conversation_replies(self, conversation: Union[ExportedSlackConversation, str]):
        """Pull conversation replies into a directory."""
        if isinstance(conversation, str):
            conversation = self.get_conversation_by_id(conversation)

        logger.info(f"checking replies for {conversation}.")

        for m in filter(lambda x: x.reply_count > 0, self.messages(conversation)):
            into = conversation.replies_file_for(m)
            if not into.exists():
                into.parent.mkdir(parents=True, exist_ok=True)
                replies = list_page_endpoint(
                    callback=self.wc.conversations_replies,
                    kwargs={'channel': conversation.conversation_id, 'ts': m.ts},
                    extract=lambda d: d["messages"]
                )
                JsonSlackExport._json_dump(into, replies)

    @skip_if_exists
    def pull_conversation_pins(self, *, conversation: Union[ExportedSlackConversation, str], into: Path):
        """Pull server file metadata into file."""
        if isinstance(conversation, str):
            conversation = self.get_conversation_by_id(conversation)

        logger.info(f"pulling pins for {conversation}.")

        pins = list_page_endpoint(
            callback=self.wc.pins_list,
            kwargs={'channel': conversation.conversation_id},
            extract=lambda d: d["items"],
        )
        JsonSlackExport._json_dump(into, pins)

    @skip_if_exists
    def pull_users(self, *, into: Path):
        """Pull user metadata into file."""
        logger.info("pulling users metadata.")
        users = list_page_endpoint(
            callback=self.wc.users_list,
            extract=lambda d: d["members"]
        )
        JsonSlackExport._json_dump(into, users)

    @skip_if_exists
    def pull_emojis(self, *, into: Path):
        """Pull server emoji metadata into file."""
        logger.info("pulling server emojis.")
        emojis = list_page_endpoint(
            callback=self.wc.emoji_list,
            extract=lambda d: d["emoji"]
        )
        JsonSlackExport._json_dump(into, emojis)

    @skip_if_exists
    def pull_files_meta(self, *, into: Path):
        """Pull server file metadata into file."""
        logger.info("pulling files metadata.")
        files = list_page_endpoint(
            callback=self.wc.files_list,
            extract=lambda d: d["files"],
            has_more=lambda d: d["paging"]["page"] < d["paging"]["pages"],
            get_page=lambda d: ("page", d["paging"]["page"] + 1)
        )
        JsonSlackExport._json_dump(into, files)

    def pull_all_files(self, *, into: Path):
        if not into.is_dir():
            raise ValueError("into must be dir.")

        files = list(self.files())
        files_len = len(files)
        for n, slack_file in enumerate(files):
            out_file = into / f"{slack_file.file_id}.{slack_file.filetype}"
            if out_file.exists():
                continue
            logger.info(f"{n}/{files_len}: {slack_file.filename!r} -> {out_file.as_posix()!r}")
            self.pull_single_file(out_file, slack_file)

    @sleep_and_retry
    @limits(calls=1, period=3)  # 20 per minute
    def pull_single_file(self, into: Path, slack_file: SlackFile):
        r = requests.get(
            slack_file.url_private,
            headers={"Authorization": f"Bearer {self.token}"}
        )
        if r.status_code != 200:
            raise SlackXportException(f"File download failed. HTTP {r.status_code}: {r.text}")
        with open(into, "xb") as fp:
            fp.write(r.content)

    @staticmethod
    def _json_dump(into, j):
        with open(into, 'x') as fp:
            json.dump(j, fp, indent=2)
