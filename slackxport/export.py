import json
import logging
import os
import re
from functools import wraps
from pathlib import Path
from pprint import pprint
from re import Pattern
from typing import Iterator, Union

import slack_sdk


__all__ = ["JsonSlackExport"]

from slackxport.exceptions import SlackXportException
from slackxport.slack_channel import SlackChannel

logger = logging.getLogger(__name__)


def skip_if_exists(func):
    """Skip decorated execution if argument :into: exists."""
    @wraps(func)
    def f(*args, **kwargs):
        kwarg_into = kwargs["into"]
        if not os.path.exists(kwarg_into):
            return func(*args, **kwargs)
        else:
            logger.info(f'skipping {func.__name__} for {kwarg_into} because the file exists.')

    return f


class JsonSlackExport:
    def __init__(self, root: Path, token: str, *, pull_if_not_found: bool = True):
        self.pull_if_not_found = pull_if_not_found
        self.root = root
        self.wc = slack_sdk.web.WebClient(
            token=token,
        )

        self.channels_file = self.root / "channels.json"
        self.channel_history_dir = self.root / "channel"

    def channels(self) -> Iterator[SlackChannel]:
        if not self.channels_file.exists() and self.pull_if_not_found:
            self.pull_channels(self.channels_file)

        with open(self.channels_file, 'r') as fp:
            j = json.load(fp)

        # pprint(j)

        for jc in j:
            yield SlackChannel(
                channel_id=jc["id"],
                name=jc["name"]
            )

    def process(self):
        self.pull_channels(into=self.channels_file)
        self.pull_channel_history_by(pattern=re.compile(".*"), into=self.channel_history_dir)

    @skip_if_exists
    def pull_channels(self, *, into: Path):
        """Pull a JSON file of all channels the user belongs to."""
        data = self.wc.conversations_list().data
        channels = data["channels"]
        JsonSlackExport._json_dump(into, channels)

    def pull_channel_history_by(self, pattern: Pattern, into: Path):
        """Pull history for all channels that match the pattern into a directory with channel-name files."""
        logger.info(f"pulling channel history that matches {pattern}.")

        if not into.is_dir():
            raise ValueError("into must be dir.")

        for channel in self.channels():
            if pattern.match(channel.name):
                self.pull_channel_history(channel=channel, into=into / f"{channel.name}.json")

    @skip_if_exists
    def pull_channel_history(self, channel: Union[SlackChannel, str], *, into: Path):
        """Pull history for a specific channel into a file."""
        logger.info(f"pulling history for {channel}")

        if isinstance(channel, SlackChannel):
            channel = channel.channel_id

        data = self.wc.conversations_history(channel=channel).data
        messages = data["messages"]

        while data["has_more"] is True:
            cursor = data["response_metadata"]["next_cursor"]
            data = self.wc.conversations_history(channel=channel, cursor=cursor).data
            messages.append(data["messages"])

        self._json_dump(into, messages)


    @staticmethod
    def _json_dump(into, j):
        with open(into, 'x') as fp:
            json.dump(j, fp, indent=2)
