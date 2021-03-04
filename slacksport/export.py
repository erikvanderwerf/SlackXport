import json
import logging
import os
from pathlib import Path

import slack_sdk


logger = logging.getLogger(__name__)


def skip_if_exists(func):
    def f(path: Path):
        if not os.path.exists(path):
            return func(path)
        else:
            logger.info(f'skipping {func.__name__} because file exists.')

    return f


class JsonSlackExport:
    def __init__(self, root: Path, token: str):
        self.root = root
        self.wc = slack_sdk.web.WebClient(
            token=token,
        )

    def process(self):
        self.pull_channels(self.root / 'channels.json')

        wc.conversations_history()

    @skip_if_exists
    def pull_channels(self, path: Path):
        channels = self.wc.conversations_list().data['channels']
        with open(path, 'x') as fp:
            json.dump(channels, fp)

