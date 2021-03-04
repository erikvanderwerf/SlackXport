import logging
import re
from pathlib import Path

from slackxport import JsonSlackExport

logging.basicConfig(level=logging.INFO)
TOKEN = Path('TOKEN').read_text('utf-8').strip()

root = Path('export')
root.mkdir(parents=True, exist_ok=True)

export = JsonSlackExport(root, TOKEN)


# export.pull_channels(into=root / 'channels.json')

channels_dir = (root / 'channels')
channels_dir.mkdir(exist_ok=True)

# export.pull_channel_history("C266MTRE0", into=root / 'channels' / 'dinner.json')
export.pull_channel_history_by(pattern=re.compile('.*'), into=channels_dir)
