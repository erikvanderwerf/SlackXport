import logging
from pathlib import Path

from slackxport import JsonSlackExport

logging.basicConfig(level=logging.INFO)
TOKEN = Path('TOKEN').read_text('utf-8').strip()

root = Path('export')
root.mkdir(parents=True, exist_ok=True)

export = JsonSlackExport(root, TOKEN)
export.process()
