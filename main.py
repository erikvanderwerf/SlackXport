from pathlib import Path

from slacksport.export import JsonSlackExport

TOKEN = Path('TOKEN').read_text('utf-8').strip()
export = JsonSlackExport(Path('export'), TOKEN)
