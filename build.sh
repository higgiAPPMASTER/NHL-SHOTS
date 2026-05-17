#!/bin/bash
set -e
pip install -r requirements.txt
python -m playwright install chromium
