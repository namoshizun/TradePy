cd /root
export PATH="/root/miniconda3/bin:PATH"
python -m tradepy.cli.autotrade --level WARNING > /root/.tradepy/logs/bot.log 2>&1
