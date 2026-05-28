import requests as rq

import tradepy
from tradepy.core.conf import PushPlusWechatNotificationConf
from tradepy.core.exceptions import NotConfiguredError


class PushPlusWechatNotifier:
    def __init__(self) -> None:
        if (conf := tradepy.config.notifications) is None:
            raise NotConfiguredError("未配置通知项")

        if conf.wechat is None:
            raise NotConfiguredError("未配置微信通知项")

        self.conf: PushPlusWechatNotificationConf = conf.wechat
        self.redis_client = tradepy.config.common.get_redis_client()

    @property
    def limit_key(self):
        return "tradepy:notification:wechat:send_count"

    @property
    def send_count(self) -> int:
        count = self.redis_client.get(self.limit_key) or 0
        return int(count)

    @property
    def quota_used(self) -> bool:
        return int(self.send_count) > self.conf.daily_limit

    def send(self, title, content):
        if not self.conf.enabled:
            tradepy.LOG.warn(f"微信推送未启用。取消发送消息: {title}")
            return

        if self.quota_used:
            tradepy.LOG.warn(f"微信推送超过每日限制{self.conf.daily_limit}条, 本次推送被取消")

        url = "http://www.pushplus.plus/send"
        data = {
            "token": self.conf.token,
            "title": title,
            "content": content,
            "template": "txt",
            "topic": self.conf.topic,
            "channel": "wechat",
        }
        res = rq.post(url, data=data)
        if res.status_code != 200:
            tradepy.LOG.error(f"微信推送失败: {res.text}")
        else:
            self.redis_client.incr(self.limit_key)
            tradepy.LOG.info(
                f"微信推送成功, 流水号{res.json()['data']}, 今日已发送{self.send_count}条"
            )
