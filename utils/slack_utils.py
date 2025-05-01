from src.base import BaseConfig
from package.slack_sdk import WebClient
import warnings

warnings.filterwarnings("ignore")


class SlackMessenger:
    def __init__(
        self,
        token: str = BaseConfig.SLACK_TOKEN,
        channel_id: str = BaseConfig.SLACK_CHANNEL,
    ):
        self.client = WebClient(token=token)
        self.channel_id = channel_id

    def send_message(
        self,
        text: str = None,
        block: list = None,
        thread_ts: str = None,
        attachments: list = None,
    ):
        return self.client.chat_postMessage(
            channel=self.channel_id,
            text=text,
            blocks=block,
            thread_ts=thread_ts,
            attachments=attachments,  # attachments 파라미터 추가
        )

    def send_slack(self, text: str, block_message: list = None, color: str = "#ffffff"):
        # validation 결과에 따라 색상 결정

        initial_msg = self.send_message(attachments=[{"color": color, "text": text}])

        if block_message:
            validation_block_message = block_message
            thread_ts = initial_msg.get("ts")
            if thread_ts:
                self.send_message(block=validation_block_message, thread_ts=thread_ts)
            else:
                print(
                    "Warning: Could not get thread_ts from initial Slack message."
                )  # 혹은 로깅
