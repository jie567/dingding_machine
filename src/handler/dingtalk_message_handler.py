import dingtalk_stream
import logging

from dingtalk_stream import AckMessage, CallbackMessage

from src.config import Config
from src.handler.task_result_sender import TaskResultSender
from src.executor.msg_executor import MessageExecutor
from src.executor.schedule_executor import ScheduleExecutor

logger = logging.getLogger(__name__)


class DingTalkMessageHandler(dingtalk_stream.AsyncChatbotHandler):
    """钉钉消息处理器，集成文件发送和卡片功能"""

    def __init__(self, logger: logging.Logger = None, conf: Config = None, max_workers: int = 8):
        super(DingTalkMessageHandler, self).__init__(max_workers=max_workers)
        if conf:
            self.conf = conf
        else:
            raise ValueError("Please provide config")
        
        self._task_result_sender = TaskResultSender(conf, message_sender_callback=self._send_message_to_group)
        self._message_executor = MessageExecutor()
        self._scheduler_executor = ScheduleExecutor(conf=conf, task_result_sender=self._task_result_sender)
        self._scheduler_executor.start()
        
        if logger:
            self.logger = logger

    def _send_message_to_group(self, chat_id: str, title: str, content: str):
        """
        发送消息到指定群聊（用于定时任务触发时的消息发送）
        :param chat_id: 群聊ID
        :param title: 消息标题
        :param content: 消息内容（支持markdown）
        """
        try:
            open_space_id = self.conf.get_open_space_id(chat_id)
            incoming_message = dingtalk_stream.chatbot.reply_specified_group_chat(open_space_id)
            self.reply_markdown_card(content, incoming_message, title=title, logo="@lALPDfJ6V_FPDmvNAfTNAfQ")
            logger.info(f"消息已发送到群聊 {chat_id}，标题：{title}")
        except Exception as e:
            logger.error(f"发送消息到群聊 {chat_id} 失败：{e}")

    def _get_available_task_list_message(self):
        """获取任务列表消息"""
        available_tasks = self._message_executor.get_available_tasks()
        if not available_tasks:
            return "当前没有可用的任务"
        
        task_list_str = "\n".join([f"- {task}" for task in available_tasks])
        return f"**可用任务列表：**\n\n{task_list_str}\n\n共 {len(available_tasks)} 个任务"

    def _get_scheduled_task_list(self):
        schedule_tasks = self._scheduler_executor.get_scheduled_tasks_list()
        if not schedule_tasks:
            return "当前没有定时任务"
        task_list_str = "\n".join([f"任务 {name} 下次执行时间: {time}" for name, time in schedule_tasks])
        return f"**定时任务列表：**\n\n{task_list_str}\n\n共 {len(schedule_tasks)} 个任务"

    def _get_command_list_message(self):
        """获取命令列表消息"""
        commands = [
            ("内容解释(左边为要完成的功能)","右边为操作示例"),
            ("查看所有可用任务","@机器人 查看所有任务 / 哪些任务 / show me all tasks"),
            ("查看所有定时任务","@机器人 查看所有定时任务 / 定时任务 / show me all scheduled tasks"),
            ("查看所有可用命令","@机器人 查看所有命令 / 哪些命令 / show me all command"),
            ("执行指定任务","@机器人 执行[任务名] / 运行[任务名] / 开始[任务名]"),
            ("执行自定义SQL查询","@机器人 运行一个sql: {sql}"),
            ("发送指定卡片","@机器人 发送该卡片：{card_name}")
        ]
        command_list_str = "\n".join([f"- **{cmd}**: {desc}" for cmd, desc in commands])
        return f"**可用命令列表：**\n\n{command_list_str}"

    def _handle_task_execution(self, incoming_message, expression):
        """处理任务执行"""
        task_name = self._message_executor.parse_message(expression)
        if not task_name:
            return "未能在消息中识别到任务名称，请确认任务名称是否正确。"
        
        self.reply_markdown_card(
            f"**任务「{task_name}」已开始运行...**\n\n请稍候，任务执行完成后将自动发送结果。",
            incoming_message,
            title="任务执行中",
            logo="@lALPDfJ6V_FPDmvNAfTNAfQ"
        )
        
        success, result, task_instance = self._message_executor.execute_task_by_name(task_name, input_str=expression)
        
        if not success:
            return f"**任务执行失败**\n\n{result}"
        
        send_success, send_msg = self._task_result_sender.send_task_result(task_instance)
        
        if send_success:
            return f"**任务执行成功**\n\n{task_instance.msg}"
        else:
            return f"**任务执行成功，但结果发送失败**\n\n{send_msg}"

    def process(self, callback_message: CallbackMessage):
        """处理任务执行消息"""
        incoming_message = dingtalk_stream.ChatbotMessage.from_dict(callback_message.data)
        expression = incoming_message.text.content.strip()
        # print(callback_message)
        scheduled_list_keywords = ["定时任务", "定时", "scheduled"]
        task_list_keywords = ["所有任务", "哪些任务", "all tasks"]
        command_keywords = ["all command", "所有命令", "命令"]
        task_exec_keywords = ["执行", "运行", "开始", "跑一个", "sql", "execute task"]

        for keyword in scheduled_list_keywords:
            if keyword in expression:
                reply_msg = self._get_scheduled_task_list()
                self.reply_markdown_card(reply_msg, incoming_message, title="定时任务列表", logo="@lALPDfJ6V_FPDmvNAfTNAfQ")
                return AckMessage.STATUS_OK, 'OK'

        for keyword in task_list_keywords:
            if keyword in expression:
                reply_msg = self._get_available_task_list_message()
                self.reply_markdown_card(reply_msg, incoming_message, title="任务列表", logo="@lALPDfJ6V_FPDmvNAfTNAfQ")
                return AckMessage.STATUS_OK, 'OK'

        for keyword in command_keywords:
            if keyword in expression:
                reply_msg = self._get_command_list_message()
                self.reply_markdown_card(reply_msg, incoming_message, title="命令列表", logo="@lALPDfJ6V_FPDmvNAfTNAfQ")
                return AckMessage.STATUS_OK, 'OK'

        for keyword in task_exec_keywords:
            if keyword in expression:
                result_msg = self._handle_task_execution(incoming_message, expression)
                return AckMessage.STATUS_OK, 'OK'

        return AckMessage.STATUS_OK, 'OK'



if __name__ == '__main__':
    conf = Config(file_path='../config.yaml')
    d = DingTalkMessageHandler(logger, conf)
    d._task_result_sender.send_file('../config.yaml', 'chatdbf714606bde86a565d7bb01767fdd75')
