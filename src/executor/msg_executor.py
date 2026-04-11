import re

from src.executor.Executor import Task_Executor
import logging
from util.oracle_pool import PooledConnection

logger = logging.getLogger(__name__)


class MessageExecutor(Task_Executor):
    def __init__(self):
        super().__init__()
        self.conn = PooledConnection()

    def parse_message(self, message):
        """
        从消息中解析出任务名称。
        只要消息中包含任务列表中的某个任务名（优先匹配最长名称），即返回该任务名。
        若未匹配到任何任务，返回 None。
        """
        available_tasks = self.get_available_tasks()
        if not available_tasks:
            logger.warning("可用任务列表为空")
            return None

        # 按任务名长度降序排序，优先匹配较长的名称，避免短名称误匹配（如“任务A”匹配到“任务”）
        sorted_tasks = sorted(available_tasks, key=len, reverse=True)

        for task_name in sorted_tasks:
            if task_name in message:
                logger.debug(f"从消息中匹配到任务: {task_name}")
                return task_name

        logger.debug(f"消息中未匹配到任何任务: {message}")
        return None

    def handle_message(self, message):
        """处理钉钉消息，返回执行结果"""
        # 解析消息获取任务名称
        task_name = self.parse_message(message)

        if not task_name:
            return False, "请指定要执行的任务名称，例如：执行CPA利润表", None

        # 获取可用任务列表
        available_tasks = self.get_available_tasks()

        # 检查任务是否存在
        if task_name not in available_tasks:
            error_msg = f"未找到任务 '{task_name}'。可用任务: {', '.join(available_tasks)}"
            return False, error_msg, None

        success, result, task_instance = self.execute_task_by_name(task_name, input_str=message)

        return success, result, task_instance

    def execute_task_by_name(self, task_name, **kwargs):
        """根据任务名称执行任务"""
        # 获取任务实例
        task_instance = self.get_task_by_name(task_name)

        if not task_instance:
            return False, f"未找到任务: {task_name}", None

        try:
            # 执行任务
            logger.info(f"开始执行消息触发的任务: {task_name}")
            if 'sql' in task_name:
                sql_str = re.split(r"[:：]", kwargs['input_str'])[1]
                task_instance.execute(self.conn, sql_str=sql_str)
            else:
                task_instance.execute(self.conn)

            if task_instance.task_status:
                return True, task_instance.msg, task_instance
            else:
                error_msg = f"任务 '{task_name}' 执行失败"
                if task_instance.error:
                    error_msg += f": {task_instance.error}"
                return False, error_msg, task_instance

        except Exception as e:
            error_msg = f"执行任务 '{task_name}' 时发生错误: {str(e)}"
            logger.error(error_msg)
            return False, error_msg, None


if __name__ == '__main__':
    m = MessageExecutor()
    result = m.get_available_tasks()
    print(result)
    task_list_str = "\n".join([f"- {task}" for task in result])
    print(f"**可用任务列表：**\n\n{task_list_str}\n\n共 {len(result)} 个任务")
    msg = '帮我执行一个任务：CAP布局'
    success, result, task_instance = m.handle_message(msg)
    print(task_instance.msg)
