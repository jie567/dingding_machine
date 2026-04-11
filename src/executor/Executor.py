from abc import ABC
import logging

from src.util.oracle_connect import OracleDataConn
from src.executor.task_registry import TaskRegistry

logger = logging.getLogger(__name__)


class Task_Executor(ABC):

    def __init__(self):
        self._task_registry = TaskRegistry()
        self.running = False

    @property
    def tasks_list(self):
        """从注册中心获取任务列表"""
        return self._task_registry.get_all_tasks_instance()
    def get_available_tasks(self):
        """获取所有可用的任务名称"""
        return self._task_registry.get_all_task_names()

    def get_task_by_name(self, task_name):
        """根据任务名称获取任务实例"""
        return self._task_registry.get_task_by_name(task_name)

    def execute_task_by_name(self, task, **kwargs):
        """执行单个任务"""
        pass
