import importlib
import os
import glob
from pathlib import Path
import logging

from src.task.Task import Task

logger = logging.getLogger(__name__)


class TaskRegistry:
    """
    任务注册中心 - 单例模式
    负责加载、存储和提供任务实例
    """
    _instance = None
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(TaskRegistry, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if TaskRegistry._initialized:
            return
        TaskRegistry._initialized = True
        self._tasks_list = []
        self._load_tasks_automatically()

    def _load_tasks_automatically(self):
        """自动加载所有Task子类"""
        try:
            task_dir = os.path.join(Path(__file__).parent.parent, 'task')
            task_files = glob.glob(os.path.join(task_dir, '*.py'))

            for task_file in task_files:
                if task_file.endswith('__init__.py') or task_file.endswith('Task.py'):
                    continue

                module_name = os.path.basename(task_file)[:-3]
                if module_name.startswith('_'):
                    continue

                try:
                    importlib.import_module(f'src.task.{module_name}')
                except Exception as e:
                    logger.warning(f"导入模块 src.task.{module_name} 失败: {e}")

            task_classes = Task.get_all_tasks()

            for task_class in task_classes:
                try:
                    task_config = getattr(task_class, '_task_config', {})
                    task_name = task_config.get('name', task_class.__name__)

                    task_instance = task_class()

                    if 'chat_id' in task_config:
                        task_instance.chat_id = task_config['chat_id']
                    if 'task_type' in task_config:
                        task_instance.task_type = task_config['task_type']
                    if 'ex_time' in task_config:
                        task_instance.ex_time = task_config['ex_time']

                    self._tasks_list.append(task_instance)
                    logger.info(f"成功加载任务: {task_name}")
                except Exception as e:
                    logger.error(f"创建任务实例 {task_class.__name__} 失败: {e}")

            logger.info(f"共加载 {len(self._tasks_list)} 个任务")
        except Exception as e:
            logger.error(f"自动加载任务失败: {e}")

    def get_all_tasks_instance(self):
        """获取所有任务实例列表"""
        return self._tasks_list

    def get_all_task_names(self):
        """获取所有可用的任务名称"""
        return [task.task_name for task in self._tasks_list]

    def get_task_by_name(self, task_name):
        """根据任务名称获取任务实例"""
        for task in self._tasks_list:
            if task.task_name == task_name:
                return task
        return None

    def reload_tasks(self):
        """重新加载所有任务"""
        self._tasks_list = []
        TaskRegistry._initialized = False
        TaskRegistry._instance = None
        self.__init__()
        logger.info("任务已重新加载")

    @classmethod
    def get_instance(cls):
        """获取单例实例"""
        if cls._instance is None:
            cls._instance = TaskRegistry()
        return cls._instance
