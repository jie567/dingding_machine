import os
import traceback
from abc import ABC, abstractmethod
import datetime, logging

logger = logging.getLogger(__name__)


def task_config(name=None, task_type='file', ex_time=None, chat_id=None, **kwargs):
    """
    任务配置装饰器
    :param name: 任务名称
    :param task_type: 任务类型：file(有文件发送), msg(纯消息任务) or card(卡片任务)
    :param ex_time: 定时执行时间（cron表达式）
    :param chat_id: 钉钉群聊ID
    :param kwargs: 其他配置参数
    """

    def decorator(cls):
        cls._task_config = {
            'name': name or cls.__name__,
            'task_type': task_type,
            'ex_time': ex_time,
            'chat_id': chat_id,
            **kwargs
        }
        return cls

    return decorator


class Task(ABC):
    _registry = {}

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # 自动注册Task子类
        cls._registry[cls.__name__] = cls

    def __init__(self, task_name):
        self.task_name = task_name
        self.task_type = 'file'
        self.start_time = None
        self.end_time = None
        self.ex_time = None
        self.task_status = False
        self.msg = ''
        self.file_list = []
        self.chat_id = "chat846258591d4cd7ee0e3d340bdd074d43"
        self.error = None

        if hasattr(self.__class__, '_task_config'):
            config = self.__class__._task_config
            self.task_type = config.get('task_type', self.task_type)
            self.chat_id = config.get('chat_id', self.chat_id)
            self.ex_time = config.get('ex_time', self.ex_time)
            self.task_type = config.get('task_type', self.ex_time)

    @classmethod
    def get_task_class(cls, task_name):
        """根据任务名称获取Task类"""
        for task_cls in cls._registry.values():
            if hasattr(task_cls, '_task_config') and task_cls._task_config.get('name') == task_name:
                return task_cls
        return None

    @classmethod
    def get_all_tasks(cls):
        """获取所有已注册的Task类"""
        return list(cls._registry.values())

    @classmethod
    def get_task_names(cls):
        """获取所有已注册的任务名称"""
        task_names = []
        for task_cls in cls._registry.values():
            if hasattr(task_cls, '_task_config') and 'name' in task_cls._task_config:
                task_names.append(task_cls._task_config['name'])
        return task_names

    def execute(self, conn, **kwargs):
        assert conn is not None, 'conn should not None'
        self.start_time = datetime.datetime.now()
        try:
            self.execute_task(conn, **kwargs)
            self.succeed()
        except Exception as e:
            error_trace = traceback.format_exc()
            print(f"{datetime.datetime.now()}： {self.task_name}执行失败: {error_trace}")
            self.error = error_trace
            self.failed()

    @abstractmethod
    def execute_task(self, conn, **kwargs):
        # 不可以写 try catch 但是可以raise 具体的错误
        pass

    def succeed(self):
        self.task_status = True
        self.end_time = datetime.datetime.now()
        time_diff = self.end_time - self.start_time
        total_seconds = time_diff.total_seconds()
        minutes = int(total_seconds // 60)
        seconds = int(total_seconds % 60)
        if self.task_type != 'file':
            self.msg += f'\n{self.task_name} task has succeed and spent time:{minutes}m:{seconds}s'
        elif self.task_type == 'file' and all(os.path.getsize(f) < 20 * 1024 * 1024 for f in self.file_list):
            self.msg += f'\n{self.task_name} task has succeed and spent time:{minutes}m:{seconds}s; please receive files'
        else:
            self.msg += f'\n{self.task_name} has succeed,but some file size is more than 20MB, can not send it '
        logger.info(self.msg)

    def failed(self):
        self.task_status = False
        self.msg += f'\n{self.task_name} has failed,please check the execute details or retry this task. ' \
                    f'\nthe error of {self.task_name} : {self.error}'
        logger.error(self.msg)

    def reset_task_status(self):
        self.start_time = None
        self.end_time = None
        self.ex_time = None
        self.task_status = False
        self.file_list = []
        self.msg = ''
        self.error = None

    def get_ex_time(self):
        return self.ex_time
