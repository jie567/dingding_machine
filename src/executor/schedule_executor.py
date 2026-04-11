from src.executor.Executor import Task_Executor
from src.handler.task_result_sender import TaskResultSender
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
import logging
from datetime import datetime

from util.oracle_connect import OracleDataConn

logger = logging.getLogger(__name__)

class ScheduleExecutor(Task_Executor):
    def __init__(self, conf=None, task_result_sender: TaskResultSender = None):
        super().__init__()
        self._task_result_sender = task_result_sender
        self._conf = conf
        self.conn = OracleDataConn()

        jobstores = {
            'default': MemoryJobStore()
        }
        executors = {
            'default': ThreadPoolExecutor(5)
        }
        job_defaults = {
            'coalesce': True,
            'max_instances': 2
        }
        
        self.scheduler = BackgroundScheduler(
            jobstores=jobstores,
            executors=executors,
            job_defaults=job_defaults,
            timezone='Asia/Shanghai'
        )
        
        self.scheduler.add_listener(self._job_executed, EVENT_JOB_EXECUTED)
        self.scheduler.add_listener(self._job_error, EVENT_JOB_ERROR)
        self.scheduled_jobs = []

    def _job_executed(self, event):
        """任务执行成功事件处理"""
        if event.job_id:
            logger.info(f"定时任务 {event.job_id} 执行成功")

    def _job_error(self, event):
        """任务执行失败事件处理"""
        if event.job_id:
            logger.error(f"定时任务 {event.job_id} 执行失败: {event.exception}")

    def schedule_task(self, task):
        """使用APScheduler安排定时任务"""
        if not hasattr(task, 'ex_time') or not task.ex_time:
            logger.info(f"任务 {task.task_name} 为非定时任务，跳过调度")
            return

        try:
            # 验证cron表达式格式 这里要分析支持的是五位的 还是六位的
            # c = CronTrigger.from_crontab(task.ex_time)
            # 添加定时任务
            job = self.scheduler.add_job(
                self.task_execute,
                CronTrigger.from_crontab(task.ex_time),
                args=[task],
                id=task.task_name,
                name=task.task_name,
                replace_existing=True,
                misfire_grace_time =300  # 允许5分钟的延迟执行
            )
            
            self.scheduled_jobs.append(job)
            logger.info(f"安排任务 {task.task_name} 定时执行: {task.ex_time}")
            
        except ValueError as e:
            logger.error(f"任务 {task.task_name} 的cron表达式格式错误: {task.ex_time}, 错误: {e}")
        except Exception as e:
            logger.error(f"安排任务 {task.task_name} 失败: {e}")

    def task_execute(self, task):
        """执行单个任务并发送结果"""
        logger.info(f"开始执行定时任务: {task.task_name}")
        task.execute(self.conn)
        
        if self._task_result_sender:
            success, msg = self._task_result_sender.send_task_result(task)
            if success:
                logger.info(f"任务 {task.task_name} 结果发送成功")
            else:
                logger.error(f"任务 {task.task_name} 结果发送失败: {msg}")
        else:
            logger.warning(f"未配置 TaskResultSender，无法发送任务结果")

    def start(self):
        """启动定时任务调度器"""
        if not self.tasks_list:
            logger.warning("没有配置任何任务")
            return

        # 筛选定时任务并安排调度
        scheduled_tasks = []
        for task in self.tasks_list:
            if hasattr(task, 'ex_time') and task.ex_time:
                self.schedule_task(task)
                scheduled_tasks.append(task.task_name)
            else:
                logger.info(f"任务 {task.task_name} 为非定时任务")

        try:
            self.scheduler.start()
            logger.info(f"定时任务调度器已启动，共安排 {len(scheduled_tasks)} 个定时任务")
            logger.info(f"定时任务列表: {scheduled_tasks}")

            # 打印所有已安排的任务信息
            jobs = self.scheduler.get_jobs()
            for job in jobs:
                next_run_time = job.next_run_time.strftime('%Y-%m-%d %H:%M:%S') if job.next_run_time else "未知"
                logger.info(f"定时任务 {job.id} 下次执行时间: {next_run_time}")
                print(f"任务 {job.id} 下次执行时间: {next_run_time}")

        except Exception as e:
            logger.error(f"启动定时任务调度器失败: {e}")

    def stop(self):
        """停止定时任务调度器"""
        try:
            if self.scheduler.running:
                self.scheduler.shutdown(wait=True)  # 等待所有任务完成
                logger.info("定时任务调度器已停止")
            else:
                logger.info("定时任务调度器未运行")
        except Exception as e:
            logger.error(f"停止定时任务调度器失败: {e}")
    
    def get_task_status(self):
        """获取所有任务状态"""
        status = {}
        jobs = self.scheduler.get_jobs()
        
        for job in jobs:
            status[job.id] = {
                'name': job.name,
                'next_run_time': job.next_run_time.strftime('%Y-%m-%d %H:%M:%S') if job.next_run_time else None,
                'trigger': str(job.trigger),
                'paused': not job.next_run_time  # 如果next_run_time为None，说明任务已暂停
            }
        
        return status

    def get_scheduled_tasks_list(self):
        """获取所有定时任务"""
        result = []
        jobs = self.scheduler.get_jobs()
        for job in jobs:
            next_run_time = job.next_run_time.strftime('%Y-%m-%d %H:%M:%S') if job.next_run_time else "未知"
            result.append((f"{job.id}",f"{next_run_time}"))
        return result

    # def pause_task(self, task_name):
    #     """暂停指定任务"""
    #     try:
    #         self.scheduler.pause_job(task_name)
    #         logger.info(f"任务 {task_name} 已暂停")
    #     except Exception as e:
    #         logger.error(f"暂停任务 {task_name} 失败: {e}")

    # def resume_task(self, task_name):
    #     """恢复指定任务"""
    #     try:
    #         self.scheduler.resume_job(task_name)
    #         logger.info(f"任务 {task_name} 已恢复")
    #     except Exception as e:
    #         logger.error(f"恢复任务 {task_name} 失败: {e}")

    # def remove_task(self, task_name):
    #     """移除指定任务"""
    #     try:
    #         self.scheduler.remove_job(task_name)
    #         logger.info(f"任务 {task_name} 已移除")
    #     except Exception as e:
    #         logger.error(f"移除任务 {task_name} 失败: {e}"


if __name__ == '__main__':
    import time
    s = ScheduleExecutor()
    
    # 启动调度器
    s.start()
    # print(s.get_scheduled_tasks_list())
    try:
        # 保持程序运行
        while True:
            time.sleep(20)
    except KeyboardInterrupt:
        s.stop()