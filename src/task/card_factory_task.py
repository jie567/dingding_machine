from src.task.Task import task_config, Task


@task_config(
    name="卡片任务",
    ex_time = None,
    task_type="card",
    chat_id="chatdbf714606bde86a565d7bb01767fdd75"
)
class CardFactoryTask(Task):
    sub_card_task = []
    def __init__(self):
        super().__init__('卡片任务')
        # super().__init_subclass__(**kwargs)

    def execute_task(self, conn, **kwargs):
        pass