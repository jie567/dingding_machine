import pandas as pd

from src.task.Task import task_config, Task
from src.util.excel_writer import excel_copy,write_to_excel_col_fastest
from src.util.oracle_connect import OracleDataConn


@task_config(
    name="sql",
    task_type="file",
    chat_id="chatdbf714606bde86a565d7bb01767fdd75",
    excel_path ='../../resources/excel_template/sql_template.xlsx'
)
class SqlTask(Task):
    def __init__(self):
        super().__init__('sql')

    def execute_task(self, conn, **kwargs):
        if 'sql_str' in kwargs.keys():
            sql_str = kwargs['sql_str']
            conn.execute(sql_str)
        else:
            raise Exception('sql_str is required')
        if conn.cursor.description is not None:
            data = conn.cursor.fetchall()
            task_conf = getattr(self.__class__, '_task_config', {})
            excel_path = task_conf.get('excel_path')
            col_names = [desc[0] for desc in conn.cursor.description]
            db_df = pd.DataFrame(data, columns=col_names)
            new_excel_path = excel_copy(excel_path)
            write_to_excel_col_fastest(new_excel_path, 'Sheet1', db_df, start_row=1, start_col=1, with_head=True)
            self.file_list.append(new_excel_path)
        else:
            self.task_type = 'msg'

if __name__ == '__main__':
    db_conn = OracleDataConn()
    s = SqlTask()
    s.execute(db_conn, sql_str='SELECT * FROM air1.TB_FOC_T1011D where rownum < 100')
    # s.execute(conn,sql_str='update dept set DEPT_ID = 103 where DEPT_ID = 301 ')

        
