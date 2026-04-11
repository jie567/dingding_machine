import datetime
import os

from smb.SMBConnection import SMBConnection


def send_nas_file(excel, SHARE_NAME = "市场营销中心数据室", NAS_FILE_PATH = r"02-科室自用\监控报表脚本\报表202510-12"):
    """
    将Excel文件发送到NAS共享文件夹

    Args:
        excel: 本地Excel文件路径
    """
    # NAS 服务器配置 (SMB协议)
    NAS_HOST = "10.4.0.13"
    NAS_USER = "jieyu.huang"
    NAS_PASS = "%>6pMcC-k@.9"

    try:
        # 获取文件名
        filename = os.path.basename(excel)
        conn = SMBConnection(NAS_USER, NAS_PASS, "", "", use_ntlm_v2=True)
        if not conn.connect(NAS_HOST, 139):
            print(f"[{datetime.datetime.now()}] 连接NAS服务器失败")
            return False

        print(f"[{datetime.datetime.now()}] 成功连接到NAS服务器")

        # 上传文件
        with open(excel, 'rb') as file_obj:
            remote_path = NAS_FILE_PATH + "\\" + filename
            conn.storeFile(SHARE_NAME, remote_path, file_obj)

        print(f"[{datetime.datetime.now()}] 文件上传成功: {filename}")
        print(f"[{datetime.datetime.now()}] 保存路径: {SHARE_NAME}\{remote_path}")
        conn.close()

        return True

    except Exception as e:
        print(f"[{datetime.datetime.now()}] 操作失败: {str(e)}")
        return False


if __name__ == "__main__":
    excel_file1 = r"../resources/excels/预售监控-20251209.xlsx"
    # excel_file2 = r"E:\桌面\2025工作资料\daily_job\resources\excels\历史评价-20251208.xlsx"
    send_nas_file(excel_file1)
    # send_nas_file(excel_file2)