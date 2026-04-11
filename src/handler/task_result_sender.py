import json
import os
import time
import uuid
import logging
import requests

from src.config import Config

from typing import Callable, Optional

logger = logging.getLogger(__name__)


class TaskResultSender:
    """
    任务结果发送器
    负责发送任务执行结果到钉钉群聊（文件、卡片等）
    """

    def __init__(self, conf: Config, message_sender_callback: Optional[Callable] = None):
        self.conf = conf
        self.client_id = conf.user.client_id
        self.client_secret = conf.user.client_secret
        self.ROBOT_CODE = conf.user.ROBOT_CODE
        
        self._access_token = None
        self._token_expire_time = 60*60*24
        
        self._message_sender_callback = message_sender_callback
        
        self._get_access_token()

    def _get_access_token(self):
        """获取钉钉接口凭证（缓存 + 过期刷新）"""
        current_time = time.time()
        
        if self._access_token and current_time < self._token_expire_time:
            return self._access_token
        
        url = f"https://oapi.dingtalk.com/gettoken?appkey={self.client_id}&appsecret={self.client_secret}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            result = response.json()
            self._access_token = result.get("access_token")
            expires_in = result.get("expires_in", 7200)
            self._token_expire_time = current_time + expires_in - 300
            logger.info("access_token 获取成功")
            return self._access_token
        except requests.RequestException as err:
            logger.error(f"获取access_token失败: {err}")
            return None

    def _get_open_space_id(self, chat_id: str):
        """根据 chat_id 获取 openSpaceId"""
        return self.conf.get_open_space_id(chat_id)

    def _get_media_id(self, file_path: str):
        """获取文件 media_id"""
        access_token = self._get_access_token()
        if not access_token:
            logger.error("无法获取access_token，上传文件失败")
            return ""
        
        url = f"https://oapi.dingtalk.com/media/upload?access_token={access_token}&type=file"
        try:
            with open(file_path, "rb") as file:
                files = {"media": file}
                response = requests.post(url, files=files)
                response.raise_for_status()
                data = response.json()
                if data.get("errcode", 0) != 0:
                    logger.error(f"获取media_id失败，错误信息：{data.get('errmsg')}")
                    return ""
                return data["media_id"]
        except requests.RequestException as err:
            logger.error(f"上传文件失败，错误信息：{err}")
            return ""
        except FileNotFoundError:
            logger.error(f"文件不存在：{file_path}")
            return ""

    def send_file(self, file_path: str, chat_id: str):
        """发送文件到钉钉群聊"""
        if not os.path.exists(file_path):
            logger.error(f"文件不存在：{file_path}")
            return False, f"文件不存在：{file_path}"
        
        file_size = os.path.getsize(file_path)
        if file_size > 20 * 1024 * 1024:
            logger.warning(f"文件超过20MB，无法发送：{file_path}")
            return False, f"文件超过20MB，无法发送"
        
        access_token = self._get_access_token()
        if not access_token:
            return False, "无法获取access_token"
        
        media_id = self._get_media_id(file_path)
        if not media_id:
            return False, "获取media_id失败"
        
        url = f"https://oapi.dingtalk.com/chat/send?access_token={access_token}"
        payload = {
            "chatid": chat_id,
            "msg": {
                "msgtype": "file",
                "file": {"media_id": media_id}
            }
        }
        
        headers = {"Content-Type": "application/json"}
        try:
            response = requests.post(url, data=json.dumps(payload), headers=headers)
            response.raise_for_status()
            data = response.json()
            if data.get("errcode", 0) != 0:
                logger.error(f"发送文件出错，错误代码：{data.get('errcode')}，错误信息：{data.get('errmsg')}")
                return False, f"发送文件失败：{data.get('errmsg')}"
            logger.info(f"文件发送成功：{file_path}")
            return True, "文件发送成功"
        except requests.RequestException as err:
            logger.error(f"发送文件请求出错：{err}")
            return False, f"发送文件请求出错：{err}"

    def send_card(self, card_data: dict, chat_id: str, card_template_id: str = "0f1f372f-fe16-44d1-9c56-be6af7fa3576.schema"):
        """发送卡片消息到钉钉群聊"""
        access_token = self._get_access_token()
        if not access_token:
            return False, "无法获取access_token"
        
        openSpaceId = self._get_open_space_id(chat_id)
        if not openSpaceId:
            logger.error(f"未找到 chat_id={chat_id} 对应的 openSpaceId")
            return False, "未找到对应的openSpaceId"
        
        headers = {
            "Content-Type": "application/json",
            "Accept": "*/*",
            "x-acs-dingtalk-access-token": access_token,
        }

        def convert_json_values_to_string(obj):
            result = {}
            for key, value in obj.items():
                if isinstance(value, str):
                    result[key] = value
                else:
                    result[key] = json.dumps(value, ensure_ascii=False)
            return result

        body = {
            "cardTemplateId": card_template_id,
            "outTrackId": str(uuid.uuid4()),
            "cardData": {"cardParamMap": convert_json_values_to_string(card_data)},
            "openSpaceId": f"dtv1.card//IM_GROUP.{openSpaceId}",
            "imGroupOpenSpaceModel": {
                "supportForward": True,
                "lastMessageI18n": {"ZH_CN": "您收到一条任务执行结果卡片"},
            },
            "imGroupOpenDeliverModel": {"robotCode": self.ROBOT_CODE},
        }

        url = "https://api.dingtalk.com/v1.0/card/instances/createAndDeliver"
        try:
            response = requests.post(url, headers=headers, json=body)
            response.raise_for_status()
            result = response.json()
            if result.get("success"):
                logger.info("卡片发送成功")
                return True, "卡片发送成功"
            else:
                logger.error(f"卡片发送失败: {result}")
                return False, f"卡片发送失败: {result}"
        except requests.RequestException as e:
            logger.error(f"发送卡片请求失败: {e}")
            return False, f"发送卡片请求失败: {e}"

    def send_task(self, task):
        """
        根据任务类型发送执行结果
        :param task: Task 实例
        :return: (success, message)
        """
        task_name = task.task_name
        task_type = task.task_type
        chat_id = task.chat_id
        
        if not task.task_status:
            error_msg = f"任务「{task_name}」执行失败\n\n{task.msg}"
            logger.error(error_msg)
            if self._message_sender_callback:
                self._message_sender_callback(
                    chat_id=chat_id,
                    title="任务执行失败",
                    content=f"**任务「{task_name}」执行失败**\n\n{task.msg}"
                )
            return False, error_msg
        
        results = []
        
        if task_type == 'msg':
            logger.info(f"任务「{task_name}」执行成功：{task.msg}")
            if self._message_sender_callback:
                self._message_sender_callback(
                    chat_id=chat_id,
                    title="任务执行成功",
                    content=f"**任务「{task_name}」执行成功**\n\n{task.msg}"
                )
            return True, task.msg
        
        elif task_type == 'file':
            if self._message_sender_callback:
                self._message_sender_callback(
                    chat_id=chat_id,
                    title="任务执行成功",
                    content=f"**任务「{task_name}」执行成功**\n\n{task.msg}\n\n正在发送文件..."
                )
            
            results.append(f"任务「{task_name}」执行成功")
            results.append(task.msg)
            
            if not task.file_list:
                logger.warning(f"任务「{task_name}」没有生成文件")
                return True, "\n".join(results)
            
            for file_path in task.file_list:
                success, msg = self.send_file(file_path, chat_id)
                if success:
                    results.append(f"文件 {os.path.basename(file_path)} 已发送")
                else:
                    results.append(f"文件 {os.path.basename(file_path)} 发送失败：{msg}")
            
            logger.info(f"任务「{task_name}」结果发送完成")
            return True, "\n".join(results)
        
        elif task_type == 'card':
            if self._message_sender_callback:
                self._message_sender_callback(
                    chat_id=chat_id,
                    title="任务执行成功",
                    content=f"**任务「{task_name}」执行成功**\n\n{task.msg}\n\n正在发送卡片..."
                )
            
            card_data = task.msg
            if isinstance(card_data, dict):
                success, msg = self.send_card(card_data, chat_id)
            else:
                card_data_dict = {"markdown": str(card_data)}
                success, msg = self.send_card(card_data_dict, chat_id)
            
            if success:
                logger.info(f"任务「{task_name}」卡片发送成功")
            else:
                logger.error(f"任务「{task_name}」卡片发送失败：{msg}")
            return success, msg
        
        else:
            logger.warning(f"未知任务类型：{task_type}")
            return False, f"未知任务类型：{task_type}"

    def send_task_result(self, task):
        """
        根据任务类型发送完执行结果，重置任务状态
        """
        exec_status, msg = self.send_task(task)
        task.reset_task_status()
        return exec_status, msg
