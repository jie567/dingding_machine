import json

import requests

class DingDingMachine:

    def __init__(self):
        self.client_id = "xxx"
        self.client_secret = "xxx"
        self.chat_id = "xxx"

    def set_chat_id(self, chat_id):
        self.chat_id = chat_id
    def get_access_token(self):
        """
        获取接口凭证
        """
        url = f"https://oapi.dingtalk.com/gettoken?appkey={self.client_id}&appsecret={self.client_secret}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json().get("access_token")
        except requests.RequestException as err:
            print(f"获取access_token失败，错误信息：{err}")
            return None
    def get_media_id(self, file_path: str):
        """
        获取文件media_id
        """
        self.access_token = self.get_access_token()  # 接口凭证，值不固定每次调用前获取
        url = f"https://oapi.dingtalk.com/media/upload?access_token={self.access_token}&type=file"
        try:
            with open(file_path, "rb") as file:
                files = {"media": file}
                response = requests.post(url, files=files)
                response.raise_for_status()
                data = response.json()
                if data["errcode"]:
                    print(f"获取media_id失败，错误信息：{data['errmsg']}")
                    return ""
                return data["media_id"]
        except requests.RequestException as err:
            print(f"上传文件失败，错误信息：{err}")
            return ""

    def send_file(self, file_path: str):
        """
        * 发送文件到钉钉
        """
        media_id = self.get_media_id(file_path)
        if not media_id:
            return

        url = f"https://oapi.dingtalk.com/chat/send?access_token={self.access_token}"
        payload = {
            "chatid": self.chat_id,
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
            if data["errcode"]:
                print(f"发送文件出错，错误代码：{data['errcode']}，错误信息：{data['errmsg']}")
            print(f"{file_path}文件发送成功")
        except requests.RequestException as err:
            print(f"发送文件请求出错，错误信息：{err}")

    def send_msg(self, message: str, *args):
        """
                发送文本消息到钉钉群聊
                :param message: 要发送的文本消息内容
                """
        access_token = self.get_access_token()
        if not access_token:
            print("无法获取access_token，消息发送失败")
            return

        url = f"https://oapi.dingtalk.com/chat/send?access_token={access_token}"
        payload = {
            "chatid": self.chat_id,
            "msg": {
                "msgtype": "text",
                "text": {"content": message}
            }
        }

        headers = {"Content-Type": "application/json"}
        try:
            response = requests.post(url, data=json.dumps(payload), headers=headers)
            response.raise_for_status()
            data = response.json()
            if data["errcode"]:
                print(f"发送消息出错，错误代码：{data['errcode']}，错误信息：{data['errmsg']}")
            else:
                print("消息发送成功")
        except requests.RequestException as err:
            print(f"发送消息请求出错，错误信息：{err}")




if __name__ == '__main__':
    dingdingMachine = DingDingMachine()
    access_token = dingdingMachine.get_access_token()
    chat_list_url=f'https://oapi.dingtalk.com/topapi/v2/group/list?access_token={access_token}'
    response = requests.get(chat_list_url)
    print(response)
