import yaml
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class GroupConfig:
    """群聊消息"""
    chat_name: str
    openSpaceId: str
    chat_id: str



@dataclass
class UserConfig:
    """用户认证信息"""
    client_id: str
    client_secret: str
    ROBOT_CODE: str


class Config:
    """主配置类，解析整个 YAML 文件"""

    def __init__(self, file_path: Optional[str] = None, yaml_str: Optional[str] = None):
        """
        从文件路径或 YAML 字符串加载配置。
        必须提供 file_path 或 yaml_str 其中之一。
        """
        if file_path is None and yaml_str is None:
            raise ValueError("必须提供 file_path 或 yaml_str 参数")

        self._raw_data: Dict[str, Any] = {}
        if file_path:
            self._load_from_file(file_path)
        elif yaml_str:
            self._load_from_string(yaml_str)

        self.user: UserConfig = UserConfig(**self._raw_data.get('User', {}))
        self.groups_list: List[GroupConfig] = [
            GroupConfig(
                chat_name=group['chat_name'],
                openSpaceId=group['openSpaceId'],
                chat_id=group['chat_id']
            )
            for group in self._raw_data.get('chat_group', [])
        ]

        self._chatid_to_openspaceid = {
            group.chat_id: group.openSpaceId for group in self.groups_list
        }

    def get_open_space_id(self, chat_id: str) -> Optional[str]:
        """根据 chat_id 获取对应的 openSpaceId，若不存在返回 None"""
        return self._chatid_to_openspaceid.get(chat_id)

    def _load_from_file(self, file_path: str):
        """从文件加载 YAML"""
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"配置文件不存在: {file_path}")
        with open(path, 'r', encoding='utf-8') as f:
            self._raw_data = yaml.safe_load(f)

    def _load_from_string(self, yaml_str: str):
        """从字符串加载 YAML"""
        self._raw_data = yaml.safe_load(yaml_str)

    def to_dict(self) -> Dict[str, Any]:
        """返回原始字典格式的配置（方便调试或序列化）"""
        return self._raw_data

    def __repr__(self) -> str:
        return f"Config(user={self.user}, tasks={self.tasks})"


if __name__ == '__main__':
    c = Config(file_path='config.yaml')
    # print(c.user)
    print(c.get_open_space_id('chat846258591d4cd7ee0e3d340bdd074d43'))