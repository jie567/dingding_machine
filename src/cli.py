from logging.handlers import TimedRotatingFileHandler
import logging
from pathlib import Path
import dingtalk_stream
import src
from src.config import Config
from src.handler.dingtalk_message_handler import DingTalkMessageHandler


def setup_logging():
    """配置日志系统"""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    logs_dir = Path("/var/log/dingding-machine")
    logs_dir.mkdir(parents=True, exist_ok=True)
    
    # 配置日志处理器
    handler = TimedRotatingFileHandler(
        filename=logs_dir / "machine.log",
        when="D",
        interval=1,
        backupCount=7,
        encoding="utf-8"
    )
    handler.setFormatter(
        logging.Formatter('%(asctime)s %(name)-8s %(levelname)-8s %(message)s [%(filename)s:%(lineno)d]')
    )
    logger.addHandler(handler)
    
    # 添加控制台输出
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(
        logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    )
    logger.addHandler(console_handler)
    
    return logger


def main():
    """主函数入口"""
    # 设置日志
    logger = setup_logging()
    
    logger.info("钉钉机器人服务启动中...")
    
    # 获取配置文件路径
    # 获取项目根目录（支持开发环境和安装后环境）
    try:
        # 尝试从包安装位置获取
        project_root = Path(src.__file__).parent.parent
    except:
        # 开发环境
        project_root = Path(__file__).parent

    config_path = project_root / "src" / "config.yaml"
    if not config_path.exists():
        logger.error(f"配置文件不存在: {config_path}")
        raise FileNotFoundError(f"配置文件不存在: {config_path}")
    
    try:
        # 加载配置
        conf = Config(file_path=str(config_path))
        logger.info("配置文件加载成功")
        
        # 创建钉钉客户端
        credential = dingtalk_stream.Credential(conf.user.client_id, conf.user.client_secret)
        client = dingtalk_stream.DingTalkStreamClient(credential)
        
        # 注册消息处理器
        client.register_callback_handler(
            dingtalk_stream.chatbot.ChatbotMessage.TOPIC, 
            DingTalkMessageHandler(logger, conf)
        )
        
        logger.info("钉钉机器人服务启动成功，开始监听消息...")
        client.start_forever()
        
    except Exception as e:
        logger.error(f"服务启动失败: {e}", exc_info=True)
        raise


if __name__ == '__main__':
    main()
