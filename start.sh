# 测试启动
nohup dingding-machine  > /dev/null 2>&1 &

echo "进程ID: $!"

sleep 2

ps aux | grep dingding-machine | grep -v grep


# 1. 创建服务文件
sudo vim /etc/systemd/system/dingding-machine.service


[Unit]
Description=DingDing Machine Bot Service
After=network.target

[Service]
Type=simple
User=jie
Group=jie
WorkingDirectory=/var/log/dingding-machine

# Conda环境路径
Environment="PATH=/mnt/miniconda3/envs/jie_personal_dev/bin"
Environment="LD_LIBRARY_PATH=/usr/lib/oracle/21/client64/lib:$LD_LIBRARY_PATH"

# 执行命令
ExecStart=/mnt/miniconda3/envs/jie_personal_dev/bin/dingding-machine

# 自动重启配置
Restart=always
RestartSec=3

# 日志配置 应用程序有自己的日志
# StandardOutput=append:/var/log/dingding-machine/logs/systemd.log
# StandardError=append:/var/log/dingding-machine/logs/systemd.log
# SyslogIdentifier=dingding-machine

# 资源限制
LimitNOFILE=65536
TimeoutStartSec=60
TimeoutStopSec=60

[Install]
WantedBy=multi-user.target



# 重载配置
sudo systemctl daemon-reload

# 启动服务
sudo systemctl start dingding-machine

# 设置开机自启
sudo systemctl enable dingding-machine

# 查看状态
sudo systemctl status dingding-machine

# 查看日志
sudo journalctl -u dingding-machine -f

# 停止服务
sudo systemctl stop dingding-machine

# 重启服务
sudo systemctl restart dingding-machine