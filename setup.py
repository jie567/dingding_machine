from setuptools import setup, find_packages
from pathlib import Path
import os

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text(encoding='utf-8') if (this_directory / "README.md").exists() else ""

def parse_requirements(filename):
    """解析requirements.txt文件"""
    requirements = []
    req_file = this_directory / filename
    if req_file.exists():
        with open(req_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and not line.startswith('-'):
                    requirements.append(line)
    return requirements

setup(
    name='dingding-machine',
    version='2.0.0',
    author='Jie Yu',
    author_email='your.email@example.com',
    description='钉钉机器人任务执行系统',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='http://36.137.10.200:3000/jieyu.huang/dingding_machine/',
    packages=['src'] + ['src.' + pkg for pkg in find_packages(where='src')],
    package_dir={'': '.'},
    
    # 包含资源文件
    package_data={
        '': ['*.yaml', '*.yml', '*.xlsx', '*.sql', '*.md'],
    },
    include_package_data=True,
    
    # 依赖项
    install_requires=parse_requirements('requirements.txt'),
    
    # Python版本要求
    python_requires='>=3.10',
    
    # 命令行入口点
    entry_points={
        'console_scripts': [
            'dingding-machine=src.cli:main',
        ],
    },
    
    # 分类信息
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ],
    
    # 关键词
    keywords='dingtalk bot automation scheduler',
    
    # 项目URLs
    project_urls={
        'Bug Reports': 'http://36.137.10.200:3000/jieyu.huang/dingding_machine/issues',
        'Source': 'http://36.137.10.200:3000/jieyu.huangingding_machine',
    },
)
