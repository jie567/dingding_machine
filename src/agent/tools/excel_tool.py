"""
Excel 报表生成工具

提供 Excel 报表生成功能，支持数据导出和模板填充。
复用现有的 excel_writer.py 功能。
"""

from typing import Any, Dict, List, Optional
import json
import os
from datetime import datetime

import pandas as pd

from .base import BaseTool, ToolResult
from ...util.excel_writer import excel_copy, batch_excel_writer


class ExcelGeneratorTool(BaseTool):
    """
    Excel 报表生成工具
    
    将数据生成 Excel 报表，支持：
    - 从 JSON 数据生成 Excel
    - 使用模板填充数据
    - 多 Sheet 支持
    - 自动命名和保存
    
    使用示例：
        tool = ExcelGeneratorTool()
        result = await tool.run(
            data_json=data_json,
            template_path="resources/excel_template/template.xlsx",
            sheet_name="报表"
        )
    """
    
    @property
    def name(self) -> str:
        """工具名称"""
        return "excel_generator"
    
    @property
    def description(self) -> str:
        """工具描述"""
        return (
            "生成 Excel 报表。"
            "支持从 JSON 数据生成，或使用模板填充。"
            "用于数据导出和报表生成。"
        )
    
    @property
    def parameters(self) -> Dict[str, Any]:
        """参数定义"""
        return {
            "data_json": {
                "type": "string",
                "description": "JSON 格式的数据",
                "required": True
            },
            "template_path": {
                "type": "string",
                "description": "Excel 模板路径（可选）",
                "required": False
            },
            "sheet_name": {
                "type": "string",
                "description": "工作表名称（默认 'Sheet1'）",
                "required": False
            },
            "output_path": {
                "type": "string",
                "description": "输出文件路径（可选，自动生成）",
                "required": False
            }
        }
    
    async def execute(
        self,
        data_json: str,
        template_path: Optional[str] = None,
        sheet_name: str = "Sheet1",
        output_path: Optional[str] = None,
        **kwargs
    ) -> ToolResult:
        """
        生成 Excel 报表
        
        Args:
            data_json: JSON 格式的数据
            template_path: Excel 模板路径
            sheet_name: 工作表名称
            output_path: 输出文件路径
            **kwargs: 额外参数
            
        Returns:
            ToolResult: 生成结果
        """
        try:
            # 解析 JSON 数据
            data = json.loads(data_json)
            df = pd.DataFrame(data)
            
            print(f"📊 生成 Excel 报表 [行数: {len(df)}]")
            
            # 确定输出路径
            if not output_path:
                # 自动生成文件名
                current_date = datetime.now().strftime('%Y%m%d_%H%M%S')
                output_path = f"resources/excels/report_{current_date}.xlsx"
            
            # 确保输出目录存在
            output_dir = os.path.dirname(output_path)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir, exist_ok=True)
            
            # 使用模板或新建文件
            if template_path and os.path.exists(template_path):
                # 使用模板
                print(f"📄 使用模板: {template_path}")
                excel_path = excel_copy(template_path, datetime.now().strftime('%Y%m%d'))
            else:
                # 新建文件
                excel_path = output_path
            
            # 写入数据
            batch_excel_writer(excel_path, [(df, sheet_name)])
            
            # 获取文件信息
            file_size = os.path.getsize(excel_path)
            file_size_kb = file_size / 1024
            
            result_data = {
                "file_path": excel_path,
                "file_size_kb": round(file_size_kb, 2),
                "row_count": len(df),
                "column_count": len(df.columns),
                "sheet_name": sheet_name
            }
            
            message = (
                f"Excel 报表生成成功！"
                f"文件: {excel_path}, "
                f"大小: {file_size_kb:.1f} KB, "
                f"数据: {len(df)} 行 × {len(df.columns)} 列"
            )
            
            return ToolResult(
                success=True,
                data=result_data,
                message=message
            )
            
        except json.JSONDecodeError as e:
            return ToolResult(
                success=False,
                error=f"JSON 解析失败: {str(e)}",
                message="请提供有效的 JSON 格式数据"
            )
        except Exception as e:
            return ToolResult(
                success=False,
                error=str(e),
                message=f"Excel 生成失败: {str(e)}"
            )


class ExcelMultiSheetTool(BaseTool):
    """
    多 Sheet Excel 生成工具
    
    生成包含多个工作表的 Excel 文件，适用于复杂报表场景。
    """
    
    @property
    def name(self) -> str:
        """工具名称"""
        return "excel_multi_sheet"
    
    @property
    def description(self) -> str:
        """工具描述"""
        return (
            "生成包含多个工作表的 Excel 报表。"
            "适用于需要多个相关报表的场景。"
        )
    
    @property
    def parameters(self) -> Dict[str, Any]:
        """参数定义"""
        return {
            "sheets_data": {
                "type": "list",
                "description": "工作表数据列表，格式: [{\"sheet_name\": \"Sheet1\", \"data_json\": \"[...]\"}]",
                "required": True
            },
            "output_path": {
                "type": "string",
                "description": "输出文件路径（可选）",
                "required": False
            }
        }
    
    async def execute(
        self,
        sheets_data: List[Dict[str, str]],
        output_path: Optional[str] = None,
        **kwargs
    ) -> ToolResult:
        """
        生成多 Sheet Excel
        
        Args:
            sheets_data: 工作表数据列表
            output_path: 输出文件路径
            **kwargs: 额外参数
            
        Returns:
            ToolResult: 生成结果
        """
        try:
            # 确定输出路径
            if not output_path:
                current_date = datetime.now().strftime('%Y%m%d_%H%M%S')
                output_path = f"resources/excels/multi_report_{current_date}.xlsx"
            
            # 确保输出目录存在
            output_dir = os.path.dirname(output_path)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir, exist_ok=True)
            
            # 解析每个工作表的数据
            sheet_tuples = []
            total_rows = 0
            
            for sheet_info in sheets_data:
                sheet_name = sheet_info.get("sheet_name", "Sheet")
                data_json = sheet_info.get("data_json", "[]")
                
                # 解析数据
                data = json.loads(data_json)
                df = pd.DataFrame(data)
                
                sheet_tuples.append((df, sheet_name))
                total_rows += len(df)
                
                print(f"  📑 Sheet '{sheet_name}': {len(df)} 行")
            
            # 生成 Excel
            batch_excel_writer(output_path, sheet_tuples)
            
            # 获取文件信息
            file_size = os.path.getsize(output_path)
            file_size_kb = file_size / 1024
            
            result_data = {
                "file_path": output_path,
                "file_size_kb": round(file_size_kb, 2),
                "sheet_count": len(sheets_data),
                "total_rows": total_rows
            }
            
            message = (
                f"多 Sheet Excel 生成成功！"
                f"文件: {output_path}, "
                f"工作表: {len(sheets_data)} 个, "
                f"总行数: {total_rows}"
            )
            
            return ToolResult(
                success=True,
                data=result_data,
                message=message
            )
            
        except Exception as e:
            return ToolResult(
                success=False,
                error=str(e),
                message=f"多 Sheet Excel 生成失败: {str(e)}"
            )
