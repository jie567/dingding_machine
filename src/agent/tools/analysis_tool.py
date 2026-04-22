"""
数据分析工具

提供数据分析功能，包括统计摘要、趋势分析、对比分析等。
帮助 Agent 对查询结果进行深入分析。
"""

from typing import Any, Dict, List, Optional
import json

import pandas as pd
import numpy as np

from .base import BaseTool, ToolResult


class DataAnalysisTool(BaseTool):
    """
    数据分析工具
    
    对 DataFrame 数据进行各种分析，包括：
    - 统计摘要（describe）
    - 趋势分析
    - 分组统计
    - 对比分析
    - 相关性分析
    
    使用示例：
        tool = DataAnalysisTool()
        result = await tool.run(
            data_json=data_json,
            analysis_type="summary"
        )
    """
    
    @property
    def name(self) -> str:
        """工具名称"""
        return "data_analysis"
    
    @property
    def description(self) -> str:
        """工具描述"""
        return (
            "对数据进行统计分析。"
            "支持统计摘要、趋势分析、分组统计、对比分析等。"
            "输入数据为 JSON 格式。"
        )
    
    @property
    def parameters(self) -> Dict[str, Any]:
        """参数定义"""
        return {
            "data_json": {
                "type": "string",
                "description": "JSON 格式的数据（DataFrame 的 records 格式）",
                "required": True
            },
            "analysis_type": {
                "type": "string",
                "description": (
                    "分析类型: 'summary'(统计摘要), 'trend'(趋势分析), "
                    "'groupby'(分组统计), 'compare'(对比分析), "
                    "'correlation'(相关性分析)"
                ),
                "required": True
            },
            "column": {
                "type": "string",
                "description": "目标列名（某些分析类型需要）",
                "required": False
            },
            "group_by": {
                "type": "string",
                "description": "分组列名（groupby 分析需要）",
                "required": False
            },
            "compare_columns": {
                "type": "list",
                "description": "对比列名列表（compare 分析需要）",
                "required": False
            }
        }
    
    async def execute(
        self,
        data_json: str,
        analysis_type: str,
        column: Optional[str] = None,
        group_by: Optional[str] = None,
        compare_columns: Optional[List[str]] = None,
        **kwargs
    ) -> ToolResult:
        """
        执行数据分析
        
        Args:
            data_json: JSON 格式的数据
            analysis_type: 分析类型
            column: 目标列名
            group_by: 分组列名
            compare_columns: 对比列名列表
            **kwargs: 额外参数
            
        Returns:
            ToolResult: 分析结果
        """
        try:
            # 解析 JSON 数据
            data = json.loads(data_json)
            df = pd.DataFrame(data)
            
            print(f"📊 执行数据分析 [类型: {analysis_type}]")
            print(f"   数据行数: {len(df)}, 列数: {len(df.columns)}")
            
            # 根据分析类型执行不同的分析
            if analysis_type == "summary":
                return await self._analyze_summary(df)
            elif analysis_type == "trend":
                return await self._analyze_trend(df, column)
            elif analysis_type == "groupby":
                return await self._analyze_groupby(df, group_by, column)
            elif analysis_type == "compare":
                return await self._analyze_compare(df, compare_columns)
            elif analysis_type == "correlation":
                return await self._analyze_correlation(df)
            else:
                return ToolResult(
                    success=False,
                    error=f"不支持的分析类型: {analysis_type}",
                    message="支持的分析类型: summary, trend, groupby, compare, correlation"
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
                message=f"数据分析失败: {str(e)}"
            )
    
    async def _analyze_summary(self, df: pd.DataFrame) -> ToolResult:
        """
        统计摘要分析
        
        Args:
            df: 数据框
            
        Returns:
            ToolResult: 统计摘要结果
        """
        # 获取数值列的统计信息
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        
        if numeric_cols:
            summary = df[numeric_cols].describe().to_dict()
            
            # 添加额外统计信息
            extra_stats = {}
            for col in numeric_cols:
                extra_stats[col] = {
                    "sum": float(df[col].sum()),
                    "median": float(df[col].median()),
                    "null_count": int(df[col].isnull().sum()),
                    "null_rate": float(df[col].isnull().sum() / len(df))
                }
            
            result_data = {
                "summary": summary,
                "extra_stats": extra_stats,
                "total_rows": len(df),
                "total_columns": len(df.columns),
                "numeric_columns": numeric_cols
            }
            
            message = (
                f"数据统计摘要完成。"
                f"共 {len(df)} 行，{len(df.columns)} 列，"
                f"其中数值列 {len(numeric_cols)} 个"
            )
            
        else:
            result_data = {
                "total_rows": len(df),
                "total_columns": len(df.columns),
                "numeric_columns": []
            }
            message = "数据中没有数值列，无法生成统计摘要"
        
        return ToolResult(
            success=True,
            data=result_data,
            message=message
        )
    
    async def _analyze_trend(
        self, 
        df: pd.DataFrame, 
        column: Optional[str]
    ) -> ToolResult:
        """
        趋势分析
        
        Args:
            df: 数据框
            column: 目标列名
            
        Returns:
            ToolResult: 趋势分析结果
        """
        if not column:
            return ToolResult(
                success=False,
                error="缺少 column 参数",
                message="趋势分析需要指定目标列"
            )
        
        if column not in df.columns:
            return ToolResult(
                success=False,
                error=f"列 '{column}' 不存在",
                message=f"可用的列: {', '.join(df.columns.tolist())}"
            )
        
        # 计算趋势指标
        values = df[column].dropna()
        
        if len(values) < 2:
            return ToolResult(
                success=False,
                error="数据点不足",
                message="趋势分析需要至少 2 个数据点"
            )
        
        # 计算变化率
        first_value = values.iloc[0]
        last_value = values.iloc[-1]
        change_rate = ((last_value - first_value) / abs(first_value) * 100) if first_value != 0 else 0
        
        # 计算移动平均
        if len(values) >= 3:
            ma3 = values.rolling(window=3).mean().dropna().tolist()
        else:
            ma3 = values.tolist()
        
        result_data = {
            "column": column,
            "first_value": float(first_value),
            "last_value": float(last_value),
            "change_rate": float(change_rate),
            "min": float(values.min()),
            "max": float(values.max()),
            "mean": float(values.mean()),
            "moving_average": [float(v) for v in ma3[-5:]]  # 最近 5 个移动平均值
        }
        
        trend_direction = "上升" if change_rate > 0 else "下降" if change_rate < 0 else "平稳"
        
        message = (
            f"列 '{column}' 的趋势分析: "
            f"整体呈{trend_direction}趋势，"
            f"变化幅度 {abs(change_rate):.2f}%"
        )
        
        return ToolResult(
            success=True,
            data=result_data,
            message=message
        )
    
    async def _analyze_groupby(
        self, 
        df: pd.DataFrame, 
        group_by: Optional[str],
        column: Optional[str]
    ) -> ToolResult:
        """
        分组统计
        
        Args:
            df: 数据框
            group_by: 分组列名
            column: 聚合列名
            
        Returns:
            ToolResult: 分组统计结果
        """
        if not group_by:
            return ToolResult(
                success=False,
                error="缺少 group_by 参数",
                message="分组统计需要指定分组列"
            )
        
        if group_by not in df.columns:
            return ToolResult(
                success=False,
                error=f"分组列 '{group_by}' 不存在",
                message=f"可用的列: {', '.join(df.columns.tolist())}"
            )
        
        # 执行分组统计
        if column and column in df.columns:
            # 对指定列进行聚合
            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
            agg_cols = [column] if column in numeric_cols else numeric_cols
            
            grouped = df.groupby(group_by)[agg_cols].agg(['sum', 'mean', 'count'])
            
            result_data = {
                "group_by": group_by,
                "aggregated_columns": agg_cols,
                "groups": grouped.to_dict(),
                "group_count": len(grouped)
            }
            
            message = (
                f"按 '{group_by}' 分组统计完成，"
                f"共 {len(grouped)} 个分组"
            )
            
        else:
            # 只统计分组数量
            group_counts = df[group_by].value_counts().to_dict()
            
            result_data = {
                "group_by": group_by,
                "group_counts": group_counts,
                "group_count": len(group_counts)
            }
            
            message = (
                f"按 '{group_by}' 分组统计完成，"
                f"共 {len(group_counts)} 个分组"
            )
        
        return ToolResult(
            success=True,
            data=result_data,
            message=message
        )
    
    async def _analyze_compare(
        self, 
        df: pd.DataFrame, 
        compare_columns: Optional[List[str]]
    ) -> ToolResult:
        """
        对比分析
        
        Args:
            df: 数据框
            compare_columns: 对比列名列表
            
        Returns:
            ToolResult: 对比分析结果
        """
        if not compare_columns or len(compare_columns) < 2:
            return ToolResult(
                success=False,
                error="对比列不足",
                message="对比分析需要至少 2 个列"
            )
        
        # 检查列是否存在
        missing_cols = [col for col in compare_columns if col not in df.columns]
        if missing_cols:
            return ToolResult(
                success=False,
                error=f"列不存在: {', '.join(missing_cols)}",
                message=f"可用的列: {', '.join(df.columns.tolist())}"
            )
        
        # 获取数值列
        numeric_df = df[compare_columns].select_dtypes(include=[np.number])
        
        if numeric_df.empty:
            return ToolResult(
                success=False,
                error="没有数值列",
                message="对比分析需要数值类型的列"
            )
        
        # 计算对比指标
        comparison = {}
        for col in numeric_df.columns:
            comparison[col] = {
                "sum": float(numeric_df[col].sum()),
                "mean": float(numeric_df[col].mean()),
                "median": float(numeric_df[col].median()),
                "std": float(numeric_df[col].std()),
                "min": float(numeric_df[col].min()),
                "max": float(numeric_df[col].max())
            }
        
        result_data = {
            "compared_columns": compare_columns,
            "numeric_columns": numeric_df.columns.tolist(),
            "comparison": comparison
        }
        
        message = f"对比分析完成，对比了 {len(numeric_df.columns)} 个数值列"
        
        return ToolResult(
            success=True,
            data=result_data,
            message=message
        )
    
    async def _analyze_correlation(self, df: pd.DataFrame) -> ToolResult:
        """
        相关性分析
        
        Args:
            df: 数据框
            
        Returns:
            ToolResult: 相关性分析结果
        """
        # 获取数值列
        numeric_df = df.select_dtypes(include=[np.number])
        
        if len(numeric_df.columns) < 2:
            return ToolResult(
                success=False,
                error="数值列不足",
                message="相关性分析需要至少 2 个数值列"
            )
        
        # 计算相关系数矩阵
        corr_matrix = numeric_df.corr()
        
        # 找出强相关的列对
        strong_correlations = []
        for i in range(len(corr_matrix.columns)):
            for j in range(i + 1, len(corr_matrix.columns)):
                col1 = corr_matrix.columns[i]
                col2 = corr_matrix.columns[j]
                corr_value = corr_matrix.iloc[i, j]
                
                if abs(corr_value) > 0.5:  # 只记录中等以上相关
                    strong_correlations.append({
                        "column1": col1,
                        "column2": col2,
                        "correlation": float(corr_value),
                        "strength": (
                            "强正相关" if corr_value > 0.7 else
                            "中等正相关" if corr_value > 0.5 else
                            "强负相关" if corr_value < -0.7 else
                            "中等负相关"
                        )
                    })
        
        result_data = {
            "correlation_matrix": corr_matrix.to_dict(),
            "strong_correlations": strong_correlations,
            "numeric_columns": numeric_df.columns.tolist()
        }
        
        message = (
            f"相关性分析完成，"
            f"发现 {len(strong_correlations)} 组显著相关"
        )
        
        return ToolResult(
            success=True,
            data=result_data,
            message=message
        )
