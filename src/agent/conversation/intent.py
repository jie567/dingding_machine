"""
意图识别模块

识别用户的自然语言输入意图，将用户请求分类为预定义的意图类型。
支持数据查询、数据分析、报表生成、任务管理等意图。
"""

from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from enum import Enum
import re


class IntentType(Enum):
    """
    意图类型枚举
    
    定义了系统支持的所有用户意图类型。
    """
    DATA_QUERY = "data_query"           # 数据查询
    DATA_ANALYSIS = "data_analysis"     # 数据分析
    REPORT_GENERATION = "report_generation"  # 报表生成
    TASK_MANAGEMENT = "task_management" # 任务管理
    HELP = "help"                       # 帮助
    GREETING = "greeting"               # 问候
    CLARIFICATION = "clarification"     # 需要澄清
    UNKNOWN = "unknown"                 # 未知意图


@dataclass
class IntentRecognitionResult:
    """
    意图识别结果
    
    Attributes:
        intent: 识别出的意图类型
        confidence: 置信度 (0-1)
        entities: 提取的实体（如时间、表名、条件等）
        raw_text: 原始输入文本
        message: 识别结果消息
    """
    intent: IntentType
    confidence: float
    entities: Dict[str, Any]
    raw_text: str
    message: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "intent": self.intent.value,
            "confidence": self.confidence,
            "entities": self.entities,
            "raw_text": self.raw_text,
            "message": self.message
        }


class IntentRecognizer:
    """
    意图识别器
    
    基于规则 + 关键词匹配的意图识别。
    后续可升级为基于 LLM 的意图识别。
    
    使用示例：
        recognizer = IntentRecognizer()
        result = recognizer.recognize("查一下昨天的航线收益")
        # result.intent == IntentType.DATA_QUERY
        # result.entities == {"time": "昨天", "subject": "航线收益"}
    """
    
    def __init__(self):
        """初始化意图识别器"""
        # 定义意图匹配规则
        self._intent_patterns = self._build_intent_patterns()
        
        # 实体提取规则
        self._entity_patterns = self._build_entity_patterns()
    
    def _build_intent_patterns(self) -> Dict[IntentType, List[str]]:
        """
        构建意图匹配模式
        
        Returns:
            Dict[IntentType, List[str]]: 意图类型 -> 关键词列表
        """
        return {
            IntentType.DATA_QUERY: [
                "查", "查询", "查一下", "看看", "看一下",
                "显示", "展示", "列出", "统计",
                "多少", "几", "什么", "哪些",
                "select", "query", "show", "list"
            ],
            IntentType.DATA_ANALYSIS: [
                "分析", "对比", "比较", "趋势", "占比",
                "为什么", "原因", "因素", "影响",
                "analyze", "compare", "trend", "proportion"
            ],
            IntentType.REPORT_GENERATION: [
                "生成", "导出", "报表", "报告", "excel",
                "下载", "发送", "推送",
                "generate", "export", "report", "download"
            ],
            IntentType.TASK_MANAGEMENT: [
                "任务", "定时", "每天", "每周", "每月",
                "cron", "schedule", "定时任务",
                "创建任务", "添加任务", "删除任务"
            ],
            IntentType.HELP: [
                "帮助", "怎么用", "能做什么", "功能",
                "help", "what can you do", "how to"
            ],
            IntentType.GREETING: [
                "你好", "您好", "嗨", "hello", "hi",
                "早上好", "下午好", "晚上好"
            ]
        }
    
    def _build_entity_patterns(self) -> Dict[str, List[str]]:
        """
        构建实体提取模式
        
        Returns:
            Dict[str, List[str]]: 实体类型 -> 正则表达式列表
        """
        return {
            "time": [
                r"(今天|昨天|前天|明天|后天)",
                r"(本周|上周|下周)",
                r"(本月|上月|下月)",
                r"(今年|去年|明年)",
                r"(\d{4}[-/年]\d{1,2}[-/月]\d{1,2}[日]?)",
                r"(\d{1,2}[-/月]\d{1,2}[日]?)",
                r"(最近\d+天|过去\d+天|近\d+天)"
            ],
            "data_source": [
                r"(Oracle|oracle|PG|pg|PostgreSQL|postgresql|Doris|doris)",
                r"(航线|航班|收益|利润|CPA|FOC)"
            ],
            "table": [
                r"(TB_[A-Z_]+)",
                r"(表\s*[:：]\s*(\w+))"
            ],
            "format": [
                r"(Excel|excel|EXCEL|表格)",
                r"(图表|图形|柱状图|折线图|饼图)",
                r"(文本|文字|消息)"
            ]
        }
    
    def recognize(self, text: str) -> IntentRecognitionResult:
        """
        识别用户意图
        
        基于关键词匹配识别用户意图，并提取相关实体。
        
        Args:
            text: 用户输入文本
            
        Returns:
            IntentRecognitionResult: 意图识别结果
        """
        if not text or not text.strip():
            return IntentRecognitionResult(
                intent=IntentType.UNKNOWN,
                confidence=0.0,
                entities={},
                raw_text=text,
                message="输入为空"
            )
        
        text = text.strip()
        
        # 1. 意图匹配
        intent_scores = self._match_intent(text)
        
        # 2. 选择最佳意图
        best_intent, best_score = self._select_best_intent(intent_scores)
        
        # 3. 实体提取
        entities = self._extract_entities(text)
        
        # 4. 生成结果消息
        message = self._generate_message(best_intent, best_score, entities)
        
        return IntentRecognitionResult(
            intent=best_intent,
            confidence=best_score,
            entities=entities,
            raw_text=text,
            message=message
        )
    
    def _match_intent(self, text: str) -> Dict[IntentType, float]:
        """
        匹配意图
        
        Args:
            text: 用户输入文本
            
        Returns:
            Dict[IntentType, float]: 意图 -> 匹配分数
        """
        scores = {}
        text_lower = text.lower()
        
        for intent_type, keywords in self._intent_patterns.items():
            score = 0.0
            matched_keywords = []
            
            for keyword in keywords:
                # 精确匹配
                if keyword in text or keyword in text_lower:
                    score += 1.0
                    matched_keywords.append(keyword)
                # 模糊匹配（包含关系）
                elif len(keyword) > 2 and (keyword in text or keyword in text_lower):
                    score += 0.5
                    matched_keywords.append(keyword)
            
            # 归一化分数
            if matched_keywords:
                score = min(score / len(keywords) * 3, 1.0)  # 最高 1.0
            
            scores[intent_type] = score
        
        return scores
    
    def _select_best_intent(
        self, 
        scores: Dict[IntentType, float]
    ) -> tuple[IntentType, float]:
        """
        选择最佳意图
        
        Args:
            scores: 意图匹配分数
            
        Returns:
            tuple[IntentType, float]: 最佳意图和分数
        """
        if not scores:
            return IntentType.UNKNOWN, 0.0
        
        # 按分数排序
        sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        
        best_intent, best_score = sorted_scores[0]
        
        # 如果最高分数太低，标记为未知
        if best_score < 0.1:
            return IntentType.UNKNOWN, best_score
        
        # 如果分数不够高，标记为需要澄清
        if best_score < 0.3:
            return IntentType.CLARIFICATION, best_score
        
        return best_intent, best_score
    
    def _extract_entities(self, text: str) -> Dict[str, List[str]]:
        """
        提取实体
        
        Args:
            text: 用户输入文本
            
        Returns:
            Dict[str, List[str]]: 实体类型 -> 实体值列表
        """
        entities = {}
        
        for entity_type, patterns in self._entity_patterns.items():
            values = []
            
            for pattern in patterns:
                matches = re.findall(pattern, text)
                if matches:
                    # 处理元组结果（当有捕获组时）
                    for match in matches:
                        if isinstance(match, tuple):
                            values.extend([m for m in match if m])
                        else:
                            values.append(match)
            
            if values:
                # 去重并保持顺序
                seen = set()
                unique_values = []
                for v in values:
                    if v not in seen:
                        seen.add(v)
                        unique_values.append(v)
                
                entities[entity_type] = unique_values
        
        return entities
    
    def _generate_message(
        self, 
        intent: IntentType, 
        confidence: float,
        entities: Dict[str, List[str]]
    ) -> str:
        """
        生成结果消息
        
        Args:
            intent: 识别出的意图
            confidence: 置信度
            entities: 提取的实体
            
        Returns:
            str: 结果消息
        """
        intent_names = {
            IntentType.DATA_QUERY: "数据查询",
            IntentType.DATA_ANALYSIS: "数据分析",
            IntentType.REPORT_GENERATION: "报表生成",
            IntentType.TASK_MANAGEMENT: "任务管理",
            IntentType.HELP: "帮助",
            IntentType.GREETING: "问候",
            IntentType.CLARIFICATION: "需要澄清",
            IntentType.UNKNOWN: "未知意图"
        }
        
        message = f"识别意图: {intent_names.get(intent, '未知')} (置信度: {confidence:.2f})"
        
        if entities:
            entity_strs = []
            for entity_type, values in entities.items():
                entity_strs.append(f"{entity_type}: {', '.join(values)}")
            message += f" | 实体: {'; '.join(entity_strs)}"
        
        return message
    
    def get_intent_description(self, intent: IntentType) -> str:
        """
        获取意图描述
        
        Args:
            intent: 意图类型
            
        Returns:
            str: 意图描述
        """
        descriptions = {
            IntentType.DATA_QUERY: "查询数据库中的数据",
            IntentType.DATA_ANALYSIS: "对数据进行分析和统计",
            IntentType.REPORT_GENERATION: "生成 Excel 报表或图表",
            IntentType.TASK_MANAGEMENT: "创建或管理定时任务",
            IntentType.HELP: "获取帮助信息",
            IntentType.GREETING: "打招呼或问候",
            IntentType.CLARIFICATION: "需要用户进一步澄清",
            IntentType.UNKNOWN: "无法识别用户意图"
        }
        
        return descriptions.get(intent, "未知意图")


# 测试代码
if __name__ == "__main__":
    recognizer = IntentRecognizer()
    
    test_cases = [
        "查一下昨天的航线收益",
        "分析一下本月和上月的 CPA 利润对比",
        "生成一个 Excel 报表",
        "每天早上 9 点发送日报",
        "你能做什么",
        "你好",
        "这个是什么意思"
    ]
    
    for text in test_cases:
        result = recognizer.recognize(text)
        print(f"\n输入: {text}")
        print(f"结果: {result.message}")
        print(f"实体: {result.entities}")
