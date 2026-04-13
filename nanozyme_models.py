# nanozyme_models.py - Pydantic 数据模型 (兼容 Pydantic 2.0)
"""
纳米酶文献提取系统 - Pydantic 数据模型

功能：
1. 定义结构化的数据模型
2. 自动验证字段类型和范围
3. 支持序列化/反序列化
4. 提供置信度报告

使用方法：
    from nanozyme_models import NanozymeResult, ExtractionOutput, ValidationReport
    
    result = NanozymeResult.model_validate(data)
    if result.validation_status == "valid":
        print(result.to_dict())
"""

from pydantic import (
    BaseModel, Field, field_validator, model_validator,
    ConfigDict, computed_field, PrivateAttr
)
from typing import Optional, List, Dict, Any, Union
from enum import Enum
from datetime import datetime
import re


class EnzymeType(str, Enum):
    """酶活性类型枚举"""
    PEROXIDASE = "peroxidase-like"
    OXIDASE = "oxidase-like"
    CATALASE = "catalase-like"
    SUPEROXIDE_DISMUTASE = "superoxide dismutase-like"
    ESTERASE = "esterase-like"
    GLUTATHIONE_PEROXIDASE = "glutathione peroxidase-like"
    UNKNOWN = "unknown"
    
    @classmethod
    def normalize(cls, value: str) -> 'EnzymeType':
        """规范化酶类型字符串"""
        if not value:
            return cls.UNKNOWN
        
        value_lower = value.lower().strip()
        
        mappings = {
            'peroxidase': cls.PEROXIDASE,
            'pod': cls.PEROXIDASE,
            'oxidase': cls.OXIDASE,
            'cat': cls.CATALASE,
            'catalase': cls.CATALASE,
            'sod': cls.SUPEROXIDE_DISMUTASE,
            'superoxide': cls.SUPEROXIDE_DISMUTASE,
            'esterase': cls.ESTERASE,
            'gpx': cls.GLUTATHIONE_PEROXIDASE,
        }
        
        for key, enum_val in mappings.items():
            if key in value_lower:
                return enum_val
        
        return cls.UNKNOWN


class ValidationStatus(str, Enum):
    """验证状态"""
    VALID = "valid"
    INVALID = "invalid"
    PARTIAL = "partial"
    UNKNOWN = "unknown"


class KineticParameters(BaseModel):
    """动力学参数"""
    Km: Optional[float] = Field(None, ge=0, description="米氏常数 (mM)")
    Vmax: Optional[float] = Field(None, ge=0, description="最大反应速率")
    Km_unit: str = Field("mM", description="Km单位")
    Vmax_unit: str = Field("mM/s", description="Vmax单位")
    substrate: Optional[str] = Field(None, description="底物")


class FieldConfidence(BaseModel):
    """字段置信度信息"""
    value: Any = Field(None, description="字段值")
    confidence: float = Field(0.0, ge=0, le=1, description="置信度")
    source: Optional[str] = Field(None, description="来源")
    needs_review: bool = Field(False, description="是否需要人工审核")
    
    @computed_field
    @property
    def is_reliable(self) -> bool:
        """是否可靠"""
        return self.confidence >= 0.7 and not self.needs_review


class NanozymeResult(BaseModel):
    """
    纳米酶提取结果模型
    
    包含所有提取字段
    """
    model_config = ConfigDict(
        str_strip_whitespace=True,
        validate_assignment=True,
    )
    
    # 材料信息
    material: Optional[str] = Field(None, description="纳米酶材料名称")
    metal_center: Optional[str] = Field(None, description="金属中心")
    coordination: Optional[str] = Field(None, description="配位环境")
    
    # 酶活性
    enzyme_type: Optional[str] = Field(None, description="酶活性类型")
    
    # 动力学参数
    Km: Optional[float] = Field(None, ge=0, description="米氏常数 (mM)")
    Vmax: Optional[float] = Field(None, ge=0, description="最大反应速率")
    
    # 反应条件
    pH_opt: Optional[float] = Field(None, ge=0, le=14, description="最适pH")
    T_opt: Optional[float] = Field(None, ge=-20, le=150, description="最适温度")
    
    # 表征和额外信息
    characterization: Optional[List[str]] = Field(default_factory=list)
    table_data: Optional[str] = Field(None, description="表格数据")
    
    # 置信度信息 - 使用普通字段存储
    confidence_data: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    
    @field_validator('enzyme_type', mode='before')
    @classmethod
    def normalize_enzyme_type(cls, v):
        if not v:
            return None
        if isinstance(v, EnzymeType):
            return v.value
        return EnzymeType.normalize(str(v)).value
    
    @field_validator('Km', 'Vmax', 'pH_opt', 'T_opt', mode='before')
    @classmethod
    def parse_numeric(cls, v):
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return float(v)
        if isinstance(v, str):
            match = re.search(r'[\d.]+', v.strip())
            if match:
                return float(match.group())
        return None
    
    @field_validator('characterization', mode='before')
    @classmethod
    def normalize_characterization(cls, v):
        if isinstance(v, str):
            return [c.strip() for c in v.split(',') if c.strip()]
        if isinstance(v, list):
            return [str(c).strip() for c in v if c]
        return []
    
    def get_confidence(self, field: str) -> FieldConfidence:
        """获取字段的置信度信息"""
        data = self.confidence_data.get(field, {})
        return FieldConfidence(**data)
    
    def set_confidence(self, field: str, confidence: FieldConfidence):
        """设置字段的置信度信息"""
        self.confidence_data[field] = confidence.model_dump()
    
    def get_confidence_report(self) -> Dict[str, Any]:
        """生成置信度报告"""
        fields_info = []
        
        for field_name in self.model_fields.keys():
            if field_name == 'confidence_data':
                continue
            value = getattr(self, field_name)
            conf_data = self.confidence_data.get(field_name, {})
            
            fields_info.append({
                'field': field_name,
                'has_value': value is not None,
                'value': value,
                'confidence': conf_data.get('confidence', 0.0),
                'needs_review': conf_data.get('needs_review', True),
                'source': conf_data.get('source')
            })
        
        total_fields = len([f for f in fields_info if f['field'] != 'confidence_data'])
        filled_fields = sum(1 for f in fields_info if f['has_value'])
        high_confidence = sum(1 for f in fields_info if f['confidence'] >= 0.7)
        
        return {
            'total_fields': total_fields,
            'filled_fields': filled_fields,
            'fill_rate': round(filled_fields / total_fields, 2) if total_fields else 0,
            'high_confidence_fields': high_confidence,
            'fields': fields_info,
            'extraction_quality': self._evaluate_quality(fields_info)
        }
    
    def _evaluate_quality(self, fields_info: List[Dict]) -> str:
        """评估提取质量"""
        if not fields_info:
            return "poor"
        fill_rate = sum(1 for f in fields_info if f['has_value']) / len(fields_info)
        avg_confidence = sum(f['confidence'] for f in fields_info) / len(fields_info)
        
        if fill_rate >= 0.8 and avg_confidence >= 0.8:
            return "excellent"
        elif fill_rate >= 0.6 and avg_confidence >= 0.6:
            return "good"
        elif fill_rate >= 0.4:
            return "fair"
        else:
            return "poor"
    
    @computed_field
    @property
    def is_complete(self) -> bool:
        """是否完整（核心字段都有值）"""
        core_fields = ['material', 'enzyme_type', 'Km', 'Vmax']
        return all(getattr(self, f) is not None for f in core_fields)


class ExtractionMetadata(BaseModel):
    """提取元数据"""
    file_name: Optional[str] = Field(None, description="原始文件名")
    title: Optional[str] = Field(None, description="文献标题")
    author: Optional[str] = Field(None, description="作者")
    pages: Optional[int] = Field(None, ge=0, description="页数")
    
    processed_at: str = Field(default_factory=lambda: datetime.now().isoformat())
    processing_time: Optional[float] = Field(None, ge=0, description="处理时间(秒)")
    llm_chunks: int = Field(0, ge=0, description="处理的LLM文本块数")
    vlm_tasks: int = Field(0, ge=0, description="处理的VLM图像任务数")
    llm_model: Optional[str] = Field(None, description="使用的LLM模型")
    vlm_model: Optional[str] = Field(None, description="使用的VLM模型")


class ExtractionOutput(BaseModel):
    """完整提取输出模型"""
    model_config = ConfigDict(str_strip_whitespace=True, validate_assignment=True)
    
    fields: Dict[str, Any] = Field(default_factory=dict)
    metadata: ExtractionMetadata = Field(default_factory=ExtractionMetadata)
    raw_confidences: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    
    def to_legacy_format(self) -> Dict[str, Any]:
        """转换为原有格式（兼容）"""
        result = {
            'fields': {},
            'metadata': self.metadata.model_dump()
        }
        
        for field_name, value in self.fields.items():
            conf = self.raw_confidences.get(field_name, {})
            result['fields'][field_name] = {
                'value': value,
                'confidence': conf.get('confidence', 0.0),
                'source': conf.get('source'),
                'needs_review': conf.get('needs_review', True)
            }
        
        return result
    
    @classmethod
    def from_legacy_format(cls, data: Dict) -> 'ExtractionOutput':
        """从原有格式创建"""
        fields = {}
        confidences = {}
        
        for field_name, field_data in data.get('fields', {}).items():
            if isinstance(field_data, dict):
                fields[field_name] = field_data.get('value')
                confidences[field_name] = {
                    'confidence': field_data.get('confidence', 0.0),
                    'source': field_data.get('source'),
                    'needs_review': field_data.get('needs_review', True)
                }
            else:
                fields[field_name] = field_data
        
        metadata = ExtractionMetadata(**data.get('metadata', {}))
        
        output = cls(fields=fields, metadata=metadata)
        output.raw_confidences = confidences
        return output
    
    def get_quality_score(self) -> float:
        """计算质量分数"""
        if not self.fields:
            return 0.0
        
        scores = []
        for field_name in self.fields.keys():
            if self.fields[field_name] is not None:
                conf = self.raw_confidences.get(field_name, {})
                scores.append(conf.get('confidence', 0.0))
        
        return sum(scores) / len(scores) if scores else 0.0


class ValidationError(BaseModel):
    """验证错误"""
    field: str = Field(description="字段名")
    message: str = Field(description="错误信息")
    severity: str = Field("error", description="严重程度")


class ValidationReport(BaseModel):
    """验证报告"""
    status: ValidationStatus = Field(default=ValidationStatus.UNKNOWN)
    errors: List[ValidationError] = Field(default_factory=list)
    warnings: List[ValidationError] = Field(default_factory=list)
    validated_at: str = Field(default_factory=lambda: datetime.now().isoformat())
    
    @computed_field
    @property
    def is_valid(self) -> bool:
        return self.status == ValidationStatus.VALID and len(self.errors) == 0
    
    @computed_field
    @property
    def error_count(self) -> int:
        return len(self.errors)
    
    @computed_field
    @property
    def warning_count(self) -> int:
        return len(self.warnings)
    
    def add_error(self, field: str, message: str):
        """添加错误"""
        self.errors.append(ValidationError(field=field, message=message, severity="error"))
        self.status = ValidationStatus.INVALID
    
    def add_warning(self, field: str, message: str):
        """添加警告"""
        self.warnings.append(ValidationError(field=field, message=message, severity="warning"))
        if self.status == ValidationStatus.UNKNOWN:
            self.status = ValidationStatus.PARTIAL
    
    def summary(self) -> str:
        """生成摘要文本"""
        parts = [f"验证状态: {self.status.value}"]
        if self.errors:
            parts.append(f"错误 {len(self.errors)} 个")
            for e in self.errors[:3]:
                parts.append(f"  - {e.field}: {e.message}")
        if self.warnings:
            parts.append(f"警告 {len(self.warnings)} 个")
        return "\n".join(parts)


# 便捷函数
def validate_extraction_result(data: Dict) -> tuple[bool, ValidationReport]:
    """验证提取结果"""
    report = ValidationReport(status=ValidationStatus.VALID)
    
    if 'fields' not in data:
        report.add_error('root', '缺少 fields 字段')
        return False, report
    
    fields = data.get('fields', {})
    
    # 检查必需字段
    required_fields = ['material', 'enzyme_type']
    for field in required_fields:
        if field not in fields or not fields[field]:
            report.add_warning(field, f'缺少必需字段 {field}')
    
    # 数值字段范围检查
    numeric_checks = {
        'Km': (0, 1000, 'mM'),
        'Vmax': (0, 10000, 'mM/s'),
        'pH_opt': (0, 14, ''),
        'T_opt': (-50, 200, '°C')
    }
    
    for field, (min_val, max_val, unit) in numeric_checks.items():
        if field in fields and fields[field] is not None:
            try:
                value = float(fields[field])
                if not (min_val <= value <= max_val):
                    report.add_warning(field, f'{field}={value}{unit} 超出合理范围')
            except (ValueError, TypeError):
                report.add_error(field, f'无效的数值: {fields[field]}')
    
    return report.is_valid, report
