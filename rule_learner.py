# rule_learner.py - 增强版：真正的规则学习机制
import json
import re
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from collections import defaultdict
from datetime import datetime
import logging
from difflib import SequenceMatcher

logger = logging.getLogger(__name__)


class RuleLearner:
    """
    增强版规则学习器：

    1. 自动规则提取：从修正记录中学习模式和规则
    2. 提示模板优化：基于历史表现调整LLM提示
    3. 置信度自适应：根据规则匹配程度动态调整置信度
    4. 规则优先级：优先使用高置信度规则
    """

    def __init__(self, rulebook_path: str = "rulebook.json"):
        self.rulebook_path = Path(rulebook_path)
        self.rules = self._load()
        self._initialize_structures()

    def _initialize_structures(self):
        """初始化规则库结构"""
        if 'corrections' not in self.rules:
            self.rules['corrections'] = []
        if 'learned_rules' not in self.rules:
            self.rules['learned_rules'] = {}
        if 'field_stats' not in self.rules:
            self.rules['field_stats'] = {}
        if 'prompt_adjustments' not in self.rules:
            self.rules['prompt_adjustments'] = {}
        if 'similarity_cache' not in self.rules:
            self.rules['similarity_cache'] = {}

    def _load(self) -> Dict:
        if self.rulebook_path.exists():
            try:
                with open(self.rulebook_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # 确保所有必需字段存在
                    default_structure = {
                        'corrections': [],
                        'learned_rules': {},
                        'field_stats': {},
                        'prompt_adjustments': {},
                        'similarity_cache': {},
                        'metadata': {
                            'created': datetime.now().isoformat(),
                            'version': '2.0'
                        }
                    }
                    for key, value in default_structure.items():
                        if key not in data:
                            data[key] = value
                    return data
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"规则库加载失败，使用默认结构: {e}")

        return {
            'corrections': [],
            'learned_rules': {},
            'field_stats': {},
            'prompt_adjustments': {},
            'similarity_cache': {},
            'metadata': {
                'created': datetime.now().isoformat(),
                'version': '2.0'
            }
        }

    def save(self):
        """保存规则库到文件"""
        self.rules['metadata']['last_updated'] = datetime.now().isoformat()
        self.rules['metadata']['total_corrections'] = len(self.rules['corrections'])
        with open(self.rulebook_path, 'w', encoding='utf-8') as f:
            json.dump(self.rules, f, indent=2, ensure_ascii=False)
        logger.info(f"规则库已保存至 {self.rulebook_path}")

    def learn_from_correction(self, field: str, original_value: Any, corrected_value: Any):
        """
        从修正中学习并更新规则库

        Args:
            field: 字段名
            original_value: LLM提取的原始值
            corrected_value: 用户修正后的正确值
        """
        correction = {
            'field': field,
            'original': str(original_value) if original_value is not None else None,
            'corrected': str(corrected_value) if corrected_value is not None else None,
            'timestamp': datetime.now().isoformat()
        }
        self.rules['corrections'].append(correction)

        # 更新字段统计
        self._update_field_stats(field, original_value, corrected_value)

        # 提取并更新规则
        self._extract_and_update_rules(field, original_value, corrected_value)

        # 更新相似性缓存
        self._update_similarity_cache(field, original_value, corrected_value)

        self.save()
        logger.info(f"记录修正并更新规则: {field} '{original_value}' -> '{corrected_value}'")

    def _update_field_stats(self, field: str, original: Any, corrected: Any):
        """更新字段统计信息"""
        if field not in self.rules['field_stats']:
            self.rules['field_stats'][field] = {
                'total_corrections': 0,
                'error_types': defaultdict(int),
                'common_errors': defaultdict(int),
                'accuracy_trend': []
            }

        stats = self.rules['field_stats'][field]
        stats['total_corrections'] += 1

        # 分析错误类型
        if original is None and corrected is not None:
            error_type = 'missing'
        elif original is not None and corrected is None:
            error_type = 'extra'
        elif original != corrected:
            error_type = 'incorrect'
        else:
            error_type = 'unchanged'

        stats['error_types'][error_type] += 1

        # 记录常见错误模式
        if original is not None and corrected is not None:
            error_key = self._normalize_for_comparison(str(original))
            if error_key:
                stats['common_errors'][error_key] += 1

        # 计算准确率趋势（最近20次）
        recent = self.rules['corrections'][-20:]
        total = len(recent)
        if total > 0:
            correct = sum(1 for c in recent if c['original'] == c['corrected'])
            accuracy = correct / total
            stats['accuracy_trend'].append({
                'timestamp': datetime.now().isoformat(),
                'accuracy': accuracy,
                'sample_size': total
            })
            # 只保留最近10个趋势点
            stats['accuracy_trend'] = stats['accuracy_trend'][-10:]

    def _normalize_for_comparison(self, text: str) -> str:
        """规范化文本用于比较"""
        # 移除数字和常见单位
        text = re.sub(r'\d+\.?\d*', 'N', text)
        text = re.sub(r'(mM|μM|uM|nM|nm|°C)', '', text)
        text = re.sub(r'\s+', '', text)
        return text.lower()

    def _extract_and_update_rules(self, field: str, original: Any, corrected: Any):
        """
        从修正中提取模式规则

        例如：
        - 如果原始值是 "Fe3O4 nanoparticles"，正确值是 "Fe3O4"
        - 学习到：field 'material'，对于包含 "nanoparticles" 的值应该简化
        """
        if original is None or corrected is None:
            return

        orig_str = str(original)
        corr_str = str(corrected)

        # 规则1：提取后缀/前缀模式
        suffixes = self._extract_suffixes(orig_str, corr_str)
        for suffix in suffixes:
            rule_key = f"{field}_remove_suffix:{suffix}"
            self._add_learned_rule(rule_key, {
                'type': 'suffix_removal',
                'pattern': suffix,
                'field': field,
                'confidence': 0.7
            })

        # 规则2：数值范围规范化
        if self._contains_numeric(orig_str) and self._contains_numeric(corr_str):
            self._learn_numeric_rule(field, orig_str, corr_str)

        # 规则3：常见错误模式
        self._learn_common_pattern(field, orig_str, corr_str)

    def _extract_suffixes(self, original: str, corrected: str) -> List[str]:
        """提取被移除的后缀"""
        suffixes = []
        orig_lower = original.lower()
        corr_lower = corrected.lower()

        common_suffixes = [
            'nanoparticles', 'nanoparticle', 'nanoparticles', 'NPs', 'NP',
            'nanocrystals', 'nanocrystal', 'nanostructure', 'nano',
            'particles', 'particle', 'composite', 'hybrid'
        ]

        for suffix in common_suffixes:
            if suffix in orig_lower and suffix not in corr_lower:
                # 提取带空格的完整后缀
                pattern = re.search(rf'\b(\w*)\s*{re.escape(suffix)}\b', original, re.IGNORECASE)
                if pattern:
                    suffixes.append(pattern.group(0))

        return suffixes

    def _contains_numeric(self, text: str) -> bool:
        """检查文本是否包含数字"""
        return bool(re.search(r'\d', text))

    def _learn_numeric_rule(self, field: str, original: str, corrected: str):
        """学习数值规范化规则"""
        # 提取数值
        orig_nums = re.findall(r'[\d.]+', original)
        corr_nums = re.findall(r'[\d.]+', corrected)

        if len(orig_nums) == 1 and len(corr_nums) == 1:
            try:
                orig_val = float(orig_nums[0])
                corr_val = float(corr_nums[0])

                # 如果是科学计数法
                if 'e' in original.lower() or '×10' in original:
                    rule_key = f"{field}_scientific_notation"
                    self._add_learned_rule(rule_key, {
                        'type': 'scientific_notation',
                        'field': field,
                        'action': 'convert_to_decimal',
                        'confidence': 0.8
                    })

                # 如果数值比例是10的幂次
                if orig_val > 0 and corr_val > 0:
                    ratio = orig_val / corr_val
                    if ratio > 100:
                        rule_key = f"{field}_unit_conversion"
                        self._add_learned_rule(rule_key, {
                            'type': 'unit_conversion',
                            'field': field,
                            'ratio': ratio,
                            'confidence': 0.75
                        })
            except ValueError:
                pass

    def _learn_common_pattern(self, field: str, original: str, corrected: str):
        """学习常见错误模式"""
        # 提取括号内容
        orig_brackets = re.findall(r'\([^)]+\)', original)
        corr_brackets = re.findall(r'\([^)]+\)', corrected)

        if len(orig_brackets) > len(corr_brackets):
            # LLM倾向于添加不必要的括号说明
            rule_key = f"{field}_unnecessary_brackets"
            self._add_learned_rule(rule_key, {
                'type': 'bracket_removal',
                'field': field,
                'action': 'simplify_brackets',
                'confidence': 0.65
            })

    def _add_learned_rule(self, rule_key: str, rule_data: Dict):
        """添加或更新学习到的规则"""
        if rule_key not in self.rules['learned_rules']:
            self.rules['learned_rules'][rule_key] = {
                'count': 0,
                'total_confidence': 0.0,
                'last_updated': datetime.now().isoformat(),
                'data': rule_data
            }

        existing = self.rules['learned_rules'][rule_key]
        existing['count'] += 1
        existing['total_confidence'] += rule_data.get('confidence', 0.5)
        existing['last_updated'] = datetime.now().isoformat()

        # 更新平均置信度
        avg_confidence = existing['total_confidence'] / existing['count']
        existing['data']['confidence'] = min(0.95, avg_confidence + 0.1)  # 随使用次数提升置信度
        existing['data']['times_used'] = existing['count']

    def _update_similarity_cache(self, field: str, original: Any, corrected: Any):
        """更新相似性缓存，用于快速查找类似错误"""
        if original is None or corrected is None:
            return

        key = self._normalize_for_comparison(str(original))
        if key:
            if field not in self.rules['similarity_cache']:
                self.rules['similarity_cache'][field] = {}

            self.rules['similarity_cache'][field][key] = {
                'correct_value': str(corrected),
                'timestamp': datetime.now().isoformat(),
                'usage_count': 0
            }

    # ========== 规则应用接口 ==========

    def apply_rules(self, field: str, value: Any) -> Tuple[Any, float]:
        """
        应用学习到的规则修正值

        Returns:
            (修正后的值, 修正置信度)
        """
        if value is None:
            return None, 0.0

        value_str = str(value)
        base_confidence = 1.0

        # 检查相似性缓存
        cache_correction = self._check_similarity_cache(field, value_str)
        if cache_correction:
            return cache_correction

        # 应用学习到的规则
        field_rules = {k: v for k, v in self.rules['learned_rules'].items()
                      if v['data'].get('field') == field}

        for rule_key, rule_info in field_rules.items():
            rule_data = rule_info['data']
            confidence = rule_data.get('confidence', 0.5)

            if rule_data['type'] == 'suffix_removal':
                pattern = rule_data.get('pattern', '')
                if pattern and pattern.lower() in value_str.lower():
                    value_str = re.sub(re.escape(pattern), '', value_str, flags=re.IGNORECASE)
                    base_confidence *= (1 + confidence) / 2

            elif rule_data['type'] == 'bracket_removal':
                brackets = re.findall(r'\([^)]+\)', value_str)
                if len(brackets) > 1:
                    # 保留最后一个括号内容
                    value_str = brackets[-1].strip('()')
                    base_confidence *= (1 + confidence) / 2

            elif rule_data['type'] == 'scientific_notation':
                if '×10' in value_str or 'e' in value_str.lower():
                    try:
                        # 转换为十进制
                        match = re.search(r'([\d.]+)\s*×\s*10\s*\^?\s*-?(\d+)', value_str)
                        if match:
                            value_str = str(float(match.group(1)) * (10 ** int(match.group(2))))
                            base_confidence *= (1 + confidence) / 2
                    except (ValueError, ArithmeticError):
                        pass

        # 清理空白
        value_str = ' '.join(value_str.split())
        if value_str == str(value):
            return value, base_confidence

        return value_str if value_str else value, min(base_confidence, 0.8)

    def _check_similarity_cache(self, field: str, value: str) -> Optional[Tuple[Any, float]]:
        """检查相似性缓存，返回修正值或None"""
        if field not in self.rules['similarity_cache']:
            return None

        normalized = self._normalize_for_comparison(value)
        cache = self.rules['similarity_cache'][field]

        # 精确匹配
        if normalized in cache:
            entry = cache[normalized]
            entry['usage_count'] = entry.get('usage_count', 0) + 1
            confidence = min(0.95, 0.6 + entry['usage_count'] * 0.05)
            logger.debug(f"相似性缓存命中: {field} '{value}' -> '{entry['correct_value']}'")
            return entry['correct_value'], confidence

        # 模糊匹配
        for pattern, entry in cache.items():
            similarity = SequenceMatcher(None, normalized, pattern).ratio()
            if similarity > 0.85:
                entry['usage_count'] = entry.get('usage_count', 0) + 1
                confidence = min(0.9, similarity * 0.7)
                logger.debug(f"模糊匹配: similarity={similarity:.2f}")
                return entry['correct_value'], confidence

        return None

    def get_prompt_adjustment(self, field: str) -> str:
        """
        获取针对特定字段的提示调整

        基于历史错误模式，生成额外的提示指导
        """
        if field not in self.rules['field_stats']:
            return ""

        stats = self.rules['field_stats'][field]
        adjustments = []

        # 根据错误类型添加提示
        error_types = stats['error_types']
        total = sum(error_types.values())
        if total >= 3:
            most_common_error = max(error_types.items(), key=lambda x: x[1])
            error_ratio = most_common_error[1] / total

            if error_ratio > 0.5:
                if most_common_error[0] == 'missing':
                    adjustments.append("注意：该字段可能需要更仔细地搜索全文")
                elif most_common_error[0] == 'incorrect':
                    adjustments.append("建议：验证该字段的数值是否合理")

        # 根据常见错误添加具体指导
        common_errors = stats.get('common_errors', {})
        if common_errors:
            top_errors = sorted(common_errors.items(), key=lambda x: x[1], reverse=True)[:2]
            for error_pattern, count in top_errors:
                if count >= 2:
                    adjustments.append(f"注意：常见错误模式 '{error_pattern}'，请简化或修正")

        return " | ".join(adjustments) if adjustments else ""

    def get_field_confidence_multiplier(self, field: str) -> float:
        """
        获取字段置信度乘数

        基于该字段的历史准确率调整置信度
        """
        if field not in self.rules['field_stats']:
            return 1.0

        stats = self.rules['field_stats'][field]
        accuracy_trend = stats.get('accuracy_trend', [])

        if not accuracy_trend:
            return 1.0

        # 使用最近的准确率
        latest = accuracy_trend[-1]
        accuracy = latest['accuracy']

        # 准确率高 -> 提高置信度
        # 准确率低 -> 降低置信度
        if accuracy >= 0.9:
            return 1.1  # 高准确率，稍微提高
        elif accuracy >= 0.7:
            return 1.0  # 正常
        elif accuracy >= 0.5:
            return 0.9  # 较低准确率，稍微降低
        else:
            return 0.8  # 准确率很低，需要人工审核

    def get_statistics(self) -> Dict:
        """获取规则库统计信息"""
        return {
            'total_corrections': len(self.rules['corrections']),
            'total_rules': len(self.rules['learned_rules']),
            'fields_with_rules': list(self.rules['field_stats'].keys()),
            'field_stats': {
                field: {
                    'corrections': stats['total_corrections'],
                    'error_types': dict(stats['error_types']),
                    'latest_accuracy': stats['accuracy_trend'][-1]['accuracy'] if stats['accuracy_trend'] else None
                }
                for field, stats in self.rules['field_stats'].items()
            },
            'most_used_rules': sorted(
                [(k, v['count']) for k, v in self.rules['learned_rules'].items()],
                key=lambda x: x[1],
                reverse=True
            )[:5],
            'last_updated': self.rules['metadata'].get('last_updated')
        }

    def prune_old_corrections(self, keep_last: int = 1000):
        """清理旧的修正记录，保留最近的"""
        if len(self.rules['corrections']) > keep_last:
            self.rules['corrections'] = self.rules['corrections'][-keep_last:]
            logger.info(f"已清理旧修正记录，保留最近 {keep_last} 条")

    def merge_rules(self, other_rulebook_path: str):
        """合并另一个规则库"""
        other = RuleLearner(other_rulebook_path)
        other_rules = other.rules

        # 合并修正记录
        self.rules['corrections'].extend(other_rules.get('corrections', []))
        self.prune_old_corrections()

        # 合并规则，保留置信度更高的
        for rule_key, rule_info in other_rules.get('learned_rules', {}).items():
            if rule_key not in self.rules['learned_rules']:
                self.rules['learned_rules'][rule_key] = rule_info
            else:
                existing = self.rules['learned_rules'][rule_key]
                if rule_info['total_confidence'] > existing['total_confidence']:
                    self.rules['learned_rules'][rule_key] = rule_info

        # 合并相似性缓存
        for field, cache in other_rules.get('similarity_cache', {}).items():
            if field not in self.rules['similarity_cache']:
                self.rules['similarity_cache'][field] = cache
            else:
                self.rules['similarity_cache'][field].update(cache)

        self.save()
        logger.info(f"已合并规则库: {other_rulebook_path}")


if __name__ == "__main__":
    # 测试代码
    logging.basicConfig(level=logging.INFO)

    learner = RuleLearner("test_rulebook.json")

    # 模拟一些修正
    learner.learn_from_correction('material', 'Fe3O4 nanoparticles', 'Fe3O4')
    learner.learn_from_correction('material', 'CoFe2O4 nano', 'CoFe2O4')
    learner.learn_from_correction('Km', '0.5×10^-3 mM', '0.0005 mM')
    learner.learn_from_correction('enzyme_type', 'peroxidase-like activity', 'peroxidase-like')

    # 应用规则
    corrected, confidence = learner.apply_rules('material', 'Fe3O4 nanoparticles')
    print(f"应用规则后: '{corrected}' (置信度: {confidence:.2f})")

    # 获取提示调整
    adjustment = learner.get_prompt_adjustment('material')
    print(f"提示调整: {adjustment}")

    # 打印统计
    print("\n规则库统计:")
    print(json.dumps(learner.get_statistics(), indent=2, ensure_ascii=False))
