# conftest.py - pytest 配置和共享 fixtures
"""
pytest 配置和共享 fixtures
"""

import sys
import os
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pytest
import json


@pytest.fixture
def sample_config():
    """示例配置数据"""
    return {
        'text_llm': {
            'base_url': 'https://api.example.com/v1',
            'api_key': 'test-key-123',
            'model': 'test-model',
            'max_retries': 3,
            'temperature': 0.1,
            'text_max_tokens': 4096
        },
        'vision_vlm': {
            'base_url': 'https://api.example.com/v1',
            'api_key': 'test-key-456',
            'model': 'test-vlm',
            'max_retries': 3,
            'vision_max_tokens': 2048
        },
        'chunk_batch_size': 5,
        'vlm_batch_size': 2,
        'confidence_threshold': 0.7,
        'results_dir': './test_results',
        'rulebook_path': './test_rulebook.json'
    }


@pytest.fixture
def sample_llm_result():
    """示例 LLM 提取结果"""
    return {
        'material': 'Fe3O4',
        'metal_center': 'Fe',
        'enzyme_type': 'peroxidase-like',
        'Km': 0.32,
        'Vmax': 1.5e-4,
        'pH_opt': 3.5,
        'T_opt': 35,
        'characterization': ['TEM', 'XRD', 'XPS'],
        'table_data': None
    }


@pytest.fixture
def sample_vlm_result():
    """示例 VLM 提取结果"""
    return {
        'chart_type': 'Lineweaver-Burk',
        'extracted_values': {
            'Km': {'value': 0.35, 'unit': 'mM'},
            'Vmax': {'value': 1.6e-4, 'unit': 'mM/s'},
            'particle_size': {'value': 8.5, 'unit': 'nm'}
        },
        'observations': '颗粒分布均匀，平均尺寸约8.5nm'
    }


@pytest.fixture
def sample_extraction_output():
    """示例提取输出"""
    return {
        'fields': {
            'material': {
                'value': 'Fe3O4 nanoparticles',
                'confidence': 0.95,
                'source': 'llm_0',
                'needs_review': False
            },
            'metal_center': {
                'value': 'Fe',
                'confidence': 0.90,
                'source': 'llm_1',
                'needs_review': False
            },
            'enzyme_type': {
                'value': 'peroxidase-like',
                'confidence': 0.85,
                'source': 'llm_2',
                'needs_review': False
            },
            'Km': {
                'value': 0.32,
                'confidence': 0.78,
                'source': 'llm_3',
                'needs_review': True
            },
            'Vmax': {
                'value': 1.5e-4,
                'confidence': 0.72,
                'source': 'vlm_0',
                'needs_review': True
            },
            'pH_opt': {
                'value': 3.5,
                'confidence': 0.88,
                'source': 'llm_4',
                'needs_review': False
            },
            'T_opt': {
                'value': 35,
                'confidence': 0.75,
                'source': 'llm_5',
                'needs_review': False
            },
            'characterization': {
                'value': ['TEM', 'XRD', 'XPS'],
                'confidence': 0.92,
                'source': 'llm_6',
                'needs_review': False
            }
        },
        'metadata': {
            'file_name': 'test_paper.pdf',
            'title': 'Fe3O4 Nanozyme with Peroxidase-like Activity',
            'llm_chunks': 10,
            'vlm_tasks': 5,
            'processed_at': '2024-01-01T12:00:00'
        }
    }


@pytest.fixture
def sample_mid_task():
    """示例中间任务 JSON"""
    return {
        'metadata': {
            'file_name': 'sample.pdf',
            'title': 'Sample Nanozyme Paper',
            'author': 'Test Author'
        },
        'llm_task': {
            'prompt_template': 'Extract nanozyme info from: {{text}}',
            'chunks': [
                'This is a Fe3O4 nanozyme with peroxidase-like activity. The Km value is 0.32 mM.',
                'The Vmax was measured to be 1.5×10^-4 mM/s under optimal conditions.'
            ]
        },
        'vlm_tasks': [
            {'image_path': 'fig1.png', 'caption': 'TEM image', 'page': 1},
            {'image_path': 'fig2.png', 'caption': 'Lineweaver-Burk plot', 'page': 2}
        ],
        'extracted_hints': {
            'chemical_formula': 'Fe3O4',
            'catalytic_activity': 'peroxidase',
            'Km_candidates': ['0.32 mM'],
            'Vmax_candidates': ['1.5×10^-4 mM/s']
        }
    }


@pytest.fixture
def temp_dir(tmp_path):
    """临时目录 fixture"""
    return tmp_path


@pytest.fixture
def mock_api_response():
    """模拟 API 响应"""
    return {
        'choices': [{
            'message': {
                'content': '{"material": "Fe3O4", "enzyme_type": "peroxidase-like"}'
            }
        }]
    }
