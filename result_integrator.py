# result_integrator.py
import re
from typing import Dict, List, Any, Tuple
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)

FIELD_DEFS = [
    {"name": "material", "type": "string"},
    {"name": "metal_center", "type": "string"},
    {"name": "coordination", "type": "string"},
    {"name": "enzyme_type", "type": "string"},
    {"name": "Km", "type": "float", "unit": "mM"},
    {"name": "Vmax", "type": "float", "unit": "mM/s"},
    {"name": "pH_opt", "type": "float"},
    {"name": "T_opt", "type": "float", "unit": "°C"},
    {"name": "characterization", "type": "list"},
    {"name": "table_data", "type": "string"},
]

class ResultIntegrator:
    def __init__(self, confidence_threshold: float = 0.7):
        self.threshold = confidence_threshold

    def _normalize_value(self, value: Any, field_def: Dict) -> Tuple[Any, float]:
        if value is None or value == "":
            return None, 0.0

        conf = 1.0
        if field_def['type'] == 'float':
            try:
                if isinstance(value, str):
                    match = re.search(r'(\d+\.?\d*)', value)
                    if match:
                        value = float(match.group(1))
                        conf = 0.9
                    else:
                        return None, 0.0
                else:
                    value = float(value)
            except:
                return None, 0.0
        elif field_def['type'] == 'list':
            if isinstance(value, str):
                value = [v.strip() for v in value.split(',')]
                conf = 0.8
            elif not isinstance(value, list):
                value = [str(value)]
                conf = 0.6
        return value, conf

    def integrate(self, llm_results: List[Dict], vlm_results: List[Dict]) -> Dict:
        candidates = defaultdict(list)

        for i, r in enumerate(llm_results):
            if not isinstance(r, dict):
                continue
            for field in FIELD_DEFS:
                if field['name'] in r and r[field['name']] is not None:
                    val, conf = self._normalize_value(r[field['name']], field)
                    if val is not None:
                        candidates[field['name']].append((val, conf, f"llm_{i}"))

        for i, r in enumerate(vlm_results):
            if not isinstance(r, dict) or 'extracted_values' not in r:
                continue
            ev = r['extracted_values']
            for key in ['Km', 'Vmax', 'particle_size']:
                if key in ev and ev[key] and 'value' in ev[key]:
                    val = ev[key]['value']
                    if val is not None:
                        field_def = next((f for f in FIELD_DEFS if f['name'] == key), None)
                        if field_def:
                            norm_val, conf = self._normalize_value(val, field_def)
                            candidates[key].append((norm_val, conf * 0.9, f"vlm_{i}"))

        final = {"fields": {}, "metadata": {}}
        for field in FIELD_DEFS:
            name = field['name']
            cands = candidates.get(name, [])
            if not cands:
                final["fields"][name] = {"value": None, "confidence": 0.0, "source": None}
                continue

            best = max(cands, key=lambda x: x[1])
            final["fields"][name] = {
                "value": best[0],
                "confidence": best[1],
                "source": best[2],
                "needs_review": best[1] < self.threshold
            }

        final["metadata"]["llm_chunks"] = len(llm_results)
        final["metadata"]["vlm_tasks"] = len(vlm_results)
        return final