# nanozyme_preprocessor_midjson.py (优化版)
import hashlib
import json
import re
import shutil
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple, Set
from collections import defaultdict
from dataclasses import dataclass

CONFIG = {
    "remove_lines_containing": [
        "Published on", "Downloaded by", "This journal is ©",
        "Cite this:", "DOI:", "Received", "Accepted",
        "View Article Online", "RSC Adv.", "Electronic supplementary information",
        "http://", "https://", "www.rsc.org", "Royal Society of Chemistry"
    ],
    "remove_page_patterns": [r"Page \d+", r"^\d+$", r"\[\d+\]"],
    "section_patterns": {
        "abstract": r"(?i)^abstract\b",
        "introduction": r"(?i)^1\.?\s*introduction\b",
        "experimental": r"(?i)^2\.?\s*(experimental|materials and methods)",
        "results": r"(?i)^3\.?\s*(results|results and discussion)",
        "conclusion": r"(?i)^4\.?\s*conclusion\b",
        "supporting": r"(?i)electronic supplementary information|esi",
    },
    "caption_pattern": r"^(Fig\.|Figure|Scheme|Table)\s+\d+",
    "nanozyme_word_terms": [
        "single-atom", "nanozyme", "nanomimetic",
        "peroxidase-like", "oxidase-like", "catalase-like"
    ],
    "nanozyme_char_terms": [
        "TEM", "XRD", "XPS", "VSM", "EPR", "HRTEM", "SAED", "Km", "Vmax"
    ],
    # === 新增：结构化预提取模式 ===
    "pre_extraction": {
        "chemical_formulas": r'\b(CoFe2O4|Fe3O4|CeO2|MnO2|TiO2|ZnO|CuO|Ag|Au|Pt|Pd)\b',
        "catalytic_activities": {
            "peroxidase": r'\bperoxidase-like\b',
            "oxidase": r'\boxidase-like\b',
            "catalase": r'\bcatalase-like\b',
            "superoxide_dismutase": r'\bsuperoxide dismutase-like\b',
        },
        # === 增强：Km 和 Vmax 的精确模式匹配 ===
        "km_pattern": r'[Kk]m\s*[= :]?\s*(\d+\.?\d*)\s*(mM|μM|uM|nM)',
        "vmax_pattern": r'[Vv]max\s*[= :]?\s*(\d+\.?\d*)\s*[×x]\s*10\s*\^?\s*-?\s*(\d+)\s*(M\s*s-1|M/s|mM\s*min-1|mM/min|μM\s*min-1|μM/min)',
        "ph_pattern": r'pH\s*[=: ]?\s*(\d+\.?\d*)',
        "temp_pattern": r'(\d+)\s*°?C',
        # === 新增：材料消歧上下文关键词 ===
        "main_material_indicators": [
            "synthesized", "prepared", "fabricated", "developed", 
            "novel", "new", "designed", "obtained",
            "we report", "in this work", "our nanozyme"
        ],
        "example_material_indicators": [
            "such as", "for example", "e.g.", "like", 
            "including", "compared to", "conventional",
            "common", "traditional", "typical"
        ],
        "material_context_keywords": [
            "nanotubes", "nanoparticles", "nanorods", "nanosheets",
            "nanoclusters", "quantum dots", "frameworks", "MOF",
            "composite", "hybrid", "doped"
        ],
        "surface_coatings": ["folic acid", "folate", "PEG", "polyethylene glycol", "chitosan", "dextran"],
        "polymers": ["oleic acid", "oleylamine", "oleamine", "PVP", "PEI", "polystyrene"],
        "substrates": ["TMB", "ABTS", "OPD", "H2O2", "dopamine", "luminol"]
    },
    "chunk_size": 1200,
    "chunk_overlap": 100,  # === 优化：减小重叠 ===
    "normalize_whitespace": True,
    "fix_hyphenation": True,
}

@dataclass
class ImageInfo:
    id: int
    page: int
    original_source: str
    bounding_box: List[float]
    caption: Optional[str] = None
    renamed_path: Optional[str] = None

@dataclass
class TextChunk:
    content: str
    section: str
    page_start: int
    page_end: int
    char_count: int

class NanozymePreprocessor:
    def __init__(self, json_path: str, images_root: Optional[str] = None, output_root: Optional[str] = None):
        self.json_path = Path(json_path)
        self.images_root = Path(images_root) if images_root else self.json_path.parent
        self.output_root = Path(output_root) if output_root else self.json_path.parent
        self.renamed_dir = self.output_root / "renamed_images"   # === 优化：统一输出目录 ===
        self.renamed_dir.mkdir(parents=True, exist_ok=True)
        
        with open(json_path, 'r', encoding='utf-8') as f:
            self.data = json.load(f)
        self.kids = self.data.get('kids', [])
        
        self.images: List[ImageInfo] = []
        self.sections: Dict[str, List[str]] = defaultdict(list)
        self.current_section = "unknown"
        self.text_chunks: List[TextChunk] = []
        self._page_height = self._calculate_page_height()
        self.renamed_count = 0
        self.extracted_hints: Dict[str, Any] = {}  # === 新增：存储预提取结果 ===
    
    def _calculate_page_height(self) -> float:
        """动态计算页面高度，基于所有元素的 bounding box 推算"""
        max_y = 842  # 默认 A4 高度
        for elem in self.kids:
            bbox = elem.get('bounding_box')
            if bbox and len(bbox) == 4:
                if bbox[3] > max_y:
                    max_y = bbox[3]
        return max_y

    # ---------- 基础清洗（保持不变） ----------
    def _is_noise_line(self, text: str) -> bool:
        if not text.strip():
            return True
        for kw in CONFIG["remove_lines_containing"]:
            if kw.lower() in text.lower():
                return True
        for pat in CONFIG["remove_page_patterns"]:
            if re.match(pat, text.strip()):
                return True
        if len(text.strip()) < 5 and not re.search(r'[a-zA-Z0-9]', text):
            return True
        return False

    def _fix_hyphenation(self, text: str) -> str:
        return re.sub(r'(\w+)-\s*\n\s*(\w+)', r'\1\2', text)

    def _normalize_whitespace(self, text: str) -> str:
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r' \. ', '. ', text)
        text = re.sub(r' , ', ', ', text)
        text = re.sub(r' \? ', '? ', text)
        return text.strip()

    # ---------- 表格修复 ----------
    def _fix_table(self, text: str) -> str:
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        if len(lines) < 2:
            return text
        if all('|' in line for line in lines):
            return text
        rows = [re.split(r'\s{2,}', line) for line in lines]
        if all(len(row) == len(rows[0]) for row in rows):
            md = "| " + " | ".join(rows[0]) + " |\n"
            md += "|" + "|".join([" --- " for _ in rows[0]]) + "|\n"
            for row in rows[1:]:
                md += "| " + " | ".join(row) + " |\n"
            return md
        return f"[unstructured table]\n{text}"

    # ---------- 分段与结构化 ----------
    def _detect_section(self, text: str) -> Optional[str]:
        for section, pattern in CONFIG["section_patterns"].items():
            if re.match(pattern, text.strip()):
                return section
        return None

    def _mark_section(self, text: str) -> str:
        section = self._detect_section(text)
        if section:
            self.current_section = section
            return f"[section:{section}]\n{text}"
        return text

    # ---------- 图片处理（优化后） ----------
    def _sanitize_filename(self, name: str, max_len: int = 200) -> str:
        safe = re.sub(r'[^\w\s\-]', '', name)
        safe = re.sub(r'[\s]+', '_', safe)
        safe = safe[:max_len]
        return safe.strip('_')

    def _find_original_image(self, source: str) -> Optional[Path]:
        """在可能的位置查找原始图片文件"""
        candidates = [
            self.images_root / source,
            self.images_root / Path(source).name,
            self.images_root / "pages" / Path(source).name,
            self.images_root / "output" / Path(source).name,
        ]
        for cand in candidates:
            if cand.exists():
                return cand
        return None

    def _merge_caption_paragraphs(self, start_idx: int, page: int) -> str:
        """从 start_idx 开始，合并属于同一图注的所有连续段落，直到遇到下一个图注或标题

        与旧版不同，此方法直接遍历 self.kids 而非仅遍历 caption 列表，
        这样可以捕获紧跟在图注后面的普通 paragraph 和 list 元素。
        """
        full = self.kids[start_idx].get('content', '')
        for j in range(start_idx + 1, len(self.kids)):
            elem = self.kids[j]
            # 跨页停止
            if elem.get('page number') != page:
                break
            # 遇到标题停止
            if elem.get('type') == 'heading':
                break
            if elem.get('type') == 'paragraph':
                text = elem.get('content', '')
                # 遇到新的图注（以 Fig./Scheme/Table 开头），停止
                if re.match(CONFIG["caption_pattern"], text):
                    break
                # 否则合并（包括子图标记如 (A), (B) 等）
                full += " " + text
            elif elem.get('type') == 'list':
                # 列表项也可能属于图注（如子图说明），合并
                list_items = elem.get('list items', [])
                if isinstance(list_items, list):
                    for item in list_items:
                        if isinstance(item, dict):
                            full += " " + item.get('content', '')
                        elif isinstance(item, str):
                            full += " " + item
            elif elem.get('type') == 'caption':
                text = elem.get('content', '')
                if re.match(CONFIG["caption_pattern"], text):
                    break
                full += " " + text
            # image 类型不合并，但也不中断（跳过即可）
            elif elem.get('type') == 'image':
                continue
            # 其他未知类型跳过
            else:
                continue
        return full.strip()

    def _rename_image_from_file(self, img_path: Path, img_info: ImageInfo) -> Optional[str]:
        """从物理文件直接重命名，避免路径查找失败"""
        if not img_info.caption:
            print(f"[DEBUG] 图片 {img_path.name} 无 caption，使用默认名称")
            new_name = f"fig_{img_path.stem}_page{img_info.page}_uncaptioned{img_path.suffix}"
        else:
            caption_prefix = self._sanitize_filename(img_info.caption, 200)
            new_name = f"fig_{caption_prefix}{img_path.suffix}"
        new_path = self.renamed_dir / new_name
        try:
            shutil.copy2(img_path, new_path)
            self.renamed_count += 1
            print(f"[DEBUG] 重命名: {img_path.name} -> {new_name}")
            return str(new_path)
        except Exception as e:
            print(f"[DEBUG] 复制失败 {img_path.name}: {e}")
            return None

    def _extract_and_rename_images(self):
        """基于页面内顺序匹配图片与图注，然后重命名

        匹配策略（优先级从高到低）：
        1. linked_caption_id：JSON 中已明确关联的图注
        2. 页面内顺序匹配：同页图片与图注按出现顺序一一对应
        3. 跨页回退：若同页图注不足，向下一页查找未匹配的图注
        """
        from collections import defaultdict
        images_by_page = defaultdict(list)   # page -> list of (order_index, elem)
        captions_by_page = defaultdict(list) # page -> list of (order_index, elem)
        all_images = []  # 全部图片，保持原始顺序

        # 第一遍遍历：按页面分组收集图片和图注
        for idx, elem in enumerate(self.kids):
            page = elem.get('page number')
            if elem.get('type') == 'image':
                images_by_page[page].append((idx, elem))
                all_images.append((idx, elem))
            elif elem.get('type') in ('paragraph', 'caption'):
                text = elem.get('content', '')
                if re.match(CONFIG["caption_pattern"], text):
                    captions_by_page[page].append((idx, elem))

        # --- 调试信息：打印每页图片和图注数量 ---
        for page in sorted(set(list(images_by_page.keys()) + list(captions_by_page.keys()))):
            imgs = images_by_page.get(page, [])
            caps = captions_by_page.get(page, [])
            print(f"[DEBUG] Page {page}: {len(imgs)} images, {len(caps)} captions")
            for i, (oidx, img_elem) in enumerate(sorted(imgs, key=lambda x: x[0])):
                src = img_elem.get('source', '?')
                linked = img_elem.get('linked_caption_id')
                print(f"  Image[{i}] idx={oidx} src={src} linked_caption_id={linked}")
            for i, (oidx, cap_elem) in enumerate(sorted(caps, key=lambda x: x[0])):
                text = cap_elem.get('content', '')[:80]
                print(f"  Caption[{i}] idx={oidx}: {text}")

        # 第二遍：为每张图片匹配图注
        # caption_map: key=图片source规范化名, value=(full_caption_text, img_elem)
        caption_map = {}
        matched_caption_idxs = set()  # 已被匹配的图注索引，避免重复匹配

        # 优先级1：linked_caption_id（JSON 中明确关联的图注）
        for idx, img_elem in all_images:
            linked_id = img_elem.get('linked_caption_id')
            if linked_id is not None:
                # 在 kids 中查找对应图注元素
                for cap_idx, cap_elem in enumerate(self.kids):
                    if cap_elem.get('id') == linked_id and cap_idx not in matched_caption_idxs:
                        cap_text = self._merge_caption_paragraphs(cap_idx, cap_elem.get('page number', 0))
                        source = img_elem.get('source', '')
                        norm_name = Path(source).name
                        caption_map[norm_name] = (cap_text, img_elem)
                        matched_caption_idxs.add(cap_idx)
                        print(f"[DEBUG] linked匹配: {norm_name} -> {cap_text[:60]}")
                        break

        # 优先级2：页面内顺序匹配
        for page in sorted(images_by_page.keys()):
            imgs = sorted(images_by_page[page], key=lambda x: x[0])
            caps = sorted(captions_by_page.get(page, []), key=lambda x: x[0])

            # 过滤掉已被 linked 匹配的图注
            available_caps = [(cidx, celem) for cidx, celem in caps if cidx not in matched_caption_idxs]
            # 过滤掉已被 linked 匹配的图片
            available_imgs = []
            for iidx, ielem in imgs:
                source = ielem.get('source', '')
                norm_name = Path(source).name
                if norm_name not in caption_map:
                    available_imgs.append((iidx, ielem))

            for i, (iidx, img_elem) in enumerate(available_imgs):
                source = img_elem.get('source', '')
                norm_name = Path(source).name
                if i < len(available_caps):
                    cap_idx, cap_elem = available_caps[i]
                    cap_text = self._merge_caption_paragraphs(cap_idx, cap_elem.get('page number', page))
                    caption_map[norm_name] = (cap_text, img_elem)
                    matched_caption_idxs.add(cap_idx)
                    print(f"[DEBUG] 顺序匹配: {norm_name} -> {cap_text[:60]}")
                else:
                    # 优先级3：跨页回退 — 向下一页查找未匹配图注
                    found = False
                    for next_page in range(page + 1, page + 3):  # 最多看2页
                        next_caps = sorted(captions_by_page.get(next_page, []), key=lambda x: x[0])
                        next_available = [(cidx, celem) for cidx, celem in next_caps
                                          if cidx not in matched_caption_idxs]
                        if next_available:
                            cap_idx, cap_elem = next_available[0]
                            cap_text = self._merge_caption_paragraphs(cap_idx, cap_elem.get('page number', next_page))
                            caption_map[norm_name] = (cap_text, img_elem)
                            matched_caption_idxs.add(cap_idx)
                            print(f"[DEBUG] 跨页匹配(page {page}->{next_page}): {norm_name} -> {cap_text[:60]}")
                            found = True
                            break
                    if not found:
                        if norm_name not in caption_map:
                            caption_map[norm_name] = (None, img_elem)
                            print(f"[DEBUG] 无图注: {norm_name}")

        # 第三遍：扫描物理图片文件，对每个文件重命名
        if not self.images_root.exists():
            print(f"[DEBUG] 警告: 图片文件夹不存在 {self.images_root}")
            return

        processed_files = set()
        # 收集 JSON 中所有 image source 的规范化文件名，用于反向查找
        json_source_map = {}  # norm_name -> (caption_text, img_elem)
        for norm_name, (cap_text, img_elem) in caption_map.items():
            json_source_map[norm_name] = (cap_text, img_elem)

        for img_file in sorted(self.images_root.glob("**/*")):
            if not img_file.is_file():
                continue
            if img_file.suffix.lower() not in ('.png', '.jpg', '.jpeg', '.gif', '.bmp', '.tiff', '.webp'):
                continue

            norm_name = img_file.name
            if norm_name in processed_files:
                continue
            processed_files.add(norm_name)

            # 尝试多种方式匹配 caption
            caption_info = caption_map.get(norm_name)
            if not caption_info:
                # 尝试 stem 匹配（去掉扩展名）
                stem = img_file.stem
                for src_name, info in caption_map.items():
                    if Path(src_name).stem == stem:
                        caption_info = info
                        break
            if not caption_info:
                # 尝试在 JSON source 中模糊匹配（包含关系）
                for src_name, info in caption_map.items():
                    if stem in src_name or src_name.replace('.png', '') in stem:
                        caption_info = info
                        break

            caption = caption_info[0] if caption_info else None
            img_elem = caption_info[1] if caption_info else None

            img_info = ImageInfo(
                id=img_elem.get('id', 0) if img_elem else 0,
                page=img_elem.get('page number', 0) if img_elem else 0,
                original_source=str(img_file),
                bounding_box=img_elem.get('bounding_box', []) if img_elem else [],
                caption=caption
            )

            renamed_path = self._rename_image_from_file(img_file, img_info)
            if renamed_path:
                img_info.renamed_path = renamed_path
            self.images.append(img_info)

    # ---------- 结构化预提取（新增） ----------
    def _extract_chemical_with_disambiguation(self, text: str) -> Optional[str]:
        """提取化学式并结合上下文消歧
        
        策略:
        1. 找到所有化学式出现位置
        2. 检查每个位置的上下文
        3. 优先选择与"主要材料指标"一起出现的
        4. 过滤掉仅作为"例子"出现的
        """
        pre_config = CONFIG["pre_extraction"]
        formula_pattern = pre_config["chemical_formulas"]
        
        # 找到所有匹配
        matches = list(re.finditer(formula_pattern, text))
        if not matches:
            return None
        
        # 为每个匹配评分
        scored_formulas = []
        for match in matches:
            formula = match.group(0)
            start = max(0, match.start() - 100)
            end = min(len(text), match.end() + 100)
            context = text[start:end].lower()
            
            score = 0
            
            # 检查是否为主要材料指标附近
            for indicator in pre_config["main_material_indicators"]:
                if indicator.lower() in context:
                    score += 10
            
            # 检查是否有材料上下文关键词
            for keyword in pre_config["material_context_keywords"]:
                if keyword.lower() in context:
                    score += 5
            
            # 如果是作为例子出现，减分
            for example_indicator in pre_config["example_material_indicators"]:
                if example_indicator.lower() in context:
                    score -= 15
            
            # ZnO、TiO2等常见材料作为例子出现的概率更高，默认减分
            if formula in ['ZnO', 'TiO2']:
                score -= 5
            
            scored_formulas.append((formula, score, match.start()))
        
        # 选择得分最高的
        if scored_formulas:
            # 按分数降序，分数相同选第一次出现的
            best = max(scored_formulas, key=lambda x: (x[1], -x[2]))
            if best[1] > 0:  # 只有正分才返回
                print(f"[DEBUG] 材料消歧: 选择 '{best[0]}' (得分: {best[1]})")
                return best[0]
            else:
                print(f"[DEBUG] 材料消歧: 无高分候选，返回第一个")
        
        # 如果都没分，返回第一个
        return matches[0].group(0)
    
    def _pre_extract_structured_data(self, text: str) -> Dict[str, Any]:
        """预提取已知模式的字段，作为 LLM 提示中的候选值

        返回提取的候选信息字典，包含：
        - chemical_formula: 化学式（消歧后）
        - catalytic_activity: 酶活类型
        - Km_candidates: Km 候选值列表
        - Vmax_candidates: Vmax 候选值列表
        - pH_opt: 最佳 pH
        - T_opt: 最佳温度
        - surface_coating: 表面修饰
        - polymer: 聚合物/表面活性剂
        - substrates: 底物
        - dimensions: 尺寸信息
        """
        extracted = {}

        # 1. 化学式（带消歧）
        extracted['chemical_formula'] = self._extract_chemical_with_disambiguation(text)
        if 'chemical_formula' in extracted:
            print(f"[DEBUG] 预提取-化学式: {extracted['chemical_formula']}")

        # 2. 酶活类型
        for activity, pattern in CONFIG["pre_extraction"]["catalytic_activities"].items():
            if re.search(pattern, text, re.IGNORECASE):
                extracted['catalytic_activity'] = activity
                print(f"[DEBUG] 预提取-酶活类型: {activity}")
                break

        # 3. === 增强：Km 值的精确匹配 ===
        km_matches = re.findall(CONFIG["pre_extraction"]["km_pattern"], text)
        if km_matches:
            # 去重
            km_candidates = list(dict.fromkeys([f"{value} {unit}" for value, unit in km_matches]))
            extracted['Km_candidates'] = km_candidates
            print(f"[DEBUG] 预提取-Km候选: {km_candidates}")
        
        # 4. === 新增：Vmax 值的精确匹配 ===
        vmax_matches = re.findall(CONFIG["pre_extraction"]["vmax_pattern"], text)
        if vmax_matches:
            vmax_candidates = []
            for value, exp, unit in vmax_matches:
                vmax_str = f"{value}×10^{exp} {unit}"
                vmax_candidates.append(vmax_str)
            vmax_candidates = list(dict.fromkeys(vmax_candidates))
            extracted['Vmax_candidates'] = vmax_candidates
            print(f"[DEBUG] 预提取-Vmax候选: {vmax_candidates}")

        # 5. pH 最佳值
        ph_match = re.search(CONFIG["pre_extraction"]["ph_pattern"], text)
        if ph_match:
            extracted['pH_opt'] = float(ph_match.group(1))
            print(f"[DEBUG] 预提取-pH: {extracted['pH_opt']}")

        # 6. 温度（需结合上下文确认是否为最佳温度）
        temp_matches = re.findall(r'(\d+)\s*°?C', text)
        # 寻找与 optim 相关的温度
        for temp in temp_matches:
            temp_context_start = max(0, text.find(f'{temp} °C') - 30)
            temp_context_end = min(len(text), text.find(f'{temp} °C') + 30)
            temp_context = text[temp_context_start:temp_context_end].lower()
            if any(kw in temp_context for kw in ['optim', 'best', 'maximum', 'highest']):
                extracted['T_opt'] = int(temp)
                print(f"[DEBUG] 预提取-温度: {extracted['T_opt']}°C")
                break

        # 7. 表面修饰
        for coating in CONFIG["pre_extraction"]["surface_coatings"]:
            if coating.lower() in text.lower():
                extracted['surface_coating'] = coating
                print(f"[DEBUG] 预提取-表面修饰: {coating}")
                break

        # 8. 聚合物/表面活性剂
        for polymer in CONFIG["pre_extraction"]["polymers"]:
            if polymer.lower() in text.lower():
                extracted['polymer'] = polymer
                print(f"[DEBUG] 预提取-聚合物: {polymer}")
                break

        # 9. 底物
        detected_substrates = []
        for substrate in CONFIG["pre_extraction"]["substrates"]:
            if re.search(r'\b' + re.escape(substrate) + r'\b', text):
                detected_substrates.append(substrate)
        if detected_substrates:
            extracted['substrates'] = detected_substrates
            print(f"[DEBUG] 预提取-底物: {detected_substrates}")

        # 10. 尺寸信息（如 4.1 ± 0.3 nm）
        dimension_matches = re.findall(r'(\d+\.?\d*)\s*±\s*(\d+\.?\d*)\s*(nm|μm)', text)
        if dimension_matches:
            extracted['dimensions'] = [f"{v}±{e} {u}" for v, e, u in dimension_matches[:3]]
            print(f"[DEBUG] 预提取-尺寸: {extracted['dimensions']}")

        return extracted

    # ---------- 其他方法保持不变 ----------
    def _enhance_terms(self, text: str) -> str:
        for term in CONFIG["nanozyme_word_terms"]:
            pattern = r'\b(' + re.escape(term) + r')\b'
            text = re.sub(pattern, r'【\1】', text, flags=re.IGNORECASE)
        for term in CONFIG["nanozyme_char_terms"]:
            pattern = r'\b(' + re.escape(term) + r')\b'
            text = re.sub(pattern, r'【\1】', text)
        return text

    def _deduplicate_paragraphs(self, paragraphs: List[str]) -> List[str]:
        seen = set()
        out = []
        for p in paragraphs:
            words = re.findall(r'\w{3,}', p.lower())
            key = ' '.join(sorted(words[:15]))
            if key not in seen:
                seen.add(key)
                out.append(p)
        return out

    def _format_list(self, text: str) -> str:
        lines = text.split('\n')
        formatted = []
        for line in lines:
            if line.strip().startswith(('•', '-', '*', '·')):
                formatted.append(line)
            elif re.match(r'^\d+\.', line.strip()):
                formatted.append(line)
            else:
                formatted.append(f"- {line}")
        return '\n'.join(formatted)

    def _remove_references(self, text: str) -> str:
        # 匹配参考文献标题（单独一行）
        match = re.search(r'(?i)\n\s*(References|REFERENCES|Bibliography|Notes and references)\s*\n', text)
        if match:
            return text[:match.start()]
        return text

    def _chunk_text(self, text: str, section: str, page: int) -> List[TextChunk]:
        sentences = re.split(r'(?<=[.!?;])\s+', text)
        chunks = []
        current = ''
        overlap = CONFIG["chunk_overlap"]
        for s in sentences:
            if len(current) + len(s) + 2 < CONFIG["chunk_size"]:
                current += ' ' + s
            else:
                if current:
                    chunks.append(TextChunk(
                        content=current.strip(),
                        section=section,
                        page_start=page,
                        page_end=page,
                        char_count=len(current)
                    ))
                current = current[-overlap:] + ' ' + s if len(current) > overlap else s
        if current.strip():
            chunks.append(TextChunk(
                content=current.strip(),
                section=section,
                page_start=page,
                page_end=page,
                char_count=len(current)
            ))
        return chunks

    def _build_prompt_template(self, extracted_hints: Optional[Dict] = None) -> str:
        """构建 LLM 提示模板，可选注入预提取的候选值"""
        hint_text = ""
        if extracted_hints:
            hint_items = []
            for k, v in extracted_hints.items():
                if isinstance(v, list):
                    hint_items.append(f"- {k}: {', '.join(str(x) for x in v)}")
                else:
                    hint_items.append(f"- {k}: {v}")
            hint_text = "\n".join(hint_items)
            hint_section = f"""
以下是从文献中预提取的候选信息，请优先参考，若无匹配请忽略：
{hint_text}
"""
        else:
            hint_section = ""
        
        return f"""
你是纳米酶专业信息抽取专家。
请从以下文献文本中严格提取结构化信息，只输出JSON，不解释、不补充、不编造。
抽取字段包括：
- material：纳米酶材料名称
- metal_center：金属中心（如 Fe, Co, Cu）
- coordination：配位环境（如 N-C, S-C）
- enzyme_type：类酶活性类型（peroxidase-like, oxidase-like, catalase-like）
- Km：米氏常数（单位 mM 或 μM）
- Vmax：最大反应速率
- pH_opt：最佳 pH
- T_opt：最佳温度（°C）
- characterization：表征手段（TEM, XRD, XPS 等）
- table_data：表格数据（如有表格，转为Markdown格式）
{hint_section}
文本内容：
{{text}}
"""
    
    # ---------- RAG 相关上下文检索（新增） ----------
    def _calculate_relevance_score(self, chunk: str, keywords: List[str]) -> float:
        """计算文本块与关键词的相关性分数
        
        Args:
            chunk: 文本块
            keywords: 关键词列表
            
        Returns:
            相关性分数
        """
        score = 0.0
        chunk_lower = chunk.lower()
        
        # 关键词出现频率
        for keyword in keywords:
            count = chunk_lower.count(keyword.lower())
            score += count * 2.0
        
        # 纳米酶专业术语加权
        nanozyme_terms = ['nanozyme', 'Km', 'Vmax', 'catalytic', 'enzyme', 'kinetic']
        for term in nanozyme_terms:
            count = chunk_lower.count(term.lower())
            score += count * 3.0
        
        # 包含数据/结果的段落更重要
        if re.search(r'\d+\.?\d*\s*(mM|μM|nM|nm|°C)', chunk):
            score += 5.0
        
        return score
    
    def _select_relevant_chunks(self, top_k: int = 10) -> List[TextChunk]:
        """基于RAG思想选择最相关的文本块
        
        策略:
        1. 定义提取目标相关的关键词
        2. 为每个文本块计算相关性分数
        3. 选择top_k个最相关的块
        4. 保证包含摘要和实验部分
        
        Args:
            top_k: 选择的文本块数量
            
        Returns:
            筛选后的文本块列表
        """
        # 提取相关的关键词
        extraction_keywords = [
            'Km', 'Vmax', 'kinetic', 'catalytic', 'activity',
            'nanozyme', 'nanomaterial', 'synthesis', 'characterization',
            'TEM', 'XRD', 'XPS', 'peroxidase', 'oxidase',
            'substrate', 'TMB', 'H2O2', 'Michaelis-Menten'
        ]
        
        # 计算每个块的相关性
        scored_chunks = []
        for chunk in self.text_chunks:
            score = self._calculate_relevance_score(chunk.content, extraction_keywords)
            
            # 摘要和实验部分强制保留
            if chunk.section in ['abstract', 'experimental']:
                score += 100.0
            elif chunk.section == 'results':
                score += 10.0
            
            scored_chunks.append((chunk, score))
        
        # 按分数排序
        scored_chunks.sort(key=lambda x: x[1], reverse=True)
        
        # 选择top_k
        selected = [chunk for chunk, score in scored_chunks[:top_k]]
        
        # 按原始顺序重新排序（保持上下文连贯性）
        selected.sort(key=lambda c: (c.page_start, c.content[:50]))
        
        print(f"[DEBUG] RAG筛选: 从 {len(self.text_chunks)} 块中选择 {len(selected)} 块")
        print(f"[DEBUG] RAG筛选: 最高相关性分数 = {scored_chunks[0][1]:.2f}")
        
        return selected
    
    def _merge_high_value_chunks(self, chunks: List[TextChunk]) -> List[TextChunk]:
        """合并包含高价值信息的文本块
        
        策略：
        1. 识别每个块是否包含高价值信息（Km, Vmax, 化学式等）
        2. 将连续的高价值块合并成一个大块
        3. 保留必要的上下文（如章节标题）
        
        Args:
            chunks: 原始文本块列表
            
        Returns:
            合并后的文本块列表
        """
        # 高价值关键词
        HIGH_VALUE_TERMS = [
            'Km', 'Vmax', 'kinetic', 'catalytic', 'activity',
            'nanozyme', 'nanomaterial', 'synthesis', 'characterization',
            'TEM', 'XRD', 'XPS', 'peroxidase', 'oxidase',
            'substrate', 'TMB', 'H2O2', 'Michaelis-Menten'
        ]
        
        # 添加从预提取中得到的化学式等
        if self.extracted_hints.get('chemical_formula'):
            HIGH_VALUE_TERMS.append(self.extracted_hints['chemical_formula'])
        
        merged = []
        current_merged_content = ""
        current_merged_pages = set()
        current_section = ""
        high_value_block_encountered = False

        for chunk in chunks:
            # 检查此块是否包含高价值信息
            chunk_is_high_value = any(
                term.lower() in chunk.content.lower() 
                for term in HIGH_VALUE_TERMS 
                if term
            )
            
            if chunk_is_high_value:
                # 如果前一个块也是高价值，合并它们
                if high_value_block_encountered:
                    current_merged_content += "\n\n" + chunk.content
                    current_merged_pages.add(chunk.page_start)
                    current_merged_pages.add(chunk.page_end)
                else:
                    # 开始新的高价值合并块
                    if current_merged_content:
                        # 保存前一个非高价值块
                        merged.append(TextChunk(
                            content=current_merged_content,
                            section=current_section,
                            page_start=min(current_merged_pages) if current_merged_pages else chunk.page_start,
                            page_end=max(current_merged_pages) if current_merged_pages else chunk.page_end,
                            char_count=len(current_merged_content)
                        ))
                    current_merged_content = chunk.content
                    current_merged_pages = {chunk.page_start, chunk.page_end}
                    current_section = chunk.section
                high_value_block_encountered = True
            else:
                # 如果之前在合并高价值块，结束合并
                if high_value_block_encountered:
                    merged.append(TextChunk(
                        content=current_merged_content,
                        section="merged_high_value",
                        page_start=min(current_merged_pages),
                        page_end=max(current_merged_pages),
                        char_count=len(current_merged_content)
                    ))
                    current_merged_content = ""
                    current_merged_pages = set()
                    current_section = ""
                    high_value_block_encountered = False
                
                # 添加这个低价值块（也可以选择过滤掉）
                merged.append(chunk)

        # 不要忘记最后一个块如果是高价值
        if current_merged_content:
            merged.append(TextChunk(
                content=current_merged_content,
                section="merged_high_value",
                page_start=min(current_merged_pages),
                page_end=max(current_merged_pages),
                char_count=len(current_merged_content)
            ))

        return merged

    # ---------- 主流程 ----------
    def process(self):
        self._extract_and_rename_images()
        paragraphs_buffer = []
        last_page = 1
        
        for elem in self.kids:
            if elem.get('type') not in ('paragraph', 'heading', 'caption', 'list'):
                continue
            content = elem.get('content', '')
            if self._is_noise_line(content):
                continue
            
            if CONFIG["normalize_whitespace"]:
                content = self._normalize_whitespace(content)
            if CONFIG["fix_hyphenation"]:
                content = self._fix_hyphenation(content)
            
            if 'table' in content.lower() and ('|' not in content):
                content = self._fix_table(content)
            
            if elem.get('type') == 'heading':
                if paragraphs_buffer:
                    full = "\n".join(paragraphs_buffer)
                    full = self._deduplicate_paragraphs([full])[0]
                    self.sections[self.current_section].append(full)
                    paragraphs_buffer = []
                content = self._mark_section(content)
                self.sections[self.current_section].append(content)
                new_section = self._detect_section(content)
                if new_section:
                    self.current_section = new_section
            else:
                if elem.get('type') == 'list':
                    content = self._format_list(content)
                if content:
                    paragraphs_buffer.append(content)
            last_page = elem.get('page number', last_page)
        
        if paragraphs_buffer:
            full = "\n".join(paragraphs_buffer)
            full = self._deduplicate_paragraphs([full])[0]
            self.sections[self.current_section].append(full)
        
        combined_text = []
        for section, paras in self.sections.items():
            if not paras:
                continue
            section_text = "\n\n".join(paras)
            section_text = self._enhance_terms(section_text)
            combined_text.append(f"[section:{section}]\n{section_text}")
        
        full_markdown = "\n\n".join(combined_text)
        full_markdown = self._remove_references(full_markdown)
        self.text_chunks = self._chunk_text(full_markdown, "full", 1)
        
        # === 新增：在文本处理完成后进行结构化预提取 ===
        print("[DEBUG] 开始预提取结构化数据...")
        self.extracted_hints = self._pre_extract_structured_data(full_markdown)
        print(f"[DEBUG] 预提取完成，共提取 {len(self.extracted_hints)} 个字段")
        
        # === 新增：智能合并高价值文本块 ===
        print("[DEBUG] 开始智能合并高价值文本块...")
        self.merged_chunks = self._merge_high_value_chunks(self.text_chunks)
        print(f"[DEBUG] 合并完成，从 {len(self.text_chunks)} 个块合并为 {len(self.merged_chunks)} 个块")
        
        return self
    
    # ---------- 输出中间 JSON ----------
    def to_mid_json(self, save_path: Optional[str] = None, use_rag: bool = False, top_k: int = 10, use_merged: bool = True) -> Dict:
        """输出中间任务 JSON
        
        Args:
            save_path: 保存路径
            use_rag: 是否使用RAG筛选文本块
            top_k: RAG筛选时选择的文本块数量
            use_merged: 是否使用合并后的高价值文本块(默认True)
            
        Returns:
            mid_json: 中间任务 JSON
        """
        # 使用预提取结果构建提示模板
        prompt_template = self._build_prompt_template(self.extracted_hints if self.extracted_hints else None)
        
        # 选择文本块
        if use_rag:
            chunks = self._select_relevant_chunks(top_k=top_k)
            chunk_contents = [chunk.content for chunk in chunks]
            print(f"[DEBUG] 使用RAG筛选: {len(chunk_contents)} 个文本块")
        elif use_merged and hasattr(self, 'merged_chunks'):
            # 使用合并后的高价值块
            chunks = self.merged_chunks
            chunk_contents = [chunk.content for chunk in chunks]
            print(f"[DEBUG] 使用合并高价值块: {len(chunk_contents)} 个文本块 (原始: {len(self.text_chunks)})")
        else:
            chunks = self.text_chunks
            chunk_contents = [chunk.content for chunk in chunks]
            print(f"[DEBUG] 使用全部文本块: {len(chunk_contents)} 个")
        
        llm_task = {
            "prompt_template": prompt_template,
            "chunks": chunk_contents
        }
        vlm_tasks = []
        for img in self.images:
            vlm_tasks.append({
                "image_path": img.renamed_path if img.renamed_path else img.original_source,
                "caption": img.caption if img.caption else "",
                "page": img.page
            })
        table_tasks = []
        for section, paras in self.sections.items():
            for para in paras:
                if "[unstructured table]" in para or ("|" in para and "---" in para):
                    table_tasks.append({"section": section, "content": para})
        mid_json = {
            "metadata": {
                "file_name": self.data.get("file name"),
                "title": self.data.get("title"),
                "author": self.data.get("author"),
                "pages": self.data.get("number of pages"),
            },
            "extracted_hints": self.extracted_hints,  # === 新增：输出预提取结果 ===
            "llm_task": llm_task,
            "vlm_tasks": vlm_tasks,
            "table_tasks": table_tasks
        }
        if save_path:
            with open(save_path, 'w', encoding='utf-8') as f:
                json.dump(mid_json, f, indent=2, ensure_ascii=False)
        return mid_json

if __name__ == "__main__":
    pre = NanozymePreprocessor(
        json_path="c4ra15675g.json",
        images_root="./c4ra15675g_images",
        output_root="./output"
    )
    pre.process()
    mid = pre.to_mid_json("mid_task.json")
    print(json.dumps(mid, indent=2, ensure_ascii=False)[:1000])