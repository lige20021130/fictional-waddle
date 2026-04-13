import sys
print("Script started", flush=True)
sys.stdout.flush()
import os
import sys
import io
import re
import json
import csv
import subprocess
import threading
import time
import requests
import tkinter as tk
from tkinter import filedialog, scrolledtext, messagebox, ttk
from pathlib import Path
import yaml
import logging

# 导入处理层（假设在同一目录）
try:
    from nanozyme_preprocessor_midjson import NanozymePreprocessor
    PREPROCESSOR_AVAILABLE = True
except ImportError:
    PREPROCESSOR_AVAILABLE = False
    print("警告: 未找到 nanozyme_preprocessor_midjson 模块，预处理功能不可用")

class PDFBasicGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("纳米酶文献PDF转换工具 (基础版)")
        self.root.geometry("900x700")
        self.root.resizable(True, True)

        # 服务器进程
        self.server_process = None 
        self.server_port = 5002
        self.server_ready = False

        # 变量
        self.input_path = tk.StringVar()
        self.output_dir = tk.StringVar()
        self.recursive = tk.BooleanVar(value=False)
        self.force_ocr = tk.BooleanVar(value=False)
        self.ocr_lang = tk.StringVar(value="default")
        self.enrich_formula = tk.BooleanVar(value=False)
        self.enrich_picture = tk.BooleanVar(value=False)
        self.hybrid_mode = tk.StringVar(value="docling-fast")
        self.enable_preprocessor = tk.BooleanVar(value=True)  # 新增：是否启用处理层
        self.mid_json_output_dir = tk.StringVar()  # 后处理 JSON 输出路径
        self.extracted_json_output_dir = tk.StringVar()  # 大模型提取 JSON 输出路径

        self.mid_json_path = None  # 最近生成的 mid_task.json 路径
        self.extracted_json_path = None  # 最近生成的提取结果路径
        self.extract_stop_flag = False  # 提取停止标志
        
        # 大模型配置信息
        self.llm_config = None
        self.vlm_config = None

        self.create_widgets()
        self.log_queue = []
        self.update_log()
        
        # 加载大模型配置(在 widgets 创建后)
        self.load_model_config()
        
        # 设置日志处理器,将 logging 日志转发到 GUI
        self.setup_logging_handler()

    def create_widgets(self):
        # 大模型配置信息展示
        model_frame = tk.LabelFrame(self.root, text="🤖 大模型配置", padx=5, pady=5)
        model_frame.pack(fill="x", padx=10, pady=5)
        
        # 文本 LLM 配置
        text_llm_frame = tk.Frame(model_frame)
        text_llm_frame.pack(fill="x", pady=2)
        tk.Label(text_llm_frame, text="文本提取:", font=('Arial', 9, 'bold'), width=10, anchor='w').pack(side='left')
        self.text_llm_label = tk.Label(text_llm_frame, text="加载中...", fg="gray", anchor='w')
        self.text_llm_label.pack(side='left', padx=5, fill='x', expand=True)
        
        # 视觉 VLM 配置
        vlm_frame = tk.Frame(model_frame)
        vlm_frame.pack(fill="x", pady=2)
        tk.Label(vlm_frame, text="图像分析:", font=('Arial', 9, 'bold'), width=10, anchor='w').pack(side='left')
        self.vlm_label = tk.Label(vlm_frame, text="加载中...", fg="gray", anchor='w')
        self.vlm_label.pack(side='left', padx=5, fill='x', expand=True)
        
        # 刷新按钮和测试按钮
        btn_frame_model = tk.Frame(model_frame)
        btn_frame_model.pack(side='right', padx=5)
        tk.Button(btn_frame_model, text="测试连接", command=self.test_model_connection, width=10, bg="lightblue").pack(side='left', padx=2)
        tk.Button(btn_frame_model, text="刷新配置", command=self.load_model_config, width=10).pack(side='left', padx=2)

        # 服务器控制
        server_frame = tk.LabelFrame(self.root, text="AI 后端服务器", padx=5, pady=5)
        server_frame.pack(fill="x", padx=10, pady=5)
        btn_start_server = tk.Button(server_frame, text="启动服务器", command=self.start_server, bg="lightgreen")
        btn_start_server.pack(side="left", padx=5)
        btn_stop_server = tk.Button(server_frame, text="停止服务器", command=self.stop_server, bg="lightcoral")
        btn_stop_server.pack(side="left", padx=5)
        self.server_status = tk.Label(server_frame, text="状态: 未启动", fg="red")
        self.server_status.pack(side="left", padx=20)

        # 输入设置
        input_frame = tk.LabelFrame(self.root, text="输入设置", padx=5, pady=5)
        input_frame.pack(fill="x", padx=10, pady=5)
        tk.Label(input_frame, text="PDF文件或文件夹:").grid(row=0, column=0, sticky="w")
        tk.Entry(input_frame, textvariable=self.input_path, width=60).grid(row=0, column=1, padx=5)
        tk.Button(input_frame, text="选择文件", command=self.select_files).grid(row=0, column=2, padx=2)
        tk.Button(input_frame, text="选择文件夹", command=self.select_folder).grid(row=0, column=3, padx=2)
        tk.Checkbutton(input_frame, text="递归处理子文件夹", variable=self.recursive).grid(row=1, column=1, sticky="w", pady=5)
        tk.Label(input_frame, text="输出目录 (可选):").grid(row=2, column=0, sticky="w")
        tk.Entry(input_frame, textvariable=self.output_dir, width=60).grid(row=2, column=1, padx=5)
        tk.Button(input_frame, text="选择目录", command=self.select_output_dir).grid(row=2, column=2, padx=2)

        # 处理模式
        mode_frame = tk.LabelFrame(self.root, text="处理模式", padx=5, pady=5)
        mode_frame.pack(fill="x", padx=10, pady=5)
        ocr_frame = tk.Frame(mode_frame)
        ocr_frame.pack(anchor="w", fill="x", pady=2)
        tk.Checkbutton(ocr_frame, text="强制OCR (扫描件/图片型PDF)", variable=self.force_ocr).pack(side="left")
        tk.Label(ocr_frame, text="OCR语言 (如 ko,en):").pack(side="left", padx=(20,5))
        tk.Entry(ocr_frame, textvariable=self.ocr_lang, width=15).pack(side="left")
        tk.Label(ocr_frame, text="  (扫描版PDF务必开启OCR)", fg="blue").pack(side="left", padx=10)

        enhance_frame = tk.Frame(mode_frame)
        enhance_frame.pack(anchor="w", fill="x", pady=2)
        tk.Checkbutton(enhance_frame, text="提取数学公式", variable=self.enrich_formula).pack(side="left")
        tk.Checkbutton(enhance_frame, text="生成图表描述", variable=self.enrich_picture).pack(side="left", padx=20)

        hybrid_frame = tk.Frame(mode_frame)
        hybrid_frame.pack(anchor="w", fill="x", pady=2)
        tk.Label(hybrid_frame, text="混合模式:").pack(side="left")
        tk.Radiobutton(hybrid_frame, text="docling-fast (推荐)", variable=self.hybrid_mode, value="docling-fast").pack(side="left", padx=5)
        tk.Radiobutton(hybrid_frame, text="full (完整AI)", variable=self.hybrid_mode, value="full").pack(side="left", padx=5)

        # 新增：预处理选项
        pre_frame = tk.LabelFrame(self.root, text="后处理 (生成 LLM/VLM 任务 JSON)", padx=5, pady=5)
        pre_frame.pack(fill="x", padx=10, pady=5)
        tk.Checkbutton(pre_frame, text="启用预处理层 (生成中间任务 JSON)", variable=self.enable_preprocessor).pack(anchor="w")
        
        # 后处理 JSON 输出路径
        mid_json_frame = tk.Frame(pre_frame)
        mid_json_frame.pack(fill="x", pady=5)
        tk.Label(mid_json_frame, text="中间 JSON 输出:").pack(side="left", padx=5)
        tk.Entry(mid_json_frame, textvariable=self.mid_json_output_dir, width=50).pack(side="left", padx=5)
        tk.Button(mid_json_frame, text="选择目录", command=self.select_mid_json_output).pack(side="left", padx=2)
        tk.Label(mid_json_frame, text="(默认: PDF同级目录)", fg="gray").pack(side="left", padx=5)
        
        if not PREPROCESSOR_AVAILABLE:
            tk.Label(pre_frame, text="⚠ 预处理模块未找到，请确保 nanozyme_preprocessor_midjson.py 在同一目录", fg="red").pack(anchor="w")

        # === 大模型提取区域 ===
        extract_frame = tk.LabelFrame(self.root, text="🤖 大模型提取 (LLM/VLM)", padx=5, pady=5)
        extract_frame.pack(fill="x", padx=10, pady=5)
        
        # 大模型提取 JSON 输出路径
        ext_json_frame = tk.Frame(extract_frame)
        ext_json_frame.pack(fill="x", pady=5)
        tk.Label(ext_json_frame, text="提取结果输出:").pack(side="left", padx=5)
        tk.Entry(ext_json_frame, textvariable=self.extracted_json_output_dir, width=50).pack(side="left", padx=5)
        tk.Button(ext_json_frame, text="选择目录", command=self.select_extracted_json_output).pack(side="left", padx=2)
        tk.Label(ext_json_frame, text="(默认: ./extraction_results)", fg="gray").pack(side="left", padx=5)

        extract_btn_frame = tk.Frame(extract_frame)
        extract_btn_frame.pack(fill="x", pady=2)

        self.extract_btn = tk.Button(extract_btn_frame, text="启动提取", command=self.start_extraction,
                                     bg="orange", fg="white", state=tk.DISABLED)
        self.extract_btn.pack(side="left", padx=5)
        
        self.stop_extract_btn = tk.Button(extract_btn_frame, text="停止提取", command=self.stop_extraction,
                                          bg="red", fg="white", state=tk.DISABLED)
        self.stop_extract_btn.pack(side="left", padx=5)

        self.view_result_btn = tk.Button(extract_btn_frame, text="查看结果", command=self.view_result,
                                         state=tk.DISABLED)
        self.view_result_btn.pack(side="left", padx=5)

        self.extract_status = tk.Label(extract_frame, text="状态: 等待预处理完成", fg="gray")
        self.extract_status.pack(anchor="w", padx=5)

        self.extract_progress = ttk.Progressbar(extract_frame, mode='determinate')
        self.extract_progress.pack(fill="x", padx=5, pady=5)

        # 控制按钮
        btn_frame = tk.Frame(self.root)
        btn_frame.pack(fill="x", padx=10, pady=5)
        self.start_btn = tk.Button(btn_frame, text="开始转换", command=self.start_conversion, bg="green", fg="white")
        self.start_btn.pack(side="left", padx=5)
        self.stop_btn = tk.Button(btn_frame, text="停止", command=self.stop_conversion, state=tk.DISABLED, bg="red", fg="white")
        self.stop_btn.pack(side="left", padx=5)

        self.progress = ttk.Progressbar(self.root, mode='indeterminate')
        self.progress.pack(fill="x", padx=10, pady=5)

        log_frame = tk.LabelFrame(self.root, text="转换日志", padx=5, pady=5)
        log_frame.pack(fill="both", expand=True, padx=10, pady=5)
        self.log_text = scrolledtext.ScrolledText(log_frame, height=15, wrap=tk.WORD)
        self.log_text.pack(fill="both", expand=True)

        self.status_var = tk.StringVar(value="就绪")
        status_bar = tk.Label(self.root, textvariable=self.status_var, bd=1, relief=tk.SUNKEN, anchor=tk.W)
        status_bar.pack(side=tk.BOTTOM, fill=tk.X)

    def load_model_config(self):
        """加载并显示大模型配置信息"""
        try:
            config_path = Path("config.yaml")
            if not config_path.exists():
                self.text_llm_label.config(text="配置文件不存在", fg="red")
                self.vlm_label.config(text="配置文件不存在", fg="red")
                return
            
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            
            self.llm_config = config.get('text_llm', {})
            self.vlm_config = config.get('vision_vlm', {})
            
            # 提取关键信息
            llm_model = self.llm_config.get('model', '未配置')
            llm_url = self.llm_config.get('base_url', '')
            llm_api_set = '✓' if self.llm_config.get('api_key') and self.llm_config['api_key'] not in ['your-deepseek-api-key', 'your-key', ''] else '✗'
            
            vlm_model = self.vlm_config.get('model', '未配置')
            vlm_url = self.vlm_config.get('base_url', '')
            vlm_api_set = '✓' if self.vlm_config.get('api_key') and self.vlm_config['api_key'] not in ['your-openai-api-key', 'your-key', ''] else '✗'
            
            # 更新显示 - 只显示配置信息,不表示连通性
            self.text_llm_label.config(
                text=f"模型: {llm_model} | API: {llm_url} | 密钥: {llm_api_set} (点击测试连接)",
                fg="blue" if llm_api_set == '✓' else "red"
            )
            self.vlm_label.config(
                text=f"模型: {vlm_model} | API: {vlm_url} | 密钥: {vlm_api_set} (点击测试连接)",
                fg="blue" if vlm_api_set == '✓' else "red"
            )
            
            self.log("[配置] 大模型配置已加载,请点击'测试连接'验证连通性")
            
        except Exception as e:
            self.text_llm_label.config(text=f"加载失败: {str(e)}", fg="red")
            self.vlm_label.config(text=f"加载失败: {str(e)}", fg="red")
            self.log(f"[配置] 加载大模型配置失败: {e}")

    def test_model_connection(self):
        """测试大模型API连通性"""
        if not self.llm_config or not self.vlm_config:
            messagebox.showwarning("提示", "请先加载配置")
            return
        
        self.log("[连接测试] 开始测试大模型连通性...")
        self.text_llm_label.config(text="测试中...", fg="orange")
        self.vlm_label.config(text="测试中...", fg="orange")
        
        # 在后台线程测试
        def test_worker():
            import asyncio
            
            async def test_both():
                from api_client_v2 import APIClient
                results = {'text': None, 'vision': None}
                
                try:
                    async with APIClient() as client:
                        # 测试文本 LLM
                        self.log("[连接测试] 测试文本 LLM...")
                        results['text'] = await client.test_connection('text')
                        
                        # 测试视觉 VLM
                        self.log("[连接测试] 测试视觉 VLM...")
                        results['vision'] = await client.test_connection('vision')
                except Exception as e:
                    self.log(f"[连接测试] 测试失败: {e}")
                    import traceback
                    self.log(traceback.format_exc())
                
                return results
            
            try:
                results = asyncio.run(test_both())
                
                # 更新 UI
                def update_ui():
                    if results.get('text'):
                        if results['text']['success']:
                            llm_model = self.llm_config.get('model', '')
                            self.text_llm_label.config(
                                text=f"✓ {llm_model} - {results['text']['message']}",
                                fg="green"
                            )
                            self.log(f"[连接测试] 文本 LLM: {results['text']['message']}")
                        else:
                            self.text_llm_label.config(
                                text=f"✗ {results['text']['message']}",
                                fg="red"
                            )
                            self.log(f"[连接测试] 文本 LLM 失败: {results['text']['message']}")
                    
                    if results.get('vision'):
                        if results['vision']['success']:
                            vlm_model = self.vlm_config.get('model', '')
                            self.vlm_label.config(
                                text=f"✓ {vlm_model} - {results['vision']['message']}",
                                fg="green"
                            )
                            self.log(f"[连接测试] 视觉 VLM: {results['vision']['message']}")
                        else:
                            self.vlm_label.config(
                                text=f"✗ {results['vision']['message']}",
                                fg="red"
                            )
                            self.log(f"[连接测试] 视觉 VLM 失败: {results['vision']['message']}")
                
                self.root.after(0, update_ui)
                
            except Exception as e:
                self.log(f"[连接测试] 异常: {e}")
                import traceback
                self.log(traceback.format_exc())
        
        threading.Thread(target=test_worker, daemon=True).start()

    def setup_logging_handler(self):
        """设置自定义日志处理器,将 logging 输出到 GUI"""
        class GUILogHandler(logging.Handler):
            def __init__(self, gui_instance):
                super().__init__()
                self.gui = gui_instance
                
            def emit(self, record):
                log_msg = self.format(record)
                # 根据日志级别添加前缀
                if record.levelno >= logging.ERROR:
                    prefix = "[ERROR]"
                elif record.levelno >= logging.WARNING:
                    prefix = "[WARN]"
                elif record.levelno >= logging.INFO:
                    prefix = "[INFO]"
                else:
                    prefix = "[DEBUG]"
                self.gui.log(f"{prefix} {log_msg}")
        
        # 创建并配置处理器
        handler = GUILogHandler(self)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(message)s')
        handler.setFormatter(formatter)
        
        # 添加到根日志器
        root_logger = logging.getLogger()
        root_logger.addHandler(handler)
        root_logger.setLevel(logging.INFO)
        
        # 为关键模块设置更详细的日志
        logging.getLogger('extraction_pipeline').setLevel(logging.INFO)
        logging.getLogger('llm_extractor').setLevel(logging.INFO)
        logging.getLogger('vlm_extractor').setLevel(logging.INFO)
        logging.getLogger('api_client_v2').setLevel(logging.INFO)
        
        self.log("[系统] 日志系统已初始化")

    def log(self, msg):
        self.log_queue.append(msg + "\n")

    def update_log(self):
        if self.log_queue:
            self.log_text.insert(tk.END, "".join(self.log_queue))
            self.log_queue.clear()
            self.log_text.see(tk.END)
        self.root.after(100, self.update_log)

    def select_files(self):
        files = filedialog.askopenfilenames(filetypes=[("PDF files", "*.pdf")])
        if files:
            self.input_path.set(";".join(files))

    def select_folder(self):
        folder = filedialog.askdirectory()
        if folder:
            self.input_path.set(folder)

    def select_output_dir(self):
        folder = filedialog.askdirectory()
        if folder:
            self.output_dir.set(folder)
    
    def select_mid_json_output(self):
        """选择后处理 JSON 输出目录"""
        folder = filedialog.askdirectory(title="选择中间 JSON 输出目录")
        if folder:
            self.mid_json_output_dir.set(folder)
            self.log(f"[配置] 中间 JSON 输出目录: {folder}")
    
    def select_extracted_json_output(self):
        """选择大模型提取 JSON 输出目录"""
        folder = filedialog.askdirectory(title="选择提取结果输出目录")
        if folder:
            self.extracted_json_output_dir.set(folder)
            self.log(f"[配置] 提取结果输出目录: {folder}")

    def start_server(self):
        if self.server_process and self.server_process.poll() is None:
            messagebox.showinfo("提示", "服务器已在运行中")
            return

        cmd = ["opendataloader-pdf-hybrid", f"--port={self.server_port}"]
        if self.force_ocr.get():
            cmd.append("--force-ocr")
        if self.ocr_lang.get().strip() and self.ocr_lang.get() != "default":
            cmd.append(f"--ocr-lang={self.ocr_lang.get()}")
        if self.enrich_formula.get():
            cmd.append("--enrich-formula")
        if self.enrich_picture.get():
            cmd.append("--enrich-picture-description")

        self.log(f"启动服务器: {' '.join(cmd)}")
        try:
            env = os.environ.copy()
            env["PYTHONIOENCODING"] = "gbk"
            env["JAVA_TOOL_OPTIONS"] = "-Dfile.encoding=UTF-8"
            env["HF_HUB_OFFLINE"] = "1"
            self.server_process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding='gbk',
                errors='replace',
                env=env
            )
            def monitor():
                for line in self.server_process.stdout:
                    self.log(f"[服务器] {line.strip()}")
                    if "Uvicorn running on" in line:
                        self.server_ready = True
                        self.root.after(0, lambda: self.server_status.config(text="状态: 运行中", fg="green"))
                self.log("服务器进程已退出")
                self.server_ready = False
                self.root.after(0, lambda: self.server_status.config(text="状态: 已停止", fg="red"))
            threading.Thread(target=monitor, daemon=True).start()
            for _ in range(30):
                time.sleep(0.5)
                if self.server_ready:
                    break
            if not self.server_ready:
                self.log("警告: 服务器可能未完全启动，请稍后手动检查")
        except Exception as e:
            self.log(f"启动服务器失败: {e}")
            self.server_process = None
            self.server_ready = False
            self.server_status.config(text="状态: 启动失败", fg="red")
            messagebox.showerror("错误", f"无法启动服务器:\n{e}")

    def stop_server(self):
        if self.server_process and self.server_process.poll() is None:
            self.server_process.terminate()
            self.log("已发送终止信号")
            self.server_status.config(text="状态: 停止中", fg="orange")
        else:
            self.log("服务器未运行")

    def start_conversion(self):
        input_path = self.input_path.get().strip()
        if not input_path:
            messagebox.showerror("错误", "请选择 PDF 文件或文件夹")
            return
        if not self.server_process or self.server_process.poll() is not None:
            if messagebox.askyesno("提示", "AI 后端未启动，是否自动启动？"):
                self.start_server()
                self.root.after(5000, self._do_conversion)
                return
            else:
                return
        self._do_conversion()

    def _do_conversion(self):
        self.start_btn.config(state=tk.DISABLED)
        self.stop_btn.config(state=tk.NORMAL)
        self.progress.start(10)
        self.stop_flag = False
        self.convert_thread = threading.Thread(target=self.convert_worker, daemon=True)
        self.convert_thread.start()

    def convert_worker(self):
        try:
            input_path = self.input_path.get().strip()
            paths = [p.strip() for p in input_path.split(";") if p.strip()]
            total = len(paths)

            for idx, p in enumerate(paths, 1):
                if self.stop_flag:
                    self.log("用户中断转换")
                    break

                self.status_var.set(f"正在处理 ({idx}/{total}): {os.path.basename(p)}")
                self.log(f"[{idx}/{total}] 开始转换: {p}")

                # 构建解析命令
                cmd = ["opendataloader-pdf", "--hybrid", self.hybrid_mode.get()]
                if self.output_dir.get():
                    cmd.append("--output"); cmd.append(self.output_dir.get())
                if self.recursive.get() and os.path.isdir(p):
                    cmd.append("--recursive")
                cmd.append(p)

                env = os.environ.copy()
                env["PYTHONIOENCODING"] = "gbk"
                env["JAVA_TOOL_OPTIONS"] = "-Dfile.encoding=UTF-8"

                try:
                    self.current_proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                            text=True, encoding='gbk', errors='replace', env=env)
                    for line in self.current_proc.stdout:
                        self.log(f"[转换] {line.strip()}")
                        if self.stop_flag:
                            self.current_proc.terminate()
                            self.log(f"已终止进程: {p}")
                            break
                    if not self.stop_flag:
                        self.current_proc.wait()
                        if self.current_proc.returncode == 0:
                            self.log(f"✓ 转换完成: {p}")
                            # 调用处理层（如果启用且模块可用）
                            if self.enable_preprocessor.get() and PREPROCESSOR_AVAILABLE:
                                self._run_preprocessor(p)
                            elif self.enable_preprocessor.get() and not PREPROCESSOR_AVAILABLE:
                                self.log("预处理模块不可用，跳过")
                        else:
                            self.log(f"✗ 转换失败 (返回码 {self.current_proc.returncode}): {p}")
                    self.current_proc = None
                except Exception as e:
                    self.log(f"✗ 转换异常: {p}\n   {str(e)}")
                    self.current_proc = None

            if not self.stop_flag:
                self.log("\n所有任务处理完毕！")
                self.status_var.set("完成")
            else:
                self.status_var.set("已停止")
        except Exception as e:
            self.log(f"发生严重错误: {str(e)}")
        finally:
            self.root.after(0, self.conversion_finished)

    def _run_preprocessor(self, pdf_path: str):
        """根据解析输出调用处理层，生成中间任务 JSON"""
        pdf_path = Path(pdf_path)
        output_dir = self.output_dir.get().strip()
        if output_dir:
            out_dir = Path(output_dir)
        else:
            out_dir = pdf_path.parent
        
        # 使用后处理 JSON 自定义输出路径(如果设置了)
        mid_json_dir = self.mid_json_output_dir.get().strip()
        if mid_json_dir:
            mid_json_dir = Path(mid_json_dir)
            mid_json_dir.mkdir(parents=True, exist_ok=True)
        else:
            mid_json_dir = out_dir

        json_path = out_dir / (pdf_path.stem + ".json")
        images_dir = out_dir / (pdf_path.stem + "_images")

        if not json_path.exists():
            self.log(f"警告: 找不到 JSON 文件 {json_path}，跳过预处理")
            return
        if not images_dir.exists():
            self.log(f"警告: 找不到图片文件夹 {images_dir}，继续但可能缺少图片")

        try:
            pre = NanozymePreprocessor(
                json_path=str(json_path),
                images_root=str(images_dir) if images_dir.exists() else None,
                output_root=str(out_dir)
            )
            
            # 重定向 stdout 捕获处理层日志
            old_stdout = sys.stdout
            sys.stdout = captured_output = io.StringIO()
            
            try:
                pre.process()
                mid_json_path = mid_json_dir / f"{pdf_path.stem}_mid_task.json"
                pre.to_mid_json(str(mid_json_path))
            finally:
                sys.stdout = old_stdout  # 确保恢复
            
            # 将捕获的日志输出到 GUI
            captured_text = captured_output.getvalue()
            if captured_text.strip():
                for line in captured_text.strip().split('\n'):
                    self.log(f"[预处理] {line}")
            
            renamed_info = f"，重命名图片 {pre.renamed_count} 张" if pre.renamed_count > 0 else ""
            self.log(f"✓ 预处理完成{renamed_info}，中间任务 JSON: {mid_json_path}")

            self.mid_json_path = str(mid_json_path)
            self.root.after(0, lambda: self.extract_btn.config(state=tk.NORMAL))
            self.root.after(0, lambda: self.extract_status.config(text="状态: 已就绪，可启动大模型提取", fg="green"))
                
        except Exception as e:
            # 确保异常时也恢复 stdout
            sys.stdout = old_stdout if 'old_stdout' in dir() else sys.stdout
            import traceback
            self.log(f"✗ 预处理失败: {e}")
            self.log(f"详细错误: {traceback.format_exc()}")

    def conversion_finished(self):
        self.progress.stop()
        self.start_btn.config(state=tk.NORMAL)
        self.stop_btn.config(state=tk.DISABLED)

    def stop_conversion(self):
        self.stop_flag = True
        if hasattr(self, 'current_proc') and self.current_proc:
            try:
                self.current_proc.terminate()
                self.log("已终止当前转换进程")
            except Exception as e:
                self.log(f"终止进程时出错: {e}")
        self.log("正在停止... 请稍候")
        self.stop_btn.config(state=tk.DISABLED)

    def start_extraction(self):
        """启动大模型提取"""
        if not self.mid_json_path or not Path(self.mid_json_path).exists():
            messagebox.showerror("错误", "未找到 mid_task.json，请先完成预处理")
            return

        # 检查 config.yaml 是否存在
        if not Path("config.yaml").exists():
            messagebox.showwarning("配置缺失",
                "未找到 config.yaml 配置文件。\n"
                "请先在程序目录创建该文件并填入 API 密钥后重试。")
            return

        self.extract_stop_flag = False  # 重置停止标志
        self.extract_btn.config(state=tk.DISABLED)
        self.stop_extract_btn.config(state=tk.NORMAL)  # 启用停止按钮
        self.extract_progress['value'] = 0
        self.extract_status.config(text="状态: 正在提取...", fg="blue")

        self.extract_thread = threading.Thread(target=self.extract_worker, daemon=True)
        self.extract_thread.start()
    
    def stop_extraction(self):
        """停止大模型提取"""
        if messagebox.askyesno("确认停止", "确定要停止当前提取任务吗?\n已处理的数据将不会保存。"):
            self.extract_stop_flag = True
            self.stop_extract_btn.config(state=tk.DISABLED)
            self.log("[提取] 用户请求停止提取...")
            self.extract_status.config(text="状态: 正在停止...", fg="orange")

    def extract_worker(self):
        """后台提取工作线程"""
        try:
            # 确保当前脚本所在目录在sys.path中
            script_dir = os.path.dirname(os.path.abspath(__file__))
            if script_dir not in sys.path:
                sys.path.insert(0, script_dir)
            
            from extraction_pipeline import ExtractionPipeline
            
            # 获取自定义输出路径
            custom_output_dir = self.extracted_json_output_dir.get().strip()
            if custom_output_dir:
                self.log(f"[提取] 使用自定义输出目录: {custom_output_dir}")
            else:
                self.log(f"[提取] 使用默认输出目录 (extraction_results)")

            def progress_callback(msg, percent=None):
                # 检查停止标志
                if self.extract_stop_flag:
                    raise KeyboardInterrupt("用户停止提取")
                self.root.after(0, lambda m=msg, p=percent: self.update_extract_progress(m, p))

            pipeline = ExtractionPipeline(
                output_dir=custom_output_dir if custom_output_dir else None
            )
            out_path = pipeline.process_mid_json_sync(
                self.mid_json_path,
                progress_callback=progress_callback
            )
            self.extracted_json_path = out_path
            # 修复: 使用默认参数捕获out_path
            self.root.after(0, lambda p=out_path: self.extraction_finished(p))
        except KeyboardInterrupt:
            self.root.after(0, self.extraction_stopped)
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            # 修复: 使用默认参数捕获e和tb,避免闭包变量问题
            self.root.after(0, lambda err=str(e), traceback=tb: self.extraction_error(err, traceback))

    def update_extract_progress(self, msg, percent=None):
        """更新提取进度(GUI线程调用)"""
        self.extract_status.config(text=f"状态: {msg}")
        if percent is not None:
            self.extract_progress['value'] = percent
        self.log(f"[提取进度] {msg} (进度: {percent}%)")

    def extraction_finished(self, out_path):
        """提取完成回调(GUI线程调用)"""
        self.extract_progress['value'] = 100
        self.extract_status.config(text="状态: 提取完成", fg="green")
        self.extract_btn.config(state=tk.NORMAL)
        self.stop_extract_btn.config(state=tk.DISABLED)
        self.view_result_btn.config(state=tk.NORMAL)
        self.log("[提取] ===== 大模型提取流程完成 =====")
        self.log(f"[提取] 结果保存至: {out_path}")
        messagebox.showinfo("提取完成", f"结果已保存至:\n{out_path}")
    
    def extraction_stopped(self):
        """提取被停止回调(GUI线程调用)"""
        self.extract_progress['value'] = 0
        self.extract_status.config(text="状态: 已停止", fg="orange")
        self.extract_btn.config(state=tk.NORMAL)
        self.stop_extract_btn.config(state=tk.DISABLED)
        self.log("[提取] ===== 大模型提取已停止 =====")
        self.log("[提取] 用户手动停止提取,结果未保存")

    def extraction_error(self, error_msg, traceback_text):
        """提取失败回调(GUI线程调用)"""
        self.extract_status.config(text="状态: 提取失败", fg="red")
        self.extract_btn.config(state=tk.NORMAL)
        self.log("[提取] ===== 大模型提取流程失败 =====")
        self.log(f"[提取] 错误信息: {error_msg}")
        self.log(f"[提取] 详细堆栈:\n{traceback_text}")
        messagebox.showerror("提取错误", f"提取过程发生错误:\n{error_msg}")

    def view_result(self):
        """查看提取结果"""
        if not self.extracted_json_path or not Path(self.extracted_json_path).exists():
            messagebox.showwarning("提示", "请先完成提取")
            return

        with open(self.extracted_json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        dialog = ResultReviewDialog(self.root, data, self.extracted_json_path,
                                    on_feedback=self.on_feedback_received)
        self.root.wait_window(dialog.top)

    def on_feedback_received(self, corrections):
        """接收人工修正反馈"""
        from rule_learner import RuleLearner
        rl = RuleLearner("rulebook.json")
        for field, new_val in corrections.items():
            rl.learn_from_correction(field, None, new_val)
        self.log(f"✓ 已记录 {len(corrections)} 条人工修正反馈")


class ResultReviewDialog:
    def __init__(self, parent, data, file_path, on_feedback=None):
        self.data = data
        self.file_path = file_path
        self.on_feedback = on_feedback
        self.corrections = {}

        self.top = tk.Toplevel(parent)
        self.top.title("提取结果审核")
        self.top.geometry("600x500")
        self.top.resizable(True, True)

        self.create_widgets()

    def create_widgets(self):
        # 标题
        meta = self.data.get('metadata', {})
        title_text = f"文献: {meta.get('title', '未知')[:50]}..."
        tk.Label(self.top, text=title_text, font=('Arial', 12, 'bold')).pack(pady=10)

        # 字段列表区域 (带滚动条)
        frame = tk.Frame(self.top)
        frame.pack(fill="both", expand=True, padx=10, pady=5)

        canvas = tk.Canvas(frame)
        scrollbar = ttk.Scrollbar(frame, orient="vertical", command=canvas.yview)
        self.scrollable_frame = ttk.Frame(canvas)

        self.scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        canvas.create_window((0, 0), window=self.scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        # 填充字段
        fields = self.data.get('fields', {})
        self.entries = {}
        row = 0
        for field_name, info in fields.items():
            value = info.get('value', '')
            conf = info.get('confidence', 0)
            needs_review = info.get('needs_review', False)

            # 字段名
            fg_color = "red" if needs_review else "black"
            label_text = f"{field_name} (置信度: {conf:.2f})" + (" ⚠️" if needs_review else "")
            tk.Label(self.scrollable_frame, text=label_text, fg=fg_color, anchor="w").grid(row=row, column=0, sticky="w", pady=2)

            # 值输入框
            var = tk.StringVar(value=str(value) if value is not None else "")
            entry = tk.Entry(self.scrollable_frame, textvariable=var, width=40)
            entry.grid(row=row, column=1, padx=5, pady=2)
            if needs_review:
                entry.config(bg="#fff0f0")
            self.entries[field_name] = var
            row += 1

        # 按钮区域
        btn_frame = tk.Frame(self.top)
        btn_frame.pack(pady=10)

        tk.Button(btn_frame, text="保存修正", command=self.save_feedback, bg="lightblue").pack(side="left", padx=10)
        tk.Button(btn_frame, text="仅关闭", command=self.top.destroy).pack(side="left", padx=10)

    def save_feedback(self):
        # 收集修改过的字段
        original_fields = self.data.get('fields', {})
        for field_name, var in self.entries.items():
            new_val_str = var.get().strip()
            if new_val_str == "":
                new_val = None
            else:
                # 尝试恢复类型
                if new_val_str.replace('.', '', 1).isdigit():
                    new_val = float(new_val_str) if '.' in new_val_str else int(new_val_str)
                else:
                    new_val = new_val_str

            original_val = original_fields.get(field_name, {}).get('value')
            if new_val != original_val:
                self.corrections[field_name] = new_val

        if self.corrections and self.on_feedback:
            self.on_feedback(self.corrections)
            messagebox.showinfo("反馈已记录", f"已记录 {len(self.corrections)} 个字段的修正")
        self.top.destroy()


if __name__ == "__main__":
    try:
        root = tk.Tk()
        app = PDFBasicGUI(root)
        root.mainloop()
    except Exception as e:
        import traceback
        traceback.print_exc()
        input("Press Enter to exit...")