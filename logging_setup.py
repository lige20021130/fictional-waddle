# logging_setup.py - 统一日志配置
"""
纳米酶文献提取系统 - 统一日志配置

功能：
1. 集中配置日志格式和级别
2. 支持不同模块的日志级别设置
3. 支持输出到文件和控制台
4. 兼容 GUI 日志转发

使用方法：
    from logging_setup import setup_logging, get_logger
    
    setup_logging()  # 在应用启动时调用
    logger = get_logger(__name__)  # 在各模块中使用
"""

import logging
import sys
from pathlib import Path
from typing import Optional, Dict
from datetime import datetime
import threading

# 全局日志配置锁
_config_lock = threading.Lock()
_configured = False


# 日志格式定义
DEFAULT_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
DETAILED_FORMAT = '%(asctime)s - %(name)s - [%(filename)s:%(lineno)d] - %(levelname)s - %(message)s'
GUI_FORMAT = '%(message)s'


class ColoredFormatter(logging.Formatter):
    """带颜色的日志格式化器"""
    
    COLORS = {
        'DEBUG': '\033[36m',     # 青色
        'INFO': '\033[32m',      # 绿色
        'WARNING': '\033[33m',   # 黄色
        'ERROR': '\033[31m',     # 红色
        'CRITICAL': '\033[35m',  # 紫色
    }
    RESET = '\033[0m'
    
    def format(self, record):
        # 在支持颜色的终端中添加颜色
        if hasattr(sys.stderr, 'isatty') and sys.stderr.isatty():
            levelname = record.levelname
            if levelname in self.COLORS:
                record.levelname = f"{self.COLORS[levelname]}{levelname}{self.RESET}"
        return super().format(record)


class GUILogHandler(logging.Handler):
    """
    GUI日志处理器
    
    将日志转发到GUI界面
    """
    
    def __init__(self, gui_callback=None):
        super().__init__()
        self.gui_callback = gui_callback
        self._queue = []
        self._lock = threading.Lock()
    
    def emit(self, record):
        try:
            log_msg = self.format(record)
            
            # 根据日志级别添加前缀
            prefix = ""
            if record.levelno >= logging.ERROR:
                prefix = "[ERROR] "
            elif record.levelno >= logging.WARNING:
                prefix = "[WARN] "
            elif record.levelno >= logging.INFO:
                prefix = "[INFO] "
            else:
                prefix = "[DEBUG] "
            
            msg = f"{prefix}{log_msg}"
            
            if self.gui_callback:
                try:
                    self.gui_callback(msg)
                except Exception:
                    # GUI回调失败，存入队列
                    with self._lock:
                        self._queue.append(msg)
            else:
                with self._lock:
                    self._queue.append(msg)
                    
        except Exception:
            self.handleError(record)
    
    def get_queue(self):
        """获取并清空队列"""
        with self._lock:
            msgs = self._queue.copy()
            self._queue.clear()
            return msgs
    
    def set_callback(self, callback):
        """设置GUI回调"""
        self.gui_callback = callback


# 模块日志级别配置
MODULE_LOG_LEVELS: Dict[str, int] = {
    'api_client': logging.INFO,
    'llm_extractor': logging.INFO,
    'vlm_extractor': logging.INFO,
    'extraction_pipeline': logging.INFO,
    'result_integrator': logging.INFO,
    'rule_learner': logging.INFO,
    'nanozyme_preprocessor': logging.INFO,
    'config_manager': logging.INFO,
    'cache_manager': logging.INFO,
    'task_queue': logging.INFO,
}


def setup_logging(
    level: int = logging.INFO,
    log_file: Optional[str] = None,
    detailed: bool = False,
    use_colors: bool = True,
    gui_callback: Optional[callable] = None
) -> None:
    """
    设置全局日志配置
    
    Args:
        level: 默认日志级别
        log_file: 日志文件路径（可选）
        detailed: 是否使用详细格式
        use_colors: 是否使用颜色
        gui_callback: GUI日志回调函数
    """
    global _configured
    
    with _config_lock:
        if _configured:
            logging.warning("日志系统已配置，忽略重复配置请求")
            return
        
        # 获取根日志器
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)  # 允许子模块控制级别
        
        # 清除现有处理器
        root_logger.handlers.clear()
        
        # 选择格式
        fmt = DETAILED_FORMAT if detailed else DEFAULT_FORMAT
        
        # 控制台处理器
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        if use_colors and hasattr(sys.stderr, 'isatty') and sys.stderr.isatty():
            console_handler.setFormatter(ColoredFormatter(fmt))
        else:
            console_handler.setFormatter(logging.Formatter(fmt))
        root_logger.addHandler(console_handler)
        
        # 文件处理器
        if log_file:
            log_path = Path(log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            
            file_handler = logging.FileHandler(log_path, encoding='utf-8')
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(logging.Formatter(DETAILED_FORMAT))
            root_logger.addHandler(file_handler)
        
        # GUI处理器
        if gui_callback:
            gui_handler = GUILogHandler(gui_callback)
            gui_handler.setLevel(logging.INFO)
            gui_handler.setFormatter(logging.Formatter(GUI_FORMAT))
            root_logger.addHandler(gui_handler)
        
        # 设置模块级别
        for module_name, module_level in MODULE_LOG_LEVELS.items():
            module_logger = logging.getLogger(module_name)
            module_logger.setLevel(module_level)
        
        _configured = True
        
        logging.info(f"日志系统初始化完成 (level={logging.getLevelName(level)})")


def get_logger(name: str) -> logging.Logger:
    """
    获取模块日志器
    
    Args:
        name: 模块名（通常使用 __name__）
        
    Returns:
        日志器
    """
    # 如果未配置，先用默认配置
    if not _configured:
        setup_logging()
    
    return logging.getLogger(name)


def set_module_level(module_name: str, level: int) -> None:
    """
    设置特定模块的日志级别
    
    Args:
        module_name: 模块名
        level: 日志级别
    """
    logger = logging.getLogger(module_name)
    logger.setLevel(level)


def add_file_handler(
    log_file: str,
    level: int = logging.INFO,
    format_str: Optional[str] = None
) -> None:
    """
    添加文件日志处理器
    
    Args:
        log_file: 日志文件路径
        level: 日志级别
        format_str: 格式字符串
    """
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    
    handler = logging.FileHandler(log_path, encoding='utf-8')
    handler.setLevel(level)
    handler.setFormatter(logging.Formatter(format_str or DETAILED_FORMAT))
    
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)


class LogContext:
    """日志上下文管理器（用于临时改变日志级别）"""
    
    def __init__(self, module_name: str, level: int):
        self.module_name = module_name
        self.new_level = level
        self.old_level = None
        self.logger = logging.getLogger(module_name)
    
    def __enter__(self):
        self.old_level = self.logger.level
        self.logger.setLevel(self.new_level)
        return self.logger
    
    def __exit__(self, *args):
        self.logger.setLevel(self.old_level)


# 便捷函数
def enable_debug(module_name: Optional[str] = None):
    """启用调试日志"""
    if module_name:
        set_module_level(module_name, logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.DEBUG)


def disable_debug(module_name: Optional[str] = None):
    """禁用调试日志"""
    if module_name:
        set_module_level(module_name, logging.INFO)
    else:
        logging.getLogger().setLevel(logging.INFO)


# 导出公共接口
__all__= [
    'setup_logging',
    'get_logger',
    'set_module_level',
    'add_file_handler',
    'LogContext',
    'enable_debug',
    'disable_debug',
    'GUILogHandler',
    'MODULE_LOG_LEVELS',
]
