"""
Построитель сложных отчетов (all-time, combined).
"""

import logging
import pandas as pd
from datetime import date
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


class ReportBuilder:
    """Класс для построения сложных отчетов."""
    
    def split_report_content(self, content: str) -> Tuple[str, str]:
        """
        Разделяет контент отчета на две части для публикации.
        
        Args:
            content: Полный текст отчета
            
        Returns:
            tuple: (first_part, second_part)
        """
        if not content:
            return ("", "")
        
        lines = content.split('\n')
        mid_point = len(lines) // 2
        
        first_part = '\n'.join(lines[:mid_point])
        second_part = '\n'.join(lines[mid_point:])
        
        return (first_part, second_part)

