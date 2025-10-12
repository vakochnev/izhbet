# izhbet/core/__init__.py
"""
Основные компоненты системы.
"""

from .integration_service import IntegrationService, create_integration_service

__all__ = [
    'IntegrationService',
    'create_integration_service'
]
