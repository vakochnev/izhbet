# tests/test_getting.py
import pytest
from getting.getting import main
from unittest.mock import patch


@patch("sys.argv", ["script_name", "live"])
@patch("getting.download.Download.download_sportradar")
def test_main_with_operation(mock_download):
    main()
    mock_download.assert_called_once()


@patch("sys.argv", ["script_name"])
@patch("getting.download.Download.download_sportradar")
def test_main_default_operation(mock_download):
    main()
    mock_download.assert_called_once()
