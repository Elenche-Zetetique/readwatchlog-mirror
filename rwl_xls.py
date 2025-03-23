#!/usr/bin/python3

from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime
from dotenv import load_dotenv
from pyexcel_ods3 import get_data, save_data
from rwl_base import BaseProcessor
from rwl_xlsx import XlsxProcessor
from tqdm import tqdm
from typing import Any, Dict, Generator, List, Optional
from utilities import Utilities
import copy

class XlsProcessor(BaseProcessor):
	"""
	A class to perform actions on a specially crafted ODS (OpenDocument Spreadsheet) file.

	This class provides functionality to interact with ODS files, extract and process data,
	and interact with the YouTube Data API to retrieve video details. It includes methods for
	handling workbook operations, validating data, and managing outputs.

	Attributes:
		__slots__ (tuple): A tuple of instance variable names to optimize memory usage.

	Methods:
		_check_for_duplicates() -> dict
			Checks for duplicate links in an ODS file and returns their indices.

		__check_record(row) -> bool
			Checks if a worksheet row represents an incomplete or invalid record.

		_convert_to_json() -> dict
			Converts the active worksheet into a JSON-compatible dictionary.

		__find_starting_row() -> int | None
			Finds the starting row containing a valid YouTube link.

		__get_col_number(col_name: str) -> int | None
			Finds the column number for a given column name in the worksheet.

		_get_links() -> dict
			Extracts YouTube links from an Excel worksheet and processes them to store video durations.

		_get_routines() -> dict
			Fetches daily routines from a worksheet as a JSON-structured dictionary.

		_get_tags() -> list[int]
			Extracts column numbers of cells containing 'Tag' in the first row.

		_order_tags() -> None
			Sorts tag values in worksheet rows in ascending order.

		_process_yt_link(link: str, row: int) -> dict
			Processes a YouTube link to extract video details and duration.

		_validate_end_range() -> bool
			Validates that `self._END` is a positive integer greater than `self._START`.
	"""
	__slots__ = ("_ws", "_ws_temp", "__xlsx_processor", "_args")

	def __init__(self):
		self.__xlsx_processor = XlsxProcessor(args=self._args)

	def _check_for_duplicates(self) -> Dict[str, List[int]]:
		"""
		Checks for duplicate links in an XLSX file and returns their indices.

		Scans the worksheet starting at row 2, column 2, collecting all link values until an
		empty cell is encountered. Identifies duplicates and maps each duplicate link to a
		list of its zero-based indices in the original sequence.

		Returns:
			dict: Mapping of duplicate links to their indices, e.g., 
				{'link1': [0, 3], 'link2': [1, 5]}. Empty dict if no duplicates found.
		"""
		return self.__xlsx_processor._check_for_duplicates()

	def _convert_to_json(self) -> Dict[str, Dict[str, Any]]:
		"""
		Converts the active worksheet into a JSON-compatible dictionary.

		The first row is treated as column headers. The second column is used as unique keys 
		for the dictionary, with the remaining columns as nested key-value pairs.

		Returns:
			dict: A dictionary where the keys are values from the second column, 
				and the values are dictionaries of the remaining row data.
		"""
		return self.__xlsx_processor._convert_to_json()

	def _get_links(self) -> dict:
		"""
		Extracts YouTube links from an Excel worksheet and processes them to store video durations.

		Returns:
			dict: Processed links and their attributes

		Raises:
			ValueError: If range is invalid (END <= START)
		"""
		return self.__xlsx_processor._get_links()

	def _get_routines(self) -> Dict[str, Dict[str, float]]:
		"""
		Fetches daily routines from a worksheet as a JSON-structured dictionary.

		Extracts routine data starting from a predefined row (`self._START`), using column numbers
		for 'Date' and 'Duration'. For each row with a valid duration (not '.'), computes a rounded
		value, assigns a color based on cell fill, and aggregates totals by date and color.

		Returns:
			dict: JSON-structured dictionary with daily routines, formatted as:
				{
					"dd-mm-yyyy": {
						"green":	float,	# Rounded total for green routines
						"red":		float,	# Rounded total for red routines (optional)
						"yellow":	float	# Rounded total for yellow routines (optional)
					},
					...
				}
				Dates are strings in "dd-mm-yyyy" format; colors and values vary by row data.
		"""
		return self.__xlsx_processor._get_routines()

	def _get_tags(self) -> List[int]:
		"""
		Extracts column numbers of cells containing 'Tag' in the first row.

		Scans row 1 of the worksheet, collecting 1-based column indices where the cell value
		contains the substring 'Tag'. Stops at the first empty cell.

		Returns:
			list[int]: List of 1-based column numbers where 'Tag' appears in the value.
		"""
		return self.__xlsx_processor._get_tags()

	def _order_tags(self) -> None:
		"""
		Sorts tag values in worksheet rows in ascending order.

		Iterates over rows in the worksheet starting from `self._START`, collects non-placeholder ('.')
		tag values from columns specified in `self._get_tags()`, sorts them, and rewrites them across
		the same columns. Stops when the first tag column in a row has no value.

		Returns:
			None: Modifies the worksheet (`self._ws`) in place.

		Raises:
			AttributeError: If `self._ws`, `self._START`, or `self._get_tags()` is not properly initialized.
			TypeError: If tag column indices returned by `self._get_tags()` are not integers.

		Notes:
			- Relies on `self._ws` as the worksheet object and `self._START` as the starting row index.
			- Uses `self._get_tags()` to retrieve the list of column indices for tag values.
			- Placeholder values ('.') are ignored during sorting and not rewritten.
		"""
		return self.__xlsx_processor._order_tags()

	@contextmanager
	def _workbook_manager(self) -> Generator[dict, None, None]:
		"""
		A context manager that handles loading and saving an ODS workbook.

		Loads an ODS workbook from `self._FILE`, selects the worksheet specified by
		`self._SHEETNAME`, and yields the workbook object (as a dict) for use within a `with` block.
		Ensures the workbook is saved after execution, even if an error occurs, either to
		`self._FILE` or to a generated output file if `self._OUTPUT` is True.

		Yields:
			dict: The loaded ODS workbook data as a dictionary where keys are sheet names
				and values are lists of rows.

		Raises:
			TypeError: If `self._FILE` or `self._SHEETNAME` is not a string or is not set.
			FileNotFoundError: If the file specified by `self._FILE` does not exist.
			KeyError: If `self._SHEETNAME` does not exist in the workbook.
			Exception: For other unexpected errors during workbook loading or saving.

		Notes:
			- Relies on instance variables `self._FILE` (str), `self._SHEETNAME` (str),
			`self._OUTPUT` (bool), and `self._CUSTOM_NAME` (str or None).
			- Uses `self._utilities.generate_output_name` to create the output filename if
			`self._OUTPUT` is True.
			- The workbook is only saved if it was successfully loaded.
			- With pyexcel_ods3, the workbook is a dict, and sheets are accessed as keys.
		"""
		return self.__xlsx_processor._workbook_manager()