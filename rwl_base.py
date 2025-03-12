#!/usr/bin/python3

from abc import ABC, abstractmethod
from contextlib import contextmanager
from datetime import datetime
from dotenv import load_dotenv
from googleapiclient.discovery import build, Resource
from typing import Any, Callable, Dict, Generator, List, Union
from utilities import Utilities
import os
import re

load_dotenv()

class BaseProcessor(ABC):
	"""
	A class to perform actions on a specially crafted XLSX/ODS file.

	This class provides functionality to interact with XLSX/ODS files, extract and process data,
	and interact with the YouTube Data API to retrieve video details. It includes methods for
	handling workbook operations, validating data, and managing outputs.

	Attributes:
		__slots__ (tuple): A tuple of instance variable names to optimize memory usage.

	Methods:
		__init__(args : dict) -> None
			Initializes the class with configuration arguments and environment variables.

		_build_youtube_client() -> googleapiclient.discovery.Resource
			Initializes and returns a YouTube API client.

		_check_for_duplicates() -> dict
			Checks for duplicate links in an XLSX file and returns their indices.

		_check_record(row) -> bool
			Checks if a worksheet row represents an incomplete or invalid record.

		_convert_to_json() -> dict
			Converts the active worksheet into a JSON-compatible dictionary.

		_extract_time_in_minutes(items: list) -> float
			Extracts the duration of a YouTube video in minutes from API response items.

		_find_starting_row() -> int | None
			Finds the starting row containing a valid YouTube link.

		_get_col_number(col_name: str) -> int | None
			Finds the column number for a given column name in the worksheet.

		_get_links() -> dict
			Extracts YouTube links from a worksheet and processes them to store video durations.

		_get_routines() -> dict
			Fetches daily routines from a worksheet as a JSON-structured dictionary.

		_get_tags() -> list[int]
			Extracts column numbers of cells containing 'Tag' in the first row.

		_get_yt_video_details(video_id: str, part: str) -> dict
			Fetches content details for a given YouTube video ID using the YouTube Data API.

		_order_tags() -> None
			Sorts tag values in worksheet rows in ascending order.

		_process_yt_link(link: str, row: int) -> dict
			Processes a YouTube link to extract video details and duration.

		_validate_end_range() -> bool
			Validates that `self._END` is a positive integer greater than `self._START`.

		_wb_handler(func: Callable[..., Any]) -> Callable[..., Any]
			A decorator that manages a workbook operations for the wrapped function.

		duplicates() -> dict
			Gets duplicate links from the worksheet with output and error handling.

		json() -> dict
			Gets the result of ordering tags in the worksheet with output and error handling.

		links() -> dict | None
			Retrieves links using a decorated workbook handler with output and error management.

		ordered_tags() -> None:
			Gets the result of ordering tags in the worksheet with output and error handling.

		routines() -> dict:
			Retrieves routines using a decorated workbook handler with output and error management.
	"""
	__slots__ = ("_AUTOSEARCH",
				"_CHUNK",
				"_COLORS",
				"_CUSTOM_NAME",
				"_END",
				"_FILE",
				"_OUTPUT",
				"_OUTPUT_NAME",
				"_SHEETNAME",
				"_START",
				"_utilities",
				"_youtube_client",
				"_YT_PREFIX",
				"_API_KEY")

	def __init__(self, args: dict) -> None:
		"""
		Initializes the class with configuration arguments and environment variables.

		Sets up instance variables from a provided args dictionary and environment variables,
		including API key, YouTube site, workbook details, and color mappings. Also instantiates
		a Utilities object for additional functionality.

		Args:
			args (dict): Dictionary containing configuration parameters:
				- start (int, optional): Starting row index (default: 2).
				- end (int, optional): Ending row index (default: None).
				- output (str): Output identifier.
				- file (str, optional): File path (default: None).
				- workbook (object): Workbook object or reference.
				- custom_name (str, optional): Custom name for the output file (default: None).

        Notes:
            - The API key must be set in the environment variable `API_KEY`.
            - The API key can be generated at: https://console.cloud.google.com/apis/credentials.
		"""
		self._API_KEY = os.getenv('API_KEY')
		self._AUTOSEARCH = args.get('auto', None)
		self._CHUNK = args.get('chunk', None)
		self._COLORS = {
					'FFFF0000': 'red', 
					'FF00FF00': 'green', 
					'FFFFFF00': 'yellow'
				}
		self._CUSTOM_NAME = args.get('custom_name', None)
		self._END = args.get('end', None)
		if self._END: self._END += 1
		self._FILE = args.get('file', None)
		self._OUTPUT = args.get('output', None)
		self._SHEETNAME = args.get('sheet', None)
		self._START = args.get('start', 2)
		self._utilities = Utilities()
		self._youtube_client = self._build_youtube_client()
		self._YT_PREFIX = 'https://youtu.be/'

	def _build_youtube_client(self) -> Resource:
		"""
		Initializes and returns a YouTube API client.
		
		Returns:
			googleapiclient.discovery.Resource: A YouTube API client instance.
		
		Notes:
			- Requires an active API key stored in `self._API_KEY`.
			- Uses the `googleapiclient.discovery` module for API communication.
		"""
		return build('youtube', 'v3', developerKey=self._API_KEY)

	@abstractmethod
	def _check_for_duplicates(self) -> Dict[str, List[int]]:
		"""Checks for duplicate links in a specific format.

		This method should be implemented by subclasses to scan a given data source 
		and identify duplicate links. It returns a dictionary where each key is a 
		duplicate link, and the value is a list of zero-based indices indicating 
		where the duplicate appears.

		Returns:
			Dict[str, List[int]]: Mapping of duplicate links to their indices.
			An empty dictionary is returned if no duplicates are found.
		"""
		pass

	@abstractmethod
	def _convert_to_json(self) -> Dict[str, Dict[str, Any]]:
		"""
		Converts a worksheet into a JSON-compatible dictionary.

		This method should be implemented by subclasses to transform worksheet data into a nested 
		dictionary structure. The first row is treated as column headers, while values from the 
		second column are used as unique keys mapping to dictionaries representing the remaining 
		row data.

		Returns:
			Dict[str, Dict[str, Any]]: A dictionary where:
				- Keys are values from the second column.
				- Values are dictionaries containing the remaining row data.
		"""
		pass

	def _extract_time_in_minutes(self, items: list) -> float:
		"""
		Extracts the duration of a YouTube video in minutes from API response items.
		
		This method follows these operations:
		- Parses the `duration` field from `contentDetails`.
		- Extracts hours, minutes, and seconds using regex.
		- Converts the duration to a floating-point number representing minutes.
		
		Parameters:
			items (list): A list of video metadata items from the YouTube API.
		
		Returns:
			float: The video's duration in minutes.
		
		Notes:
			- Rounds seconds to the nearest 5% of a minute.
			- Assumes the first item contains the relevant duration.
		"""
		duration = items[0]['contentDetails']['duration']
		matches = re.findall(r'(\d+)([HMS])', duration)
		time_dict = {unit: int(value) for value, unit in matches}
		
		minutes = time_dict.get('H', 0) * 60 + time_dict.get('M', 0)
		seconds = time_dict.get('S', 0)
		minutes += round(seconds / 3) * 5 / 100  # Approximate fractional minutes
		
		return minutes

	@abstractmethod
	def _get_links(self) -> Dict[str, Dict[str, Any]]:
		"""Extracts and processes video links from a worksheet.

		This method should be implemented by subclasses to scan a worksheet,
		extract YouTube links, and associate them with relevant attributes such as
		video duration or metadata.

		Returns:
			Dict[str, Dict[str, Any]]: A dictionary where:
				- Keys (str): Extracted YouTube links.
				- Values (Dict[str, Any]): A dictionary of extracted attributes, such as:
					{
						"duration": str,
						"title": str,
						"views": int,
						...
					}
		"""
		pass

	@abstractmethod
	def _get_routines(self) -> Dict[str, Dict[str, float]]:
		"""
		Extracts and structures routine data from a worksheet.

		This method should be implemented by subclasses to parse routine information 
		from a worksheet, grouping totals by date and color categories.

		Returns:
			Dict[str, Dict[str, float]]: A JSON-like dictionary where:
				- Keys (str): Dates in "dd-mm-yyyy" format.
				- Values (Dict[str, float]): Sub-dictionaries mapping colors ("green", "red", "yellow") 
					to accumulated and rounded routine durations.
		"""
		pass

	@abstractmethod
	def _get_tags(self) -> List[int]:
		"""Retrieves the column indices containing tag headers.

		This method should be implemented by subclasses to scan the worksheet's first row 
		and identify the columns where the cell value contains the substring 'Tag'.

		Returns:
			List[int]: A list of 1-based column indices where 'Tag' appears in the value.
		"""
		pass

	def _get_yt_video_details(self, video_id: str, part: str) -> dict:
		"""
		Fetches content details for a given YouTube video ID using the YouTube Data API.

		Uses a pre-initialized YouTube API client to request video metadata for the specified `video_id`
		and `part`, returning the raw API response.

		Args:
			video_id (str): The unique identifier for the YouTube video (e.g., 'dQw4w9WgXcQ').
			part (str): The resource properties to retrieve (e.g., 'contentDetails', 'snippet').

		Returns:
			dict: The API response containing the requested video metadata.

		Raises:
			ValueError: If `video_id` is empty or invalid.
			googleapiclient.errors.HttpError: If the API request fails (e.g., due to invalid `part`, quota limits, or network issues).

		Notes:
			- Assumes the YouTube API client is initialized once in the class `__init__` method.
		"""
		return self._youtube_client.videos().list(part=part, id=video_id).execute()

	@abstractmethod
	def _order_tags(self) -> None:
		"""Sorts tag values in worksheet rows.

		This method should be implemented by subclasses to iterate through worksheet rows, 
		collect tag values, sort them in ascending order, and rewrite them into the same columns.

		Returns:
			None: This method modifies the worksheet in place.
		"""
		pass

	def _process_yt_link(self, link: str, row: int) -> dict:
		"""
		Processes a YouTube link to extract video details and duration.

		Extracts the video ID from a YouTube URL, fetches metadata using the YouTube Data API, and converts
		the video duration to minutes. Returns the details or an error message if processing fails.

		Args:
			link (str): The YouTube video URL to process.
			row (int): The row number in the worksheet where the link is located.

		Returns:
			dict: A dictionary containing video details (e.g., {'duration': float}) on success, 
				or an error message (e.g., {'error': str}) on failure.

		Raises:
			ValueError: If the provided `link` is not a valid YouTube URL.
			RuntimeError: If the YouTube Data API request fails (e.g., due to network issues or API limits).

		Notes:
			- Relies on `_get_yt_video_details` to fetch video metadata from the YouTube Data API.
			- Uses `_extract_time_in_minutes` to convert the video duration to a float value in minutes.
		"""
		try:
			video_id = link.split(self._YT_PREFIX)[1]
			content_details = self._get_yt_video_details(video_id=video_id, part='contentDetails')
			snippet = self._get_yt_video_details(video_id=video_id, part='snippet')
		except Exception as e:
			raise Exception(f"Unexpected error: {e}")

		link_info = {
						link: {
							'Duration': None,
							'Published': None,
							'Author': None,
							'Exist': None
						}
					}

		if content_details.get('items', []): 
			link_info[link]['Duration'] = self._extract_time_in_minutes(items=content_details['items'])

		if snippet.get('items', []):

			# Check if timestamp is not None before parsing
			timestamp = snippet['items'][0]['snippet'].get('publishedAt', None)
			published = (
				datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ").strftime("%H:%M:%S %d-%m-%Y")
				if timestamp else None
			)

			link_info[link]['Published'] = published
			link_info[link]['Author'] = snippet['items'][0]['snippet'].get('channelTitle', None)

		if not link_info[link]['Duration'] or not link_info[link]['Published'] or not link_info[link]['Author']:
			link_info[link]['Exist'] = 'non-existent'

		return link_info

	def _validate_end_range(self) -> bool:
		"""
		Validates that `self._END` is a positive integer greater than `self._START`.

		Checks if the instance variable `self._END` is properly set as an integer and exceeds
		`self._START` to ensure a valid range for processing.

		Returns:
			bool: True if `self._END` is a positive integer and greater than `self._START`, False otherwise.

		Raises:
			AttributeError: If `self._START` or `self._END` is not defined.

		Notes:
			- Both `self._START` and `self._END` are assumed to be instance variables set elsewhere in the class.
		"""
		return isinstance(self._END, int) and self._END > self._START

	def _wb_handler(self, func: Callable[..., Any]) -> Callable[..., Any]:
			"""
			A decorator that manages a workbook operations for the wrapped function using a context manager.

			Ensures a workbook is loaded and worksheet is selected before executing the wrapped function,
			then saves the workbook afterward, even if an error occurs.

			Args:
				func (Callable[..., Any]): The function to decorate, which operates on the workbook.

			Returns:
				Callable[..., Any]: A wrapper function that encapsulates workbook handling.

			Raises:
				TypeError: If func is not callable.
				ValueError: If workbook or sheet cannot be loaded.
			"""
			if not callable(func):
				raise TypeError("The provided func must be a callable.")

			def wrapper(*args: Any, **kwargs: Any) -> Any:
				with self._workbook_manager():
					result = func(*args, **kwargs)
					return result
			return wrapper

	@abstractmethod
	@contextmanager
	def _workbook_manager(self) -> Generator[Any, None, None]:
		"""Context manager for handling a workbook.

		This method should be implemented by subclasses to manage the lifecycle of a 
		workbook, ensuring proper loading and saving. The method yields the workbook object 
		for use within a `with` block.

		Yields:
			Any: The loaded workbook object, allowing operations to be performed on it.
		"""
		pass

	@property
	def duplicates(self) -> dict:
		"""
		Gets duplicate links from the worksheet with output and error handling.

		Retrieves a mapping of duplicate links found in the worksheet, processed within a
		managed output with workbook handling and exception logging. Each duplicate link is
		mapped to a list of its zero-based indices from column 2, starting at row 2.

		Returns:
			dict: Mapping of duplicate links to their indices, e.g., 
					{'link1': [0, 3], 'link2': [1, 5]}. Empty dict if no duplicates found.

		Notes:
			- Relies on Utilities methods for output creation and exception handling.
			- Uses the output ID and custom name from instance variables if provided.
		"""
		return	self._utilities.exception_handler(
					self._utilities.create_output(
						self._wb_handler(
							self._check_for_duplicates),
						create_output=self._OUTPUT, 
						custom_name=self._CUSTOM_NAME),
					log_error=True)

	@property
	def json(self) -> dict:
		"""
		Gets the result of ordering tags in the worksheet with output and error handling.

		Executes the tag-ordering process within a managed output, applying workbook handling
		and exception logging. The underlying operation sorts tag values across specified
		columns in the worksheet.

		Returns:
			None: If the operation completes with no meaningful return value or on error.

		Notes:
			- Relies on Utilities methods for output creation and exception handling.
			- Uses the custom name and output ID from instance variables if provided.
		"""
		return	self._utilities.exception_handler(
					self._utilities.create_output(
						self._wb_handler(
							self._convert_to_json),
						create_output=self._OUTPUT, 
						custom_name=self._CUSTOM_NAME),
					log_error=True)

	@property
	def links(self) -> dict | None:
		"""
		Retrieves links using a decorated workbook handler with output and error management.

		This property wraps the `_get_links` method with `_wb_handler`, creates a output
		using `_utilities.create_output` with the decorated links and output parameters, and manages
		exceptions via `_utilities.exception_handler` with error logging.

		Returns:
			Any: The result of the link retrieval routine, potentially modified by output management
				and exception handling.

		Raises:
			AttributeError: If required instance variables (e.g., custom name or output ID) are not set.
			RuntimeError: If output creation or link retrieval fails due to underlying utility errors.

		Notes:
			- Relies on `_utilities.create_output` for output management and `_utilities.exception_handler`
			for error handling.
			- Uses instance variables for custom name and output ID if provided, falling back to defaults otherwise.
			- Decorated with `_wb_handler` to enhance workbook-related functionality.
		"""
		return	self._utilities.exception_handler(
					self._utilities.create_output(
						self._wb_handler(
							self._get_links),
						create_output=self._OUTPUT, 
						custom_name=self._CUSTOM_NAME),
					log_error=True)

	@property
	def tags(self) -> None:
		"""
		Gets the result of ordering tags in the worksheet with output and error handling.

		Executes the tag-ordering process within a managed output, applying workbook handling
		and exception logging. The underlying operation sorts tag values across specified
		columns in the worksheet.

		Returns:
			None: If the operation completes with no meaningful return value or on error.

		Notes:
			- Relies on Utilities methods for output creation and exception handling.
			- Uses the custom name and output ID from instance variables if provided.
		"""
		return	self._utilities.exception_handler(
					self._utilities.create_output(
						self._wb_handler(
							self._order_tags),
						create_output=self._OUTPUT, 
						custom_name=self._CUSTOM_NAME),
					log_error=True)

	@property
	def routines(self) -> dict:
		"""
		Retrieves routines using a decorated workbook handler with output and error management.

		This property wraps the `_get_routines` method with `_wb_handler`, creates a output
		using `_utilities.create_output` with the decorated routines and output parameters, and manages
		exceptions via `_utilities.exception_handler` with error logging.

		Returns:
			Any: The result of the routine retrieval, potentially modified by output management
				and exception handling.

		Raises:
			AttributeError: If required instance variables (e.g., custom name or output ID) are not set.
			RuntimeError: If output creation or routine retrieval fails due to underlying utility errors.

		Notes:
			- Relies on `_utilities.create_output` for output management and `_utilities.exception_handler`
			for error handling.
			- Uses instance variables for custom name and output ID if provided, falling back to defaults otherwise.
			- Decorated with `_wb_handler` to enhance workbook-related functionality.
		"""
		return	self._utilities.exception_handler(
					self._utilities.create_output(
						self._wb_handler(
							self._get_routines),
						create_output=self._OUTPUT, 
						custom_name=self._CUSTOM_NAME),
					log_error=True)