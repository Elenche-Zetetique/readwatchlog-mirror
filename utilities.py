#!/usr/bin/python3

from datetime import datetime
from functools import wraps
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Callable
import colorlog
import json
import logging
import os
import uuid

class Utilities:
	"""
	A class to represent extra functions,
	that can be used within classes.

	...

	Attributes
	----------
	None

	Methods
	-------
	@staticmethod
	def merge_outputs()
		Merges output files

	@staticmethod
	def create_output(file)
		Create output file
	"""
	def __init__(self):

		self.__TIMESTAMP_FORMAT = '%m%d%Y_%H%M%S_%f'  # Class-level constant
		self.__directories = {
								'outputs':	'outputs',
								'logs':		'logs'
							}
		(os.makedirs(d, exist_ok=True) for d in self.__directories.values())

		# Method for logger-setup
		self.__setup_logger()

	def __setup_logger(self):

		# Disable default handlers by not using basicConfig
		self.logger = logging.getLogger(__name__)	# Use module-specific logger instead of root logger
		self.logger.setLevel(logging.DEBUG)			# Set to DEBUG to allow all levels
		self.logger.handlers.clear()				# Remove existing handlers (useful when running multiple instances)

		## Defining console format
		formatter = logging.Formatter(
			f'\n{'-' * 50}\n%(asctime)s | %(levelname)-8s | %(threadName)s | %(name)s | %(message)s\n{'-' * 50}'
		)

		# Console Stream Handler
		# Defining console format
		console_formatter = colorlog.ColoredFormatter(
			'%(log_color)s%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
			log_colors={
				'DEBUG':	'cyan',
				'INFO':		'green',
				'WARNING':	'yellow',
				'ERROR':	'red',
				'CRITICAL':	'bold_red',
			}
		)

		console_handler = logging.StreamHandler()		# Handles all log types
		console_handler.setFormatter(console_formatter)	# Setting formatter
		self.logger.addHandler(console_handler)			# Adding handler

		# RotatingFileHandler for INFO logs (info.log)
		info_handler = RotatingFileHandler(
			os.path.join(self.__directories['logs'], "info.log"),
			maxBytes=1024 * 1024,  # 1 MB per file
			backupCount=5,
			encoding='utf-8'
		)
		info_handler.addFilter(lambda record: record.levelno < logging.ERROR)	# Filters out ERROR and CRITICAL
		info_handler.setLevel(logging.INFO)		# Handles INFO and WARNING
		info_handler.setFormatter(formatter)	# Setting formatter
		self.logger.addHandler(info_handler)	# Adding handler

		# RotatingFileHandler for ERROR logs (error.log)
		error_handler = RotatingFileHandler(
			os.path.join(self.__directories['logs'], "error.log"),
			maxBytes=1024 * 1024,  # 1 MB per file
			backupCount=5,
			encoding='utf-8'
		)
		error_handler.setLevel(logging.ERROR)	# Handles only ERROR and CRITICAL
		error_handler.setFormatter(formatter)	# Setting formatter
		self.logger.addHandler(error_handler)	# Adding handler

		# Ensure logs are not duplicated
		self.logger.propagate = False  

	# @staticmethod
	def merge_outputs(self):
		"""
		Merges output files

		...

		Parameters
		----------
		None

		Returns
		-------
		None
		"""
		output_files = os.listdir(self.__directories['outputs'])

		json_files = filter(lambda json_file: True if '.json' in json_file else False, output_files)

		merged_outputs = {}

		for sf in json_files:

			self.logger.info(f"Adding the output-file '{sf}'")
			with open(os.path.join(self.__directories['outputs'], sf), 'r') as json_file:
				json_output_data = json.load(json_file)

			merged_outputs.update(json_output_data)

		# Gnerating output-file name
		now_ts = datetime.now().strftime("%d%m%Y_%H%M%S_%f")

		merged_file = f'merged_outputs_{now_ts}_{uuid.uuid4().hex}.json'

		self.logger.info(f"Saving the outputs into '{merged_file}'")

		# Save all files into output-file
		with open(os.path.join(self.__directories['results'], merged_file), 'w', encoding='utf-8') as merged:
			json.dump(merged_outputs, merged, ensure_ascii=False, indent=4)

	def generate_output_name(self, custom_name=None, unique_id=None):
		"""
		Generates a output file name with optional custom or unique identifiers.

		Constructs a output file name using the outputs directory prefix. Appends either a
		custom name, a unique UUID, or a timestamp based on provided arguments or defaults.

		Args:
			custom_name: Optional string to append to the output name (default: None).
			unique_id: Optional flag to append a UUID hex string (default: None).

		Returns:
			str: output file name in one of these formats:
				- "{outputs_dir}/output_{custom_name}" if custom_name is provided
				- "{outputs_dir}/output_{uuid_hex}" if unique_id is True
				- "{outputs_dir}/output_{MMDDYYYY_HHMMSS_ffffff}" otherwise
		"""
		base = f"{self.__directories['outputs']}/output"
		separator = "_"

		if custom_name:
			suffix = custom_name
		elif unique_id:
			suffix = uuid.uuid4().hex
		else:
			suffix = datetime.now().strftime(self.__TIMESTAMP_FORMAT)

		return f"{base}{separator}{suffix}"

	def create_output(self, func: Callable[..., Any], *, create_output: bool = False, custom_name: str | None) -> Callable[..., Any]:
		"""
		Creates a decorator that optionally saves the function's result to a JSON output file.

		This method wraps a function and, if `create_output` is True, saves its result to a JSON file
		with a generated or custom name. The original function's result is returned unchanged.

		Args:
			func: The function to decorate, which can take any arguments and return any value.
			create_output: If True, saves the function's result to a JSON file. Defaults to False.
			custom_name: Optional custom name for the output file. If None, a name is generated.
				Defaults to None.

		Returns:
			Callable[..., Any]: A wrapped function that retains the original function's behavior
				and optionally saves its result.

		Examples:
			>>> class Example(YourClass):
			...     def generate_output_name(self, name): return name or "output"
			...     logger = logging.getLogger()
			>>> @Example().create_output(create_output=True, custom_name="test")
			... def process_data(x): return {"data": x}
			>>> process_data(42)
			{"data": 42}  # Also saves to "test.json"
		"""
		def wrapper(*args: Any, **kwargs: Any) -> Any:
			result = func(*args, **kwargs)
			if not result:  # Early return for falsy results
				return None

			if create_output:
				# Use pathlib for better file handling
				from pathlib import Path
				filename = Path(f"{self.generate_output_name(custom_name)}.json")
				self.logger.info(f"Saving output file: {filename}")
				
				# Write JSON with error handling
				try:
					filename.write_text(
						json.dumps(result, ensure_ascii=False, indent=4),
						encoding="utf-8"
					)
				except (IOError, TypeError) as e:
					self.logger.error(f"Failed to save output file {filename}: {e}")

			return result

		return wrapper

	def exception_handler(self, func: Callable[..., Any], log_error: bool = True) -> Callable[..., Any]:
		"""A decorator to handle workbook-related exceptions and raise custom exceptions.

		Args:
			log_error (bool, optional): Whether to log the error. Defaults to True.

	    Returns:
	        callable: The decorated function, which returns the result of the original function
	                  or None if an exception occurs.
		"""
		def wrapper(*args, **kwargs):
			try:
				return func(*args, **kwargs)
			except Exception as e:
				# Handle exceptions
				if log_error: self.logger.error(f"Error in {func.__name__}: {e}", exc_info=True)
				return None
		return wrapper