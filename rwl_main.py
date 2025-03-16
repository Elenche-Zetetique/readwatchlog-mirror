#!/usr/bin/python3

from datetime import datetime
from rwl_base import BaseProcessor
from rwl_ods import OdsProcessor
from rwl_xlsx import XlsxProcessor
import argparse
import magic
import os
import sys
from typing import Any, Dict, Optional

class ReadWatchLog:
	"""
	Processes spreadsheet files and extracts information using the appropriate processor.

	Determines the file type (e.g., XLSX, ODS), selects the corresponding processor,
	and delegates processing tasks such as link extraction, JSON conversion, or tag sorting.

	Attributes:
		__filetypes (dict): Mapping of MIME types to file extensions (e.g., 'xlsx', 'ods').
		__processors (dict): Mapping of file extensions to processor classes (e.g., XlsxProcessor).
		__args (dict): Input arguments including file path, sheet name, and processing options.
		__processor (BaseProcessor): Instance of the selected processor class.
		__directories (dict): Directory paths for input/output operations.
	"""
	__slots__ = ("__filetypes",
				"__processors",
				"__called_arg",
				"__args",
				"__processor",
				"__args",
				"__directories")

	def __init__(self, args: dict):
		"""
		Initializes the processor with arguments and selects the appropriate file processor.
		
		Args:
			args: Dictionary of arguments with keys:
				- file (str): Path to the spreadsheet file.
				- sheet (str): Name of the sheet to process.
				- output (bool): Whether to save output to a new file.
				- custom_name (str, optional): Custom name for the output file.
				- chunk (int, optional): Number of rows to process (for 'links').
				- start (int, optional): Starting row index.
				- end (int, optional): Ending row index.
				- auto (bool, optional): Enable autosearch for unprocessed records (for 'links').
				- links, routines, tags, json, duplicates (bool, optional): Action flags.
		
		Raises:
			SystemExit: If file doesn’t exist or arguments are invalid.
		"""
		self.__filetypes: Dict[str, str] = {
			'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': 'xlsx',
			'application/vnd.oasis.opendocument.spreadsheet': 'ods'
		}
		self.__processors: Dict[str, Type[BaseProcessor]] = {
			'xlsx': XlsxProcessor,
			'ods': OdsProcessor
		}
		self.__directories: Dict[str, str] = {'inputs': 'inputs'}
		(os.makedirs(d, exist_ok=True) for d in self.__directories.values())
		self.__args: Dict[str, Any] = args
		self.__args['file'] = os.path.join(self.__directories['inputs'], self.__args['file'])
		self.__called_arg: Optional[str] = None
		self.__check_args()
		self.__processor: BaseProcessor = self.__select_processor()

	def __select_processor(self) -> BaseProcessor:
		"""
		Selects the appropriate processor based on the file's MIME type.

		Determines the file type using MIME detection and initializes the corresponding processor.

		Returns:
			BaseProcessor: An instance of the selected processor.

		Raises:
			KeyError: If the file format is not recognized or unsupported.
		"""
		mime: magic.Magic = magic.Magic(mime=True)
		fileformat: str = mime.from_file(self.__args['file'])
		filetype: Optional[str] = self.__filetypes.get(fileformat, None)
		if filetype is None:
			raise KeyError(f"No such format: {fileformat} (file: {self.__args['file']})")
	
		return self.__processors[filetype](args=self.__args)

	@property
	def links(self)-> Optional[Dict[str, Any]]:
		"""
		Retrieves links from the worksheet with output and error handling.

		Delegates the call to the `links` method of the selected processor instance.

		Returns:
			dict | None: Dictionary of processed links and their attributes, or None if an error occurs.
		"""
		return self.__processor.links()

	@property
	def json(self)-> Dict[str, Any]:
		"""
		Converts worksheet data to a JSON-compatible dictionary with output and error handling.

		Delegates the call to the `json` method of the selected processor instance.

		Returns:
			dict: A dictionary of worksheet data, or an empty dict if an error occurs.
		"""
		return self.__processor.json()

	@property
	def tags(self) -> None:
		"""
		Processes and sorts tags in the worksheet with output and error handling.

		Delegates the call to the `tags` method of the selected processor instance.

		Returns:
			None: Operation completes with no return value or on error.
		"""
		return self.__processor.tags()

	@property
	def duplicates(self)-> Dict[str, Any]:
		"""
		Gets duplicate links from the worksheet with output and error handling.

		Delegates the call to the `links` method of the selected processor instance.

		Returns:
			dict: Mapping of duplicate links to their indices, e.g., 
					{'link1': [0, 3], 'link2': [1, 5]}. Empty dict if no duplicates found.
		"""
		return self.__processor.duplicates()

	def run(self) -> None:
		"""
		Executes the action specified in the arguments.

		Calls the property corresponding to the selected action (e.g., 'links', 'json').
		"""
		getattr(self, self.__called_arg, None)

	def __check_args(self) -> None:
		"""Validates input arguments and determines the action to perform.

		Raises:
			SystemExit: If arguments are invalid or no action is specified.
		"""
		properties:	set[str]		= {"links", "routines", "tags", "json", "duplicates"}
		end:		Optional[int]	= self.__args.get("end", None)
		chunk:		int				= self.__args.get("chunk", 0)
		start:		Optional[int]	= self.__args.get("start", None)
		auto:		bool			= self.__args.get("auto", False)
		output:		bool			= self.__args.get("output", False)

		if not os.path.exists(self.__args['file']):
			sys.exit(f"File {self.__args['file']} does not exist")

		# Find the called argument
		self.__called_arg = next((key for key in self.__args if key in properties and self.__args[key] is True), None)
		if self.__called_arg is None: 
			sys.exit(f"No action specified. Use one of the following properties: {properties}")

		if end:
			# Validate range once
			try:
				result = isinstance(end, int) and end > start
			except Exception:
				sys.exit("Invalid inputs: --end and--start must be integers")

			if not result:
				sys.exit("Invalid range: --end must be greater than --start")

		# Define disallowed flags for certain actions
		range_flags: tuple[Optional[int], Optional[int], int, bool] = (start, end, chunk, auto)
		no_range_actions: set[str] = {"duplicates", "tags", "json"}

		match self.__called_arg:

			case "links":
				valid_links_combinations = [
					(start is not None and end is not None),
					(start is not None and chunk > 0),
					(auto and chunk > 0),
					(auto),
				]

				if not any(valid_links_combinations):
					sys.exit("Invalid combination for --links. Allowed combinations are: "
									"--start with --end, --start with --chunk, --auto with --chunk, or --auto alone.")

			# Ensure `--duplicates` does not allow `--start`, `--end`, `--chunk`, or `--auto`
			case "duplicates":
				if not output:
					sys.exit("--duplicates requires --output")
				if any(range_flags):
					sys.exit("Invalid combination for --duplicates. Cannot use with --start, --end, --chunk, or --auto.")

			# Ensure `--routines` requires `--start`
			case "routines":
				if start is None:
					sys.exit("--routines requires --start to be specified")
				if end is not None or chunk > 0 or auto:
					sys.exit("Invalid combination for --routines. It cannot be used with --end, --chunk, or --auto.")

			case "tags" | "json":
				if any(range_flags):
					sys.exit(f"Invalid combination for --{self.__called_arg}. Cannot use with --start, --end, --chunk, or --auto.")

def main(args: argparse.Namespace) -> None:
	# def main(args: dict):
	"""
	Processes command-line arguments and executes the specified action.

	Args:
		args: Parsed command-line arguments from argparse.Namespace.

	Raises:
		SystemExit: If no valid action is specified or arguments are invalid.

	Examples:
		>>> args = argparse.Namespace(links=True, file='Vault.xlsx', sheet='Sheet1', output=True)
		>>> main(args)  # Processes links from Vault.xlsx
	"""
	# Parse the arguments
	args = vars(args)

	# Initialize the ReadWatchLog class with the arguments
	rwl = ReadWatchLog(args=args)

	# Runs the action
	rwl.run()

if __name__ == '__main__':

	parser = argparse.ArgumentParser(description="Read-Watch-Log")

	# Define the arguments
	parser.add_argument('--start', type=int, default=None, help='Start value')
	parser.add_argument('--end', type=int, default=None, help='End value')
	parser.add_argument('--output', action='store_true', help='Output flag. If true, output is created. Optional argument.')
	parser.add_argument('--file', type=str, required=True, default="Vault.xlsx", help='File name to process (XSLX/ODS formats). Required argument.')
	parser.add_argument('--custom_name', type=str, default=datetime.now().strftime("output_%d%m%Y%H%M%S%f"), help='Custom name of output file')
	parser.add_argument('--chunk', type=int, default=0, help='Chunk size. Algorithm processes only given number of records. Used with argument `links`.')
	parser.add_argument('--auto', action='store_true', help='Autosearch of a non-processed record. Used with argument `links`.')
	parser.add_argument('--sheet', type=str, required=True, default="Vault", help='Sheetname of the given document. Required argument.')

	# Create a mutually exclusive group
	group = parser.add_mutually_exclusive_group()

	# Add mutually exclusive arguments
	group.add_argument('--links', action='store_true', help='Get links')
	group.add_argument('--routines', action='store_true', help='Get routines')
	group.add_argument('--tags', action='store_true', help='Order tags')
	group.add_argument('--json', action='store_true', help='Convert to JSON')
	group.add_argument('--duplicates', action='store_true', help='Detect duplicates')

	# args = {"links": True, "start": 41500, "end": 41650, "output": True, "chunk": 0, "file": "Vault.ods", "sheet": "Vault"}

	main(args=parser.parse_args())
	# main(args=args)