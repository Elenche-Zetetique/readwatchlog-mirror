#!/usr/bin/python3

from datetime import datetime
from rwl_xlsx import XlsxProcessor
from typing import Any, Dict, Optional
import argparse
import magic
import os
import shutil
import subprocess
import sys
from utilities import Utilities

class ReadWatchLog:
	"""
	Processes spreadsheet files and extracts information using the appropriate processor.

	Determines the file type (e.g., XLSX, ODS), selects the corresponding processor,
	and delegates processing tasks such as link extraction, JSON conversion, or tag sorting.

	Attributes:
		__filetypes (dict): Mapping of MIME types to file extensions (e.g., 'xlsx', 'ods').
		__processors (dict): Mapping of file extensions to processor classes (e.g., XlsxProcessor).
		_args (dict): Input arguments including file path, sheet name, and processing options.
		__processor (BaseProcessor): Instance of the selected processor class.
	"""
	__slots__ = ("_args",
				 "__conversion",
				 "__called_arg",
				 "__processor",
				 "__processors",
				 "__filetype",
				 "__filetypes",
				 "__utilities",
				 "__temp_file",
				 "__error_msg",
				 "__file",
				 "__preparation")

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
		self.__filetype = None
		self.__error_msg = "An error occured. For details see the error-log."
		self.__filetypes: Dict[str, str] = {
			'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': 'xlsx',
			'application/vnd.oasis.opendocument.spreadsheet': 'ods',
			'application/vnd.ms-excel': 'xls'
		}
		self._args: Dict[str, Any] = args
		self.__called_arg: Optional[str] = None
		self.__utilities = Utilities()

	def __remove_temp(self):
		shutil.rmtree(self._args['temp_dir'])
		if not(self._args['main_extension'] in self._args['file']): 
			os.remove(self._args['output_file'])

	def __convert(self, convert_args : dict) -> None:
		"""
		Converts a given file to XLSX format using LibreOffice.

		Executes a shell command to convert the input file to XLSX format using LibreOffice 
		in headless mode. If the conversion is successful, logs the output. If the conversion 
		fails, logs the error and exits the program with an error message.

		Raises:
			SystemExit: If the conversion process fails.
		"""
		command: list[str] = [
			"/usr/bin/libreoffice", 
			"--headless", 
			"--invisible", 
			"--convert-to", 
			convert_args['to_extension'], 
			convert_args['file'],
			"--outdir",
			convert_args['output_dir']
		]

		name, extension = os.path.splitext(convert_args['file'])
		new_file = ''.join([name, '.', convert_args['to_extension']])
		try:
			result = subprocess.run(command, check=True, capture_output=True, text=True)
			self.__utilities.logger.info(f"Successful conversion of {convert_args['file']} into {new_file}")
		except subprocess.CalledProcessError as e:
			self.__utilities.logger.error(f"Conversion failed: {e.stderr}")
			sys.exit(self.__error_msg)

		if not os.path.exists(new_file):
			self.__utilities.logger.error(f"New file {new_file} does not exist")
			sys.exit(self.__error_msg)

		self._args['file'] = new_file

	def __check_filetype(self) -> None:
		"""
		Check the filetype based on the file's MIME type.

		Determines the file type using MIME detection and initializes the corresponding processor.

		Raises:
			KeyError: If the file format is not recognized or unsupported.
		"""
		mime: magic.Magic = magic.Magic(mime=True)
		fileformat: str = mime.from_file(self._args['file'])
		self.__filetype: Optional[str] = self.__filetypes.get(fileformat, None)
		if self.__filetype is None:
			self.__utilities.logger.error(f"No such format: {fileformat} (file: {self._args['file']})")
			sys.exit(self.__error_msg)

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

	def __processing(self, stage : str):

		match stage:

			case "preprocessing":
				self.__check_args()
				self.__check_filetype()

				convert_args = {
					'file':			self._args['file'],
					'to_extension':	self._args['main_extension'],
					'output_dir':	self._args['temp_dir']
				}
				self.__convert(convert_args=convert_args)

			case "postprocessing":

				convert_args = {
					'file':			self._args['output_file'],
					'to_extension':	args['original_extension'],
					'output_dir':	self._args['outputs_dir']
				}
				self.__convert(convert_args=convert_args)
				self.__remove_temp()

	def run(self) -> None:
		"""
		Executes the specified action by processing an input file.

		This method follows a structured processing pipeline:
		-	Preprocessing Stage: 
				Validates the provided arguments and checks the file type.
				If necessary, converts the input file to an intermediate format.
		-	Action Execution: 
				Calls the method corresponding to the selected action 
				(e.g., 'links', 'json'), which processes the file.
		-	Postprocessing Stage: 
				Converts the processed file back to its original format 
				and performs cleanup by removing temporary files.

		The action to be executed is determined by `self.__called_arg`, which is dynamically
		retrieved using `getattr(self, self.__called_arg, None)`.
		"""
		self.__processing('preprocessing')
		self.__processor = XlsxProcessor(args=self._args)
		getattr(self, self.__called_arg, None)
		self.__processing('postprocessing')

	def __check_args(self) -> None:
		"""
		Validates input arguments and determines the action to perform.

		This function:
		- Ensures the specified file exists.
		- Determines the action (`links`, `routines`, `tags`, `json`, `duplicates`) based on provided arguments.
		- Validates argument dependencies and constraints for each action.
		- Raises an error and exits if arguments are missing or invalid.

		Raises:
			SystemExit: If arguments are invalid or no action is specified.
		"""
		properties:	set[str]		= {"links", "routines", "tags", "json", "duplicates"}
		end:		Optional[int]	= self._args.get("end", None)
		chunk:		int				= self._args.get("chunk", 0)
		start:		Optional[int]	= self._args.get("start", None)
		auto:		bool			= self._args.get("auto", False)
		output:		bool			= self._args.get("output", False)

		if not os.path.exists(self._args['file']):
			self.__utilities.logger.error(f"File {self._args['file']} does not exist")
			sys.exit(self.__error_msg)

		# Find the called argument
		self.__called_arg = next((key for key in self._args if key in properties and self._args[key] is True), None)
		if self.__called_arg is None: 
			self.__utilities.logger.error(f"No action specified. Use one of the following properties: {properties}")
			sys.exit(self.__error_msg)

		if end:
			# Validate range once
			try:
				result = isinstance(end, int) and end > start
			except Exception:
				self.__utilities.logger.error("Invalid inputs: --end and--start must be integers")
				sys.exit(self.__error_msg)

			if not result:
				self.__utilities.logger.error("Invalid range: --end must be greater than --start")
				sys.exit(self.__error_msg)

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
					self.__utilities.logger.error("Invalid combination for --links. Allowed combinations are: "
									"--start with --end, --start with --chunk, --auto with --chunk, or --auto alone.")
					sys.exit(self.__error_msg)

			# Ensure `--duplicates` does not allow `--start`, `--end`, `--chunk`, or `--auto`
			case "duplicates":
				if not output:
					self.__utilities.logger.error("--duplicates requires --output")
					sys.exit(self.__error_msg)

				if any(range_flags):
					self.__utilities.logger.error("Invalid combination for --duplicates. Cannot use with --start, --end, --chunk, or --auto.")
					sys.exit(self.__error_msg)

			# Ensure `--routines` requires `--start`
			case "routines":
				if start is None:
					self.__utilities.logger.error("--routines requires --start to be specified")
					sys.exit(self.__error_msg)
				if end is not None or chunk > 0 or auto:
					self.__utilities.logger.error("Invalid combination for --routines. It cannot be used with --end, --chunk, or --auto.")
					sys.exit(self.__error_msg)

			case "tags" | "json":
				if any(range_flags):
					self.__utilities.logger.error(f"Invalid combination for --{self.__called_arg}. Cannot use with --start, --end, --chunk, or --auto.")
					sys.exit(self.__error_msg)
				
def main(args: argparse.Namespace) -> None:
	"""
	Processes command-line arguments and executes the specified action.

	This function:
	- Ensures necessary directories (`inputs`, `outputs`, `temp`) exist.
	- Prepares file paths, handling input, temporary, and output file assignments.
	- Initializes the `ReadWatchLog` class and executes the requested action.

	Args:
		args (argparse.Namespace): Parsed command-line arguments.

	Raises:
		SystemExit: If no valid action is specified or arguments are invalid.

	Examples:
		>>> args = argparse.Namespace(links=True, file='Vault.xlsx', sheet='Vault', output=True)
		>>> main(args)  # Processes links from Vault.xlsx
	"""
	# Parse the arguments
	args = vars(args)

	# Check existence of directories
	directories = {key: key for key in ['inputs', 'outputs', 'temp']}
	[os.makedirs(d, exist_ok=True) for d in directories.values()]

	# Updating the 'args'
	only_filename, original_extension = os.path.splitext(args['file'])
	filename = args['file']
	main_extension = 'xlsx'
	output_file = os.path.join(directories['outputs'],
		f"{args['custom_name']}.{main_extension}" if args.get('output') else f"{only_filename}.{main_extension}"
	)
	args.update({
		'file':				  os.path.join(directories['inputs'], filename),
		'filename':			  filename,
		'inputs_dir':		  directories['inputs'],
		'main_extension':	  main_extension,
		'only_filename':	  only_filename,
		'original_extension': original_extension[1:],
		'output_file':		  output_file,
		'outputs_dir':		  directories['outputs'],
		'temp_dir':			  directories['temp'],
		'temp_file':		  os.path.join(directories['temp'], filename)
	})

	shutil.copy2(args['file'], args['temp_file'])
	args['temp_file'], args['file'] = args['file'], args['temp_file']

	del only_filename, original_extension, filename, main_extension, output_file

	# Initialize the ReadWatchLog class with the arguments
	rwl = ReadWatchLog(args=args)

	# Runs the action
	rwl.run()

if __name__ == '__main__':

	parser = argparse.ArgumentParser(description="Read-Watch-Log")

	Define the arguments
	parser.add_argument('--start', type=int, default=None, help='Start value')
	parser.add_argument('--end', type=int, default=None, help='End value')
	parser.add_argument('--output', action='store_true', help='Output flag. If true, output is created. Optional argument.')
	parser.add_argument('--file', type=str, required=True, default="Vault.xlsx", help='File name to process (XSLX/ODS formats). Required argument.')
	parser.add_argument('--custom_name', type=str, default=datetime.now().strftime("output_%d%m%Y%H%M%S%f"), help='Custom name of output file (without filetype)')
	parser.add_argument('--chunk', type=int, default=0, help='Chunk size. Algorithm processes only given number of records. Used with argument `links`.')
	parser.add_argument('--auto', action='store_true', help='Autosearch of a non-processed record. Used with argument `links`.')
	parser.add_argument('--sheet', type=str, required=True, default="Vault", help='Sheetname of the given document. Required argument.')

	Create a mutually exclusive group
	group = parser.add_mutually_exclusive_group()

	Add mutually exclusive arguments
	group.add_argument('--links', action='store_true', help='Get links')
	group.add_argument('--routines', action='store_true', help='Get routines')
	group.add_argument('--tags', action='store_true', help='Order tags')
	group.add_argument('--json', action='store_true', help='Convert to JSON')
	group.add_argument('--duplicates', action='store_true', help='Detect duplicates')

	main(args=parser.parse_args())