#!/usr/bin/python3

from datetime import datetime
from rwl_base import BaseProcessor
from rwl_ods import OdsProcessor
from rwl_xlsx import XlsxProcessor
import argparse
import magic
import sys

class ReadWatchLog:
	"""
	Processes spreadsheet files and extracts information using the appropriate processor.

	This class determines the file type, selects the corresponding processor,
	and delegates processing tasks to it.

	Attributes:
		__filetypes (dict): A mapping of MIME types to supported file extensions.
		__processors (dict): A mapping of file extensions to their corresponding processor classes.
		__args (dict): Dictionary containing input arguments such as file path, sheet name, and processing options.
		__processor (BaseProcessor): An instance of the selected processor class for handling the file.
	"""
	def __init__(self, args: dict):
		"""
		Initialize the aggregator with file details and select the appropriate processor.
		
		Args:
			file (str): Path to the spreadsheet file.
			sheetname (str): Name of the sheet to process.
			output (bool): Whether to save to a new file.
			custom_name (str, optional): Custom name for output file.
			chunk (int, optional): Number of rows to process.
		
		Raises:
			ValueError: If the file format is unsupported.
		"""
		self.__filetypes = {
			'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': 'xlsx',
			'application/vnd.oasis.opendocument.spreadsheet': 'ods'
		}
		self.__processors = {
			'xlsx': XlsxProcessor,
			'ods': OdsProcessor
		}
		self.__args = args
		self.__processor = self.__select_processor()

	def __select_processor(self) -> BaseProcessor:
		"""
		Selects the appropriate processor based on the file's MIME type.

		Determines the file type using MIME detection and initializes the corresponding processor.

		Returns:
			BaseProcessor: An instance of the selected processor.

		Raises:
			KeyError: If the file format is not recognized or unsupported.
		"""
		filename = self.__args['file']
		mime = magic.Magic(mime=True)
		fileformat = mime.from_file(filename)
		filetype = self.__filetypes.get(fileformat, None)
		if filetype is None:
			raise KeyError(f"No such format: {fileformat} (file: {filename})")
		
		return self.__processors[filetype](args=self.__args)

	@property
	def links(self) -> dict | None:
		"""
		Retrieves extracted links from the selected processor.

		Delegates the call to the `links` method of the selected processor instance.

		Returns:
			dict | None: A dictionary containing extracted links if available, otherwise None.
		"""
		return self.__processor.links()



def main(args) -> None:
	"""
	Processes command-line arguments and executes the specified action.

	Args:
		args: An object containing command-line arguments, typically from argparse.Namespace.

	Raises:
		SystemExit: If no valid action is specified in the arguments.

	Examples:
		>>> args = argparse.Namespace(links=True, routines=False, tags=False, json=False, duplicates=False)
		>>> main(args)  # Executes the 'links' action using ReadWatchLog
	"""
	properties = {
		"links",
		"routines",
		"tags",
		"json",
		"duplicates"
	}

	# Find the called argument
	called_arg = next((arg for arg, value in vars(args).items() if arg in properties), None)
	if called_arg is None: sys.exit(f"No action specified. Use one of the following properties: {properties}")

	# Parse the arguments
	args = vars(args)

	# Initialize the ReadWatchLog class with the arguments
	rwl = ReadWatchLog(args=args)

	# Getting a property
	getattr(rwl, called_arg, None)

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

	# args = {"links": True, "start": 41000, "end": 41100, "output": True, "chunk": 0, "file": "Vault.xlsx"}

	main(args=parser.parse_args())