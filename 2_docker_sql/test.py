import os

from dotenv import load_dotenv
import argparse

load_dotenv()

API_KEY = os.getenv("API_KEY")

parser = argparse.ArgumentParser(description=None)

parser.add_argument("filename", help="Position Argument 1")
parser.add_argument(
    "-c", "--copy", help="Optional Argument 1", metavar="text", type=int
)
parser.add_argument("-s", "--something", action="store_const", const=API_KEY)
parser.add_argument(
    "-e", "--else", action="store_true"
)  # namespace returns true if flag is included
parser.add_argument("-n", "--name", default="file_copy")
parser.add_argument("-o", "--options", choices=["option_1", "option_2"])


arguments = parser.parse_args()
print(arguments)
print(arguments.filename)
