"""Some fancy stuff with Docker and Pandas"""

import sys
import pandas as pd

date = sys.argv[1]
version = pd.__version__

print(f"Pipeline completed successfully on {date}")
