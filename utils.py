import sys, win32com.client
import re
import os
import subprocess
import time
import pandas as pd
import itertools
from xlsx2csv import Xlsx2csv
from joblib import Parallel, delayed

n_jobs=-3

