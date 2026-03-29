import time
import regex as re
import os
import pandas as pd
import numpy as np
import argparse
from datetime import datetime
from natsort import natsorted

def common_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--dataset', type=str,
                        help="Set this as dataset name",
                        default=None, required=True)
    parser.add_argument('-o', '--output', type=str,
                        help="Set this as output folder name",
                        default=None, required=True)
    parser.add_argument('-i', '--input', type=str,
                        help="Set this as input folder name",
                        default=None, required=True)
    parser.add_argument('-n', '--sample', type=int, default=1,
                        help="Set this as log sample size for template, default is 1")
    args = parser.parse_args()
    return args