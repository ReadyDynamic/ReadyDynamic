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
    parser.add_argument('-p', '--parser', type=str,
                        help="Set the name of the parser",
                        default=None, required=True)
    parser.add_argument('-n', '--sample', type=int, default=10,
                        help="Set this as log sample size for template, default is 10")
    args = parser.parse_args()
    return args