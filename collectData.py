'''
collectData.py - get large chunks of data
Ben Brittain (bbrittain)
'''
import sys
import subprocess
import datetime
import os
import json as json
import argparse
import Queue
import threading

def main():
  parser = argparse.ArgumentParser(description='Get Telemetry Data.',
      formatter_class=argparse.ArgumentDefaultsHelpFormatter)
  parser.add_argument("measure", help="Telemetry prope to measure")
  parser.add_argument("date", help="Day to get data for (yyyymmdd)")
  parser.add_argument("ndays", help="days back to get", type=int)
#  parser.add_argument("-b", "--branch", help="aurora, beta, release, nightly", default="*")
#  parser.add_argument("-p", "--platform", help="Fennec, Firefox, etc...", default="*")
#  parser.add_argument("-v", "--version", help="28.0, 30.0, etc...", default="*")
  args = parser.parse_args()

  branches = ['nightly', 'beta', 'aurora', 'release']
  print(int(args.date[0:2]))
  day = datetime.date(int(args.date[0:4]), int(args.date[4:6]), int(args.date[6:8]))
  delta = datetime.timedelta(days=-1)
  for x in range(0, args.ndays):
    day = day + delta
    for branch in branches:
      print('getting ' + branch + ' on ' + str(day))
      call = "python getData.py " + args.measure + " " + day.strftime("%Y%m%d") + " -p Firefox " + "-b " + branch + ""
      print(call)
      subprocess.check_call([call])

if __name__ == "__main__":
  main()
