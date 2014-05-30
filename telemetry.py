import subprocess
import os
import uuid
import sys
import urllib.request
import urllib.parse
import json


def getHisto():
    url = 'http://hg.mozilla.org/mozilla-central/raw-file/tip/toolkit/components/telemetry/Histograms.json'
    f = urllib.request.urlopen(url)
    return json.loads(f.read().decode('utf-8'))


def genFilter(startDay, endDay):
    return json.dumps({"version": 1, "dimensions":
        [{"field_name": "reason", "allowed_values": ["saved_session"]},
        {"field_name": "appName", "allowed_values": ["Firefox"]}, {"field_name": "appUpdateChannel", "allowed_values": "*"},
        {"field_name": "appVersion", "allowed_values": "*"}, {"field_name": "appBuildID", "allowed_values": "*"},
        {"field_name": "submission_date", "allowed_values": {"min":startDay, "max":endDay}}]},indent=2)

#def createMapReduce():

def startMapReduce(functionFile, filterFile, cached):
    command = "python -m mapreduce.job " + functionFile + " --input-filter " + filterFile + " "
    command = command +  "--num-mappers 16 --num-reducers 4 --work-dir /mnt/telemetry/work --output /mnt/telemetry/my_mapreduce_results.out --bucket 'telemetry-published-v1' "
    if not cached:
        command = command + " --local-only \
            --data-dir /mnt/telemetry/work/cache"
    else:
        command = command + "--data-dir /mnt/telemetry/work"

    print(command)

    p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    for line in p.stdout.readlines():
        print(line)
    retval = p.wait()

def makeMapReduce(measure):
    fout = "import json \ndef map(k, d, v, cx):\n\tj = json.loads(v)\n\tif u\'WEBRTC_ICE_SUCCESS_RATE\' in j['histograms'].keys():\n\t\thisto = json.dumps(j['histograms'][u'WEBRTC_ICE_SUCCESS_RATE'])\n\t\tversion = j['info']['appUpdateChannel']\n\t\tcx.write(histo, version)"
    return fout

def main():
    if len(sys.argv) != 3:
        print("telemetryData.py [measure] [date (yyyymmdd)] [telemetry mapreduce directory]")
        sys.exit(1)
    measure = sys.argv[1]
    date = sys.argv[2]

    uid = uuid.uuid4().fields[-1]

    histogram = getHisto();
    print(histogram[measure])

    if not (os.path.isdir("tmp")):
        os.makedirs("tmp")


    filterFile = 'tmp/filter_' + str(uid) + '.json'
    f = open(filterFile, 'a')
    f.write(genFilter('20140526','20140526'))
    f.close()

    functionFile = 'tmp/mapreduce_' + str(uid) + '.py'
    f = open(functionFile, 'a')
    f.write(makeMapReduce(measure))
    f.close()

    startMapReduce(functionFile, filterFile, True)


    pass

#if __name__ == 'main':
main()
