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

def main():
    if len(sys.argv) != 3:
        print("telemetryData.py [measure] [date (yyyymmdd)] [telemetry mapreduce directory]")
        sys.exit(1)
    measure = sys.argv[1]
    date = sys.argv[2]

    uid = uuid.uuid4().fields[-1]

    histogram = getHisto();
    print(histogram[measure])

    f = open('filter_' + str(uid) + '.json', 'a')
#    f.write(genFilter('20140526','20140526'))
    f.close()






    startMapReduce()


    pass

#if __name__ == 'main':
main()
