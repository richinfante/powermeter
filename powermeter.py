import signal
import subprocess
import json
import threading
import time
import argparse
import datetime
from datetime import datetime as dt
"""
Copyright 2021 Richard Infante

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""


"""
Starts, and manages an rtlamr instance:

https://github.com/bemasher/rtlamr
"""
class AMRProvider:
    def __init__(self):
        self.proc = None
        self.amr_proc = None
        self.read_thread = None
        self.known_sensors = {}

    # terminate sub-processes
    def stop(self):
        if self.proc:
            print("stopping rtl_tcp...")
            self.proc.send_signal(signal.SIGKILL)
        if self.amr_proc:
            print("stopping rtl_amr...")
            self.amr_proc.send_signal(signal.SIGINT)

    # get consumption metric for a meter. shortcut for get_meter_packet()['Message']['Consumption']
    def get_consumption_for_meter(self, meter_id):
      try:
          return self.known_sensors[meter_id].get('Message', {}).get('Consumption')
      except KeyError:
          return None

    # get latest meter packet
    def get_meter_packet(self, meter_id):
      try:
          return self.known_sensors[meter_id]
      except KeyError:
          return None

    # get latest meter message
    def get_meter(self, meter_id):
      try:
          return self.known_sensors[meter_id].get('Message')
      except KeyError:
          return None

    # open sub-processes
    def start(self):
        # start rtl_tcp - needed for rtlamr to run.
        self.proc = subprocess.Popen(["rtl_tcp"])
        print("starting rtl_tcp...")

        self.known_sensors = {}

        def read_amr_events():
            time.sleep(2)
            # "-msgtype=all" causes problems with rtl_tcp
            self.amr_proc = subprocess.Popen(
              ["rtlamr", "-format=json", '--msgtype=all'],
              stdout=subprocess.PIPE
            )

            # consume and parse all lines from rtl-amr's stdout.
            while True:
                output = self.amr_proc.stdout.readline()
                if len(output) > 0:
                    # write to out buffer
                    try:
                        # parse, and register the packet with the known_sensors array.
                        msg = json.loads(output)
                        ident = str(msg['Message']['ID'])
                        self.known_sensors[ident] = msg
                    except Exception as err:
                        print(err)
                else:
                    # poll to see if the cmd died yet. if it did, exit.
                    error_msg = self.amr_proc.poll()
                    if error_msg is not None:
                        break

            self.amr_proc.stdout.close()


        # the read thread processes in the background, independent of our main logic.
        # we need to call stop() to avoid leaving rtlamr and rtl_tcp running
        self.read_thread = threading.Thread(
          target=read_amr_events,
          args=[]
        )
        self.read_thread.start()

# main execution
parser = argparse.ArgumentParser(description='Collect and write AMR meter readings to a .prom file for prometheus')
parser.add_argument('--promfile', type=str, help="prometheus file destination for use with node exporter.")
parser.add_argument('--targets', nargs='+', help='meter id to search for')
parser.add_argument('--timeout', default=60, type=int, help='how long to wait before erroring out (seconds)')

args = parser.parse_args()

# create an amr provider
amr_provider = AMRProvider()
amr_provider.start()

started = dt.utcnow()


# signal handler to kill rtlamr and rtl_tcp
def signal_handler(sig, frame):
    print('trying to stop SDR...')
    amr_provider.stop()
    exit(0)


# register handler for ctrl+c
signal.signal(signal.SIGINT, signal_handler)

# main loop - wait for sensors.
while True:
    # handle timeout
    if dt.utcnow() - started > datetime.timedelta(seconds=args.timeout):
        amr_provider.stop()
        exit(1)

    # pause to not waste time doing the same thing repeatedly
    time.sleep(1)

    has_all = True

    # check that all the target meters have data
    # TODO: there's probably a better approach in case one meter dies.
    for meter in args.targets:
        data = amr_provider.get_meter(meter)
        packet = amr_provider.get_meter_packet(meter)

        # get usage + print
        if data and packet:
            consumption = data.get('Consumption')
            kind = packet.get('Type')
            print('consumption for %s (%s) = %s' % (meter, kind, consumption))
        else:
            # mark one as not ready
            has_all = False
            print('%s: not found' % meter)

    # if we have all, write to file for prometheus's node collector to scrape.
    if has_all:
        output = []

        # write info
        output.append('# HELP meter_consumption The consumption value for a smart meter.')
        output.append('# TYPE meter_consumption counter')

        # write metrics
        for meter in args.targets:
            data = amr_provider.get_meter(meter)
            packet = amr_provider.get_meter_packet(meter)
            if data and packet:
                consumption = data.get('Consumption')
                kind = packet.get('Type')

                output.append("meter_consumption{meter_id=%s, meter_type=%s} %s" % (
                    json.dumps(meter),
                    json.dumps(kind),
                    consumption
                ))
                print('wrote consumption for %s (%s) = %s' % (meter, kind, consumption))

        # blank line at end of file
        output.append('')
        print("final file: ")
        print('\n'.join(output))

        # write file
        f = open(args.promfile, 'w+')
        f.write('\n'.join(output))
        f.close()

        # clean up + end.
        amr_provider.stop()
        exit(0)
