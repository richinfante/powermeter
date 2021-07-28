import signal
import subprocess
import json
import threading
import time
import argparse
import datetime
from datetime import datetime as dt

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

    def stop(self):
        if self.proc:
            print("stopping rtl_tcp...")
            self.proc.send_signal(signal.SIGKILL)
        if self.amr_proc:
            print("stopping rtl_amr...")
            self.amr_proc.send_signal(signal.SIGINT)

    def get_consumption_for_meter(self, meter_id):
      try:
          return self.known_sensors[meter_id].get('Message', {}).get('Consumption')
      except KeyError:
          return None

    def get_meter_packet(self, meter_id):
      try:
          return self.known_sensors[meter_id]
      except KeyError:
          return None


    def get_meter(self, meter_id):
      try:
          return self.known_sensors[meter_id].get('Message')
      except KeyError:
          return None

    def start(self):
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

            while True:
                output = self.amr_proc.stdout.readline()
                if len(output) > 0:
                    # write to out buffer
                    try:
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

        self.read_thread = threading.Thread(
          target=read_amr_events,
          args=[]
        )
        self.read_thread.start()


parser = argparse.ArgumentParser(description='Collect and write AMR meter readings to a .prom file for prometheus')
parser.add_argument('--promfile', type=str, help="prometheus file destination for use with node exporter.")

parser.add_argument('--targets', nargs='+', help='meter id to search for')
parser.add_argument('--timeout', default=60, type=int, help='how long to wait before erroring out (seconds)')

args = parser.parse_args()

amr_provider = AMRProvider()
amr_provider.start()

started = dt.utcnow()

def signal_handler(sig, frame):
    print('trying to stop SDR...')
    amr_provider.stop()
    exit(0)

signal.signal(signal.SIGINT, signal_handler)
print('Press Ctrl+C')

while True:

    if dt.utcnow() - started > datetime.timedelta(seconds=args.timeout):
      amr_provider.stop()
      exit(1)

    time.sleep(1)

    has_all = True
    for meter in args.targets:
      data = amr_provider.get_meter(meter)
      packet = amr_provider.get_meter_packet(meter)
      if data and packet:
        consumption = data.get('Consumption')
        kind = packet.get('Type')

        print('consumption for %s (%s) = %s' % (meter, kind, consumption))
      else:
        has_all = False
        print('%s: not found' % meter)

    if has_all:
      output = []
      output.append('# HELP meter_consumption The consumption value for a smart meter.')
      output.append('# TYPE meter_consumption counter')
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

      output.append('')
      print("final file: ")
      print('\n'.join(output))

      f = open(args.promfile, 'w+')
      f.write('\n'.join(output))
      f.close()
      amr_provider.stop()
      exit(0)

