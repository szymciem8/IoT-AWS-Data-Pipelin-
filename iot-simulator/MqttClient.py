import json
from keys import *
import logging
from paho.mqtt.client import Client
import pandas as pd
import ssl
import threading


logging.basicConfig(filename='logs/simulator.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')


class MqttAWSClient(Client):

    def __init__(self):
        super().__init__()
        self.topic = 'pico_test'
        self.tls_set(ROOT_FILE, certfile=CERTFILE, keyfile=KEYFILE, cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLSv1_2, ciphers=None)
        self.tls_insecure_set(True)
        self.connect(AWS_IOT_HOST, 8883, 60)


class Device:

    def __init__(self, df):
        self.df = df
        self.df['date'] = pd.to_datetime(self.df['date'])
        self.device_id = df['device'].unique()[0]
        # self.mqtt_client = MqttAWSClient()

    def send_data_to_aws(self, mqtt_client, topic, data):
        mqtt_client.publish('device/'+topic, json.dumps(data), qos=1)
        # logging.info(f'Sending data for device {self.device_id} to AWS')

    def get_data(self, date):
        data = self.df[self.df['date'] == date].to_dict(orient='records')
        if not data:
            return None
        data[0]['date'] = data[0]['date'].strftime('%Y-%m-%d')
        return data[0]


class AllDevices:

    def __init__(self, xlsx_file):
        self.devices = self._create_devices(xlsx_file)  
        self.start_date = pd.to_datetime('1/1/2015')
        self.end_date = pd.to_datetime('9/9/2015')
        self.mqtt_client = MqttAWSClient()
        self.current_date = self.start_date


    def send_all_data_date_by_date(self, batch_size=25):
        current_date = self.start_date
        end_date = self.end_date
        period = pd.DateOffset(days=batch_size)

        while current_date <= end_date:
            print('Sending data for date: ', current_date.strftime('%m/%d/%Y'), ' to ', (current_date + period).strftime('%m/%d/%Y'))
            self.send_batch_date_by_date(current_date, current_date + period)
            current_date += period


    def send_batch_date_by_date(self, start_date, end_date):
        threads = []
        current_date = start_date
        while current_date <= end_date:
            if current_date > end_date:
                break

            x = threading.Thread(target=self.send_data_to_aws_iot, args=([current_date]), daemon=True)
            threads.append(x)
            x.start()
            current_date += pd.DateOffset(days=1)

        for thread in threads:
            thread.join()


    def send_data_to_aws_iot(self, date):
        mqtt_client = MqttAWSClient()

        for device, device_obj in self.devices.items():
            data = device_obj.get_data(date.strftime('%Y-%m-%d'))
            if data:
                device_obj.send_data_to_aws(mqtt_client, device, data)


    def _create_devices(self, xlsx_file):
        '''
        Creates a dictionary of devices from the xlsx file
        '''
        print('Creating devices...')
        device_dfs = self._load_data(xlsx_file)
        devices = {}
        for device, df in device_dfs.items():
            devices[device] = Device(df)
        print('Devices created!')
        return devices


    def _load_data(self, xlsx_file):
        '''
        Loads data from the xlsx file
        '''
        device_dfs = {}
        with pd.ExcelFile(xlsx_file) as xls:
            distict_devices = xls.sheet_names
            for device in distict_devices:
                device_dfs[device] = pd.read_excel(xls, sheet_name=device)
        return device_dfs
        

if __name__ == '__main__':
    all_devices = AllDevices('data/predictive_maintenance_dataset.xlsx')
    all_devices.send_all_data_date_by_date()