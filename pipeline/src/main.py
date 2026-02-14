from datetime import datetime as dt


from util import load_config
from util import UDLogger
from lta.pv_train import PVTrain
from lta.pv_bus import PVBus
from loaders.import_pv_train import import_pv_train
from loaders.import_pv_bus import import_pv_bus

# create logger
ud_logger = UDLogger(filename='main.log', name=__name__)
logger = ud_logger.create_logger()

config = load_config('config.yaml')
config_pv_train = load_config('lta_pv_train.yaml')['config_pv_train']
config_pv_bus = load_config('lta_pv_bus.yaml')['config_pv_bus']


def main():

    curr_date = dt.now().date()

    # do api call to lta to get zip link
    pv_train = PVTrain(curr_date)
    pv_bus = PVBus(curr_date)

    # step 1: api call
    dl_link = pv_train.api_call()
    pv_train.download_zip(dl_link, config['incoming']['pv_train'] + '/zip')
    dl_link = pv_bus.api_call()
    pv_bus.download_zip(dl_link, config['incoming']['pv_bus'] + '/zip')

    # step 2: file transfer
    pv_train.unzip_to_incoming(
        config_pv_train['zip_prefix'],
        config_pv_train['csv_prefix'],
    )
    pv_bus.unzip_to_incoming(
        config_pv_bus['zip_prefix'],
        config_pv_bus['csv_prefix'],
    )

    # step 3: load csv into mariadb
    import_pv_train()
    import_pv_bus()


if __name__ == '__main__':

    main()
