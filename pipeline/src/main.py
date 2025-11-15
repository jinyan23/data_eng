from datetime import datetime as dt


from util import load_config
from util import UDLogger
from lta.pv_train import PVTrain
from loaders.import_pv_train import import_pv_train

# create logger
ud_logger = UDLogger(filename='main.log', name=__name__)
logger = ud_logger.create_logger()

config = load_config('config.yaml')
config_pv_train = load_config('lta_pv_train.yaml')['config_pv_train']


def main():

    curr_date = dt.now().date()
    # do api call to lta to get zip link
    pv_train = PVTrain(curr_date)
    dl_link = pv_train.api_call()
    pv_train.download_zip(dl_link, config['incoming']['pv_train'] + '/zip')

    pv_train.unzip_to_incoming(
        config_pv_train['zip_prefix'],
        config_pv_train['csv_prefix'],
        config_pv_train['arc_prefix'],
    )

    # load csv into mariadb
    import_pv_train()


if __name__ == '__main__':

    main()
