import os
import json
from datetime import datetime, date, timedelta
from quantutils.api.datasource import MIDataStoreRemote
import MIPriceAggregator.utils.store as priceStore
from MIPriceAggregator.api.aggregator import MarketDataAggregator

# run
if __name__ == '__main__':

    # Get environment variables
    MI_DATASTORE_LOCATION = os.getenv('MI_DATASTORE_LOCATION')
    MI_DATASOURCE_LOCATION = os.environ.get('MI_DATASOURCE_LOCATION')
    MI_PRICE_STORE_URL = os.environ.get('MI_PRICE_STORE_URL')

    ds_location = "../datasources/datasources.json"
    with open(ds_location) as json_file:
        ds_config = json.load(json_file)

    # Get data
    aggregator = MarketDataAggregator(ds_config)
    data = aggregator.getData(aggregate=False)

    # Save data
    mds = MIDataStoreRemote(location="http://pricestore.192.168.1.203.nip.io")
    savedData = priceStore.saveData(mds, data, dry_run=True, delta=True, debug=False,)

    print(data)
    print(savedData)

    print("Updates complete at " + str(datetime.utcnow()))
