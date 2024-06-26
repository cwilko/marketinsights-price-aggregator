import os
import json
from datetime import datetime, date, timedelta
from marketinsights.remote.datastore import MIDataStoreRemote
import marketinsights.utils.store as priceStore

# run
if __name__ == '__main__':

    # Get environment variables
    MI_DATASTORE_LOCATION = os.getenv('MI_DATASTORE_LOCATION')
    MI_DATASOURCE_LOCATION = os.environ.get('MI_DATASOURCE_LOCATION')
    MI_PRICE_STORE_URL = os.environ.get('MI_PRICE_STORE_URL')

    ds_location = "../datasources/datasources.json"
    with open(ds_location) as json_file:
        ds_config = json.load(json_file)

    # Local Options
    mds = MIDataStoreRemote(location="http://pricestore.192.168.1.203.nip.io")
    priceStore.saveHistoricalOptionData(mds, ds_config, start=str(date.today()), end=str(date.today() + timedelta(days=1)), records=1, refreshUnderyling=True, debug=False)
    print("Updates complete at " + str(datetime.utcnow()))
