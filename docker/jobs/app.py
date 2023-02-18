import os
from datetime import datetime, date, timedelta
from quantutils.api.datasource import MIDataStoreRemote
import MIPriceAggregator.utils.store as priceStore

# run
if __name__ == '__main__':

    # Get environment variables
    MI_DATASTORE_LOCATION = os.getenv('MI_DATASTORE_LOCATION')
    MI_DATASOURCE_LOCATION = os.environ.get('MI_DATASOURCE_LOCATION')
    MI_PRICE_STORE_URL = os.environ.get('MI_PRICE_STORE_URL')

    ds_location = "../datasources/datasources.json"

    # Local Options
    mds = MIDataStoreRemote(location="http://pricestore.192.168.1.203.nip.io")
    priceStore.saveHistoricalOptionData(mds, ds_location, start=str(date.today()), end=str(date.today() + timedelta(days=1)), records=1, refreshUnderyling=True, debug=True)
    print("Updates complete at " + str(datetime.utcnow()))
