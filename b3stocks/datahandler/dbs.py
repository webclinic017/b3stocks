import os
import requests
import pandas as pd
from io import StringIO
from .utils import (_init_session, _format_date,
                     _sanitize_dates, _url, RemoteDataError,
                    _is_cached)
from .cfg import (exchange_data, currencies_data,
                 index_data)

EOD_HISTORICAL_DATA_API_KEY_ENV_VAR = "EOD_HISTORICAL_API_KEY"
EOD_HISTORICAL_DATA_API_KEY_DEFAULT = "5f73e6871488b8.86223894"
EOD_HISTORICAL_DATA_API_URL = "https://eodhistoricaldata.com/api"


def get_api_key(env_var=EOD_HISTORICAL_DATA_API_KEY_ENV_VAR):
    """
    Returns API key from environment variable
    API key must have been set previously
    bash> export EOD_HISTORICAL_API_KEY="YOURAPI"
    Returns default API key, if environment variable is not found
    """
    return os.environ.get(env_var, EOD_HISTORICAL_DATA_API_KEY_DEFAULT)


def get_eod_data(symbol, exchange, start=None, end=None,
                 api_key=EOD_HISTORICAL_DATA_API_KEY_DEFAULT,
                 session=None):
    """
    Returns EOD (end of day data) for a given symbol
    """
    symbol_exchange = symbol + "." + exchange
    session = _init_session(session)
    start, end = _sanitize_dates(start, end)
    endpoint = "/eod/{symbol_exchange}".format(symbol_exchange=symbol_exchange)
    url = EOD_HISTORICAL_DATA_API_URL + endpoint
    params = {
        "api_token": api_key,
        "from": _format_date(start),
        "to": _format_date(end)
    }
    r = session.get(url, params=params)
    if r.status_code == requests.codes.ok:
        df = pd.read_csv(StringIO(r.text), skipfooter=1,
                         parse_dates=[0], index_col=0)
        return df
    else:
        params["api_token"] = "YOUR_HIDDEN_API"
        raise RemoteDataError(r.status_code, r.reason, _url(url, params))


def get_dividends(symbol, exchange, start=None, end=None,
                  api_key=EOD_HISTORICAL_DATA_API_KEY_DEFAULT,
                  session=None):
    """
    Returns dividends
    """
    symbol_exchange = symbol + "." + exchange
    session = _init_session(session)
    start, end = _sanitize_dates(start, end)
    endpoint = "/div/{symbol_exchange}".format(symbol_exchange=symbol_exchange)
    url = EOD_HISTORICAL_DATA_API_URL + endpoint
    params = {
        "api_token": api_key,
        "from": _format_date(start),
        "to": _format_date(end)
    }
    r = session.get(url, params=params)
    if r.status_code == requests.codes.ok:
        df = pd.read_csv(StringIO(r.text), skipfooter=1,
                         parse_dates=[0], index_col=0)
        assert len(df.columns) == 1
        ts = df["Dividends"]
        return ts
    else:
        params["api_token"] = "YOUR_HIDDEN_API"
        raise RemoteDataError(r.status_code, r.reason, _url(url, params))


def get_exchange_symbols(exchange_code,
                         api_key=EOD_HISTORICAL_DATA_API_KEY_DEFAULT,
                         session=None):
    """
    Returns list of symbols for a given exchange
    """
    session = _init_session(session)
    endpoint = "/exchanges/{exchange_code}".format(exchange_code=exchange_code)
    url = EOD_HISTORICAL_DATA_API_URL + endpoint
    params = {
        "api_token": api_key
    }
    r = session.get(url, params=params)
    if r.status_code == requests.codes.ok:
        df = pd.read_csv(StringIO(r.text), skipfooter=1, index_col=0)
        return df
    else:
        params["api_token"] = "YOUR_HIDDEN_API"
        raise RemoteDataError(r.status_code, r.reason, _url(url, params))


def get_exchanges():
    """
    Returns list of exchanges
    https://eodhistoricaldata.com/knowledgebase/list-supported-exchanges/
    """
    df = pd.read_csv(StringIO(exchange_data), sep="\t")
    df = df.set_index("ID")
    return(df)


def get_currencies():
    """
    Returns list of supported currencies
    https://eodhistoricaldata.com/knowledgebase/list-supported-currencies/
    """
    df = pd.read_csv(StringIO(currencies_data), sep="\t")
    df = df.set_index("ID")
    return(df)


def get_indexes():
    """
    Returns list of supported indexes
    https://eodhistoricaldata.com/knowledgebase/list-supported-indexes/
    """
    df = pd.read_csv(StringIO(data), sep="\t")
    df = df.set_index("ID")
    return(df)


def get_eod_all(exchange, out_path):
    """
    #TODO: Writing doc string
    """

    symbols = get_exchange_symbols(exchange)
    for symbol in symbols.index:
        df = dbs.get_eod_data(symbol, exchange)
        df.to_csv("{}/{}.csv".format(out_path, symbol))
    return None

def update_last(path, symbol, exchange="SA"):
    """

    """

    if _is_cached(path, symbol):
        pass
    else:
        ddf = dbs.get_eod_data(symbol, exchange)
        df.to_csv("{}/{}.csv".format(out_path, symbol))
        return None


