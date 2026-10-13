"""Bounded read-only EncoreDB adapter. No connection occurs on import."""
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

START = date(2024, 10, 1)
MAX_ROWS = 1600
STOCKS = tuple(sorted(('INTC','DELL','ZTS','CHWY','T','TMUS','CHTR','CMCSA','VZ','SPCX','UNH','HUM','ELV','CNC','MOH')))

class DataUnavailable(Exception):
    """Public-safe error; never wrap raw driver exceptions for display."""

IDENTITY_SQL = '''SELECT instrument_id,ticker,name,asset_class,currency,bbg_security,active_flag
FROM encoredb.instruments WHERE upper(btrim(ticker))=%s ORDER BY instrument_id LIMIT 3'''
PRICES_SQL = '''SELECT trade_date,close_price,volume FROM encoredb.equity_daily_prices
WHERE instrument_id=%s AND trade_date BETWEEN %s AND %s
ORDER BY trade_date LIMIT %s'''

def completed_day():
    # Exclude the current New York day. The latest row, not this bound, is the freshness date.
    return datetime.now(ZoneInfo('America/New_York')).date() - timedelta(days=1)

def number(value, positive=False):
    if value is None:
        return None
    if isinstance(value, bool):
        raise DataUnavailable('Invalid numeric data returned; no report produced.')
    try:
        d = Decimal(str(value))
    except Exception:
        raise DataUnavailable('Invalid numeric data returned; no report produced.') from None
    if not d.is_finite() or d < 0 or (positive and d == 0):
        raise DataUnavailable('Invalid numeric data returned; no report produced.')
    return str(d)

def read_stock(settings, ticker, end=None, connector=None):
    if ticker not in STOCKS:
        raise DataUnavailable('Stock is outside this dashboard’s selected universe.')
    end = end or completed_day()
    if type(end) is not date or not START <= end <= completed_day():
        raise DataUnavailable('Invalid date boundary.')
    # Match the existing dashboards' [db] secrets, but enforce TLS and read-only operations.
    allowed = {'host','port','dbname','database','user','password','sslmode','sslrootcert','sslcert','sslkey'}
    settings = dict(settings)
    if set(settings) - allowed or not {'host','user','password'} <= settings.keys():
        raise DataUnavailable('Database settings are missing or unsupported. Contact the app owner.')
    if 'database' in settings:
        if 'dbname' in settings:
            raise DataUnavailable('Specify dbname or database, not both.')
        settings['dbname'] = settings.pop('database')
    if not settings.get('dbname'):
        raise DataUnavailable('Database name is required.')
    if settings.get('sslmode','require') not in ('require','verify-ca','verify-full'):
        raise DataUnavailable('TLS is required for the database connection.')
    settings.setdefault('sslmode','require')
    settings.update(connect_timeout=10, application_name='scfr_dashboard_readonly')
    conn = None
    result = None
    error = None
    try:
        if connector is None:
            import psycopg2
            connector = psycopg2.connect
        conn = connector(**settings)
        if getattr(getattr(conn,'info',None),'ssl_in_use',None) is not True:
            raise DataUnavailable('Database TLS could not be confirmed.')
        conn.set_session(readonly=True, autocommit=False, isolation_level='REPEATABLE READ')
        with conn.cursor() as cur:
            for sql in ("SET LOCAL statement_timeout = '10000ms'", "SET LOCAL lock_timeout = '2000ms'", "SET LOCAL idle_in_transaction_session_timeout = '30000ms'"):
                cur.execute(sql)
            cur.execute("SELECT current_database(),current_setting('transaction_read_only')")
            guard = cur.fetchone()
            if guard != (settings['dbname'], 'on'):
                raise DataUnavailable('Read-only database identity check failed.')
            cur.execute(IDENTITY_SQL, (ticker,))
            identities = cur.fetchmany(3)
            if not identities:
                raise DataUnavailable('This stock has no EncoreDB mapping.')
            if len(identities) != 1:
                raise DataUnavailable('This stock has ambiguous EncoreDB mappings.')
            i, stored_ticker, name, asset, currency, bbg, active = identities[0]
            if (type(i) is not int or i <= 0 or str(stored_ticker).strip().upper() != ticker
                or str(asset).lower() not in ('equity','stock','common stock') or currency != 'USD'
                or active is not True or bbg != ticker+' US Equity'):
                raise DataUnavailable('The stored identity is outside the expected active USD equity scope.')
            cur.execute(PRICES_SQL, (i,START,end,MAX_ROWS+1))
            raw = cur.fetchmany(MAX_ROWS+1)
            if len(raw) > MAX_ROWS:
                raise DataUnavailable('History exceeds the bounded read. Contact the app owner.')
            rows = []
            previous = None
            for day, close, volume in raw:
                if type(day) is not date or not START <= day <= end or (previous and day <= previous):
                    raise DataUnavailable('Duplicate or invalid observation dates.')
                rows.append({'date':day.isoformat(),'close':number(close,True),'volume':number(volume)})
                previous = day
            result = {'ticker':ticker,'instrument_id':i,'name':name,'bbg_security':bbg,
                      'rows':rows,'requested_end':end.isoformat(),
                      'queried_at':datetime.now(timezone.utc).isoformat()}
    except DataUnavailable as exc:
        error = exc
    except Exception:
        error = DataUnavailable('EncoreDB could not be read. Check the private connection settings and network access; no automatic retry was made.')
    finally:
        if conn is not None:
            for operation in ('rollback','close'):
                try:
                    getattr(conn,operation)()
                except Exception:
                    error = error or DataUnavailable('Database cleanup could not be confirmed; results withheld.')
    if error:
        raise error from None
    return result
