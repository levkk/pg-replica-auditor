'''Check that two tables on two databases have reasonably identical rows.
Assumes that both tables have "id" and "updated_at" columns and indexes on those columns.'''
import psycopg2
import os
from tqdm import tqdm
from colorama import Fore
import colorama
import click
import psycopg2.extras
import random

colorama.init()

ROWS = 8128
VERSION = '0.2.3'

__version__ = VERSION
__author__ = 'Lev Kokotov <lev.kokotov@instacart.com>'


def _debug(cursor):
    '''Print the executed query in a pretty color.'''
    if os.getenv('DEBUG'):
        print(Fore.BLUE, '\b{}: '.format(cursor.connection.dsn) + cursor.query.decode('utf-8'), Fore.RESET)


def _debug2(text):
    if os.getenv('DEBUG'):
        print(Fore.BLUE, '\b{}'.format(text), Fore.RESET)


def connect():
    '''Connect to source and replicaination DBs.'''
    primary = psycopg2.connect(os.getenv('PRIMARY_DB_URL'))
    replica = psycopg2.connect(os.getenv('REPLICA_DB_URL'))

    return primary, replica


def _minmax(cursor, table):
    assert table is not None

    cursor.execute('SELECT MIN(id) AS "min", MAX(id) as "max" FROM {}'.format(table))
    _debug(cursor)

    result = cursor.fetchone()

    return result['min'], result['max']


def _get(cursor, table, id_):
    assert id_ is not None
    assert table is not None

    query = 'SELECT * FROM {table} WHERE id = %s LIMIT 1'.format(table=table)

    cursor.execute(query, (id_,))
    _debug(cursor)

    return cursor.fetchone()


def _exec(cursor, query, params=tuple()):
    cursor.execute(query, params)
    _debug(cursor)

    return cursor


def _pick(cursor, table, mi, ma):
    assert mi is not None
    assert ma is not None

    result = None
    attempts = 0

    while result is None and attempts < 3:
        id_ = random.randint(mi, ma)
        result = _get(cursor, table, id_)
        attempts += 1

    return result


def _result(checked, skipped):
    print(Fore.GREEN, '\bChecked: {}'.format(checked), Fore.RESET)
    print(Fore.GREEN, '\bSkipped: {}'.format(skipped), Fore.RESET)


def _result2(text):
    print(Fore.GREEN, '\b{}'.format(text), Fore.RESET)


def _error(p, r):
    id_ = p['id']
    print(Fore.RED, '\bRows at id = {} are different'.format(id_), Fore.RESET)
    print('primary: {}'.format(p))
    print('replica: {}'.format(r))
    exit(1)


def _error2(text):
    print(Fore.RED, '\b{}'.format(text), Fore.RESET)
    exit(1)


def _announce(name, table):
    print(Fore.YELLOW, '\bRunning check "{}" on table "{}"'.format(name, table), Fore.RESET)


def randcheck(primary, replica, table, rows):
    _announce('random check', table)
    rmin, rmax = _minmax(replica, table)

    checked = 0
    skipped = 0
    for _ in tqdm(range(rows)):
        r = _pick(replica, table, rmin, rmax)
        if r is None:
            skipped += 1
            continue
        p = _get(primary, table, r['id'])
        if p is None:
            raise Exception('Replica is "ahead" of the primary, it has rows primary does not.')
        assert r['id'] == p['id']
        _debug2('Comparing id = {}'.format(r['id']))
        if dict(p) != dict(r):
            _error(dict(p), dict(r))
        checked += 1
    _result(checked, skipped)


def last_1000(primary, replica, table):
    _announce('last 1000', table)
    _, rmax = _minmax(replica, table)
    rmin = rmax - 1000

    checked = 0
    skipped = 0
    for id_ in tqdm(range(rmin, rmax)):
        p = _get(primary, table, id_)
        r = _get(replica, table, id_)
        if p is None or r is None:
            skipped += 1
            continue
        assert p['id'] == r['id']
        if dict(p) != dict(r):
            _error(dict(p), dict(r))
        checked += 1
    _result(checked, skipped)


def lag(primary, replica, table):
    '''Check logical lag between primary and replica table using Django/Rails "updated_at".'''
    _announce('replica lag', table)
    query = 'SELECT MAX(updated_at) AS "updated_at" FROM "{table}"'.format(table=table)
    primary = _exec(primary, query)
    replica = _exec(replica, query)
    p = primary.fetchone()['updated_at']
    r = replica.fetchone()['updated_at']
    _result2(p - r)


def minmax(primary, replica, table):
    '''Check MIN(id) and MAX(id) match between primary and replica.'''
    _announce('minmax', table)
    pmin, pmax = _minmax(primary, table)
    rmin, rmax = _minmax(replica, table)
    _result2('replica: {}/{}'.format(rmin, rmax))
    _result2('primary: {}/{}'.format(pmin, pmax))



def bulk_1000_sum(primary, replica, table):
    '''Check that two databases have the same ids in blocks of 1000.
    Assuming Postgres is good at retrieving adjacent blocks, this should be a fast checksum.'''
    _announce('bulk 1000 sum', table)
    rmin, rmax = _minmax(replica, table)
    blocks = round(rmax / 1000)
    for _ in tqdm(range(1000)):
        block = random.randint(1, blocks)
        query = 'SELECT SUM(id::bigint) AS "sum" FROM {} WHERE id > %s AND id < %s'.format(table)
        gt = block * 1000
        lt = gt + 1000
        psum = _exec(primary, query, (gt, lt)).fetchone()['sum']
        rsum = _exec(replica, query, (gt, lt)).fetchone()['sum']
        _debug2('primary: {}'.format(psum))
        _debug2('replica: {}'.format(rsum))

        if psum != rsum:
            _error2('Sum failed at id = ({}, {}), psum = {}, rsum = {}'.format(lt, gt, psum, rsum))
    _result2('OK')



def main(table, rows, exclude_tables):
    print(Fore.CYAN, '\b=== Welcome to the Postgres auditor v{} ==='.format(VERSION), Fore.RESET)
    print()

    pconn, rconn = connect()

    primary = pconn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    replica = rconn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    _debug2('Primary: {}'.format(primary.connection.dsn))
    _debug2('Replica: {}'.format(replica.connection.dsn))
    if table:
        tables = [table]
        _debug2('Checking table "{}"'.format(table))
    else:
        _debug2('Checking all tables in schema "public"')
        tables = map(lambda t: t['table_name'],
            _exec(replica, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'").fetchall())


    for table in tables:
        if table in exclude_tables:
            continue
        randcheck(primary, replica, table, rows)
        print()
        last_1000(primary, replica, table)
        print()
        lag(primary, replica, table)
        print()
        minmax(primary, replica, table)
        print()
        bulk_1000_sum(primary, replica, table)
        print()


@click.command()
@click.option('--primary', required=True, help='DSN of the primary.')
@click.option('--replica', required=True, help='DSN of the replica.')
@click.option('--table', required=False, help='Scan this table only.')
@click.option('--debug/--release', default=False, help='Print debug information as we go along.')
@click.option('--rows', default=ROWS, help='Number of rows to sample in the randcheck.')
@click.option('--exclude-tables', default='', help='Exclude these tables (comma separated) from the check.')
def checksummer(primary, replica, table, debug, rows, exclude_tables):
    os.environ['REPLICA_DB_URL'] = replica
    os.environ['PRIMARY_DB_URL'] = primary

    if debug:
        os.environ['DEBUG'] = 'True'

    main(table, rows, exclude_tables.split(','))

