'''Check that two tables on two databases have reasonably identical rows.
Assumes that both tables have "id" and "updated_at" columns and indexes on those columns.'''
import psycopg2
import os
from tqdm import tqdm
from colorama import Fore
import colorama
import click

colorama.init()

ROWS = 8128
VERSION = '0.0.3'

__version__ = VERSION
__author__ = '<Lev Kokotov> lev.kokotov@instacart.com'


def _func(i):
    '''Gives 8128 rows from ~0 to ~2.4 billion.'''
    return i ** 2.4


def _debug(cursor, executor):
    '''Print the executed query in a pretty color.'''
    if os.getenv('DEBUG'):
        print(Fore.BLUE, '\b{}: '.format(executor) + cursor.query.decode('utf-8'), Fore.RESET)


def connect():
    '''Connect to source and replicaination DBs.'''
    primary = psycopg2.connect(os.getenv('PRIMARY_DB_URL'))
    replica = psycopg2.connect(os.getenv('REPLICA_DB_URL'))

    return primary, replica


def check(id_, primary, replica, table):
    '''Check two rows on primary and replicaination tables.'''
    query = 'SELECT * FROM "{table}" WHERE "id" = {id_} LIMIT 1'.format(table=table, id_=id_)

    primary.execute(query)
    replica.execute(query)

    _debug(primary, 'primary')
    _debug(replica, 'replica')

    r1 = primary.fetchone()
    r2 = replica.fetchone()

    # No rows
    if r1 is None and r2 is None:
        return None

    if r1 != r2:
        return False
    else:
        return True


def last_1000(primary, replica, table):
    '''Check last 1000 rows available on the replica table.'''
    query1 = 'SELECT "id" FROM "{table}" ORDER BY "id" DESC LIMIT 1000'.format(table=table)

    replica.execute(query1)
    _debug(replica, 'replica')

    ids_available_on_replica = map(lambda row: str(row[0]), replica.fetchall())

    query = 'SELECT * FROM "{table}" WHERE id IN ({ids}) ORDER BY "id" DESC LIMIT 1000'.format(
        table=table, ids=','.join(ids_available_on_replica))

    primary.execute(query)
    _debug(primary, 'primary')
    replica.execute(query)
    _debug(replica, 'replica')

    r1 = primary.fetchall()
    r2 = replica.fetchall()

    for idx, row1 in enumerate(r1):
        row2 = r2[idx]

        if row1 != row2:
            raise Exception('Last 1000: Rows at id = {id_} are different.'.format(id_=row1[0]))


def lag(primary, replica, table):
    '''Check logical lag between primary and replica table using Django/Rails "updated_at".'''
    query = 'SELECT MAX(updated_at) FROM "{table}"'.format(table=table)

    primary.execute(query)
    _debug(primary, 'primary')
    replica.execute(query)
    _debug(replica, 'replica')

    r1 = primary.fetchone()[0]
    r2 = replica.fetchone()[0]

    return r1 - r2


def main(table):
    print(Fore.CYAN, '\b=== Welcome to the Postgres auditor v{} ==='.format(VERSION))
    print()

    primary, replica = connect()

    c1 = primary.cursor()
    c2 = primary.cursor()

    print(Fore.BLUE, '\bprimary: {}'.format(primary.dsn), Fore.RESET)
    print(Fore.BLUE, '\breplica: {}'.format(replica.dsn), Fore.RESET)
    print()
    print(Fore.BLUE, '\bChecking table "{}"'.format(table), Fore.RESET)
    print()

    print(Fore.YELLOW, '\bChecking intermediate rows using f(i) = i^(2.4), i = [0, {rows}]'.format(rows=ROWS), Fore.RESET)

    with tqdm(total=ROWS) as pbar:
        for i in range(ROWS):
            id_ = round(_func(i))
            result = check(id_, c1, c2, table)
            if result is None:
                pbar.update(1)
                continue
            if result is False:
                raise Exception('Rows at id = {} are different.'.format(id_))
            pbar.update(1)

    print(Fore.GREEN, '\bOK.', Fore.RESET)
    print()

    print(Fore.YELLOW, '\bChecking last 1000 rows', Fore.RESET)

    last_1000(c1, c2, table)

    print(Fore.GREEN, '\bOK.', Fore.RESET)
    print()

    print('Current lag:', Fore.MAGENTA, '\b{lag}'.format(lag=lag(c1, c2, table)), Fore.RESET)

    print()
    print('Bye.')
    print()


@click.command()
@click.option('--primary', required=True)
@click.option('--replica', required=True)
@click.option('--table', required=True)
@click.option('--debug/--release', default=False)
def checksummer(primary, replica, table, debug):
    os.environ['REPLICA_DB_URL'] = replica
    os.environ['PRIMARY_DB_URL'] = primary

    if debug:
        os.environ['DEBUG'] = 'True'

    main(table)
