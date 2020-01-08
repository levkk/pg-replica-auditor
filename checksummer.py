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

def _func(i):
    '''Gives 8128 rows from ~0 to ~2.4 billion.'''
    return i ** 2.4


def _debug(cursor, executor):
    '''Print the executed query in a pretty color.'''
    if os.getenv('DEBUG'):
        print(Fore.BLUE, '{}: '.format(executor) + cursor.query.decode('utf-8'), Fore.RESET)


def connect():
    '''Connect to source and destination DBs.'''
    src = psycopg2.connect(os.getenv('SRC_DB_URL'))
    dest = psycopg2.connect(os.getenv('DEST_DB_URL'))

    return src, dest


def check(id_, src, dest, table):
    '''Check two rows on src and destination tables.'''
    query = 'SELECT * FROM "{table}" WHERE "id" = {id_} LIMIT 1'.format(table=table, id_=id_)

    src.execute(query)
    dest.execute(query)

    _debug(src, 'src')
    _debug(dest, 'dest')

    r1 = src.fetchone()
    r2 = dest.fetchone()

    # No rows
    if r1 is None and r2 is None:
        return None

    if r1 != r2:
        return False
    else:
        return True


def last_1000(src, dest, table):
    '''Check last 1000 rows available on destination table.'''
    query1 = 'SELECT "id" FROM "{table}" ORDER BY "id" DESC LIMIT 1000'.format(table=table)

    dest.execute(query1)
    _debug(dest, 'dest')

    ids_available_on_dest = map(lambda row: str(row[0]), dest.fetchall())

    query = 'SELECT * FROM "{table}" WHERE id IN ({ids}) ORDER BY "id" DESC LIMIT 1000'.format(
        table=table, ids=','.join(ids_available_on_dest))

    src.execute(query)
    _debug(src, 'src')
    dest.execute(query)
    _debug(dest, 'dest')

    r1 = src.fetchall()
    r2 = dest.fetchall()

    for idx, row1 in enumerate(r1):
        row2 = r2[idx]

        if row1 != row2:
            raise Exception('Last 1000: Rows at id = {id_} are different.'.format(id_=row1[0]))


def lag(src, dest, table):
    '''Check logical lag between src and destination table using Django/Rails "updated_at".'''
    query = 'SELECT MAX(updated_at) FROM "{table}"'.format(table=table)

    src.execute(query)
    _debug(src, 'src')
    dest.execute(query)
    _debug(dest, 'dest')

    r1 = src.fetchone()[0]
    r2 = dest.fetchone()[0]

    return r1 - r2


def main(table):
    print(Fore.CYAN, '\b=== Welcome to the Postgres checksummer. ===')
    print()

    src, dest = connect()

    c1 = src.cursor()
    c2 = src.cursor()

    print(Fore.BLUE, '\bsrc: {}'.format(src.dsn), Fore.RESET)
    print(Fore.BLUE, '\bdest: {}'.format(dest.dsn), Fore.RESET)
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
@click.option('--source', required=True)
@click.option('--destination', required=True)
@click.option('--table', required=True)
@click.option('--debug/--release', default=False)
def checksummer(source, destination, table, debug):
    os.environ['DEST_DB_URL'] = destination
    os.environ['SRC_DB_URL'] = source

    if debug:
        os.environ['DEBUG'] = 'True'

    main(table)


if __name__ == '__main__':
    checksummer()
