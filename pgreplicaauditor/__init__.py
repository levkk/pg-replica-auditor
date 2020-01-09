from .checksummer import VERSION
from .checksummer import main, checksummer

def cli():
    checksummer(prog_name='pgreplicaauditor')
