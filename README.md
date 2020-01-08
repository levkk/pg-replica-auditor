# pg-replica-checksummer

## Installation
Using virtualenv, `pip install -r requirements.txt`

## Usage

```bash
$ (venv) python checksummer.py --soruce=postgres://primary-db.amazonaws.com:5432/my_db --destination=postgres://replica-db.amazonaws.com:5432/my_db --table=immutable_items
```