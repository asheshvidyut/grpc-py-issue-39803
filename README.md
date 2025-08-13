### Setup

```

cd .
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Run Server

```
export PYTHONASYNCIODEBUG=1
source .venv/bin/activate
python server.py
```

### Run Client

```
export PYTHONASYNCIODEBUG=1
source .venv/bin/activate
python client.py
```
