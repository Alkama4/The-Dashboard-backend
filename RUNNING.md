# Running

## Initial setup

### Virtual enviroment

Create a virtual environment:

```bash
python -m venv venv
```

Activate it:

```bash
venv\Scripts\activate
```

### Install Dependencies

```bash
pip install -r requirements.txt
```

### Enviroment variables

1. Create a copy of the `.env.example` called `.env.local` in the `/app` folder.
2. Edit the `/app/.env.local` and set your credentials.

### Launch the application

```bash
uvicorn app.main:app --reload
```

## Normal usage

After the initial setup, you can just activate the virtual environment and start the app:

```bash
venv\Scripts\activate
uvicorn app.main:app --reload
```
