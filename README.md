## Getting Started

The application environment is set up using [pipenv](https://docs.pipenv.org/). Make sure the pipenv shell is started. 

```bash
cd scometrics
pipenv shell 
```  

To install required packages

```bash
pipenv install
```

#### Environment Variables

Environment variables are set in `.env` file. It's not tracked in git, so it has to be created manually. 

The .env file will be automatically loaded with pipenv shell is started, or python is run with pipenv. Therefore, make sure to run commands in the 
pipenv shell when developing/testing, or use `pipenv run python` when running python from command line. 

## Testing

[py.test](https://docs.pytest.org/en/latest/) is used to run tests: 

```bash
pytest tests 
```

To run an individual test:

```bash
pytest tests/test_main.py
```
