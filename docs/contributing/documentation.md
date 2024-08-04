Building documentation requires Python 3.6+.

### Install Dependencies

Install mkdocs, themes, and extensions.

```shell
pip install -r docs/requirements_docs.txt
```

### Serving

To serve the documentation locally run

```shell
mkdocs serve
```

and visit the docs at [http://127.0.0.1:8000/wrappers/](http://127.0.0.1:8000/wrappers/)

### Deploying

If you have write access to the repo, docs can be updated using

```
mkdocs gh-deploy
```
