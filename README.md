# StylePage tools: Python MongoDB

## Dependencies

This tool requires pymongo and access to a MongoDB server

## Installation

```bash
pip install spmongo
```

or

```bash
pip install -e "git+http://github.com/stylepage/spmongo.git#egg=spmongo"
```

or

```bash
git clone git@github.com:stylepage/spmongo.git spmongo
pip install -e spmongo
```

## Examples

```python
import spmongo
server = spmongo.mongo(host='127.0.0.1')
server['my_db']['my_collection'].insert({'foo':'bar'})
```

```python
import spmongo
```
