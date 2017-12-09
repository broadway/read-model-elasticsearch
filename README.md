broadway/read-model-elasticsearch
=================================

Elasticsearch read model implementation for [broadway/broadway](https://github.com/broadway/broadway) 
using [elastic/elasticsearch-php](https://github.com/elastic/elasticsearch-php)

[![Build Status](https://travis-ci.org/broadway/read-model-elasticsearch.svg?branch=master)](https://travis-ci.org/broadway/read-model-elasticsearch)

## Installation

```
$ composer require broadway/read-model-elasticsearch
```

## Version Matrix

| Elasticsearch Version | broadway/read-model-elasticsearch version |
| --------------------- | ----------------------------------------- |
| >= 5.0                | ~0.2                                      |
| >= 1.0, < 5.0         | ^0.1                                      |

 - If you are using Elasticsearch 5.0+ , use broadway/read-model-elasticsearch 0.2.
 - If you are using Elasticsearch 1.x or 2.x, use broadway/read-model-elasticsearch 0.1.

## Testing
For testing you obviously need a running Elasticsearch instance, therefore
these tests are marked with `@group functional`.

To start a local Elasticsearch you can use the provided [docker-compose.yml](https://docs.docker.com/compose/compose-file/):

```
docker-compose up -d
```

To run all the tests:

```
./vendor/bin/phpunit --exclude-group=none
```

## License

MIT, see LICENSE.
