broadway/read-model-elasticsearch
=================================

Elasticsearch read model implementation for [broadway/broadway](https://github.com/broadway/broadway) 
using [elastic/elasticsearch-php](https://github.com/elastic/elasticsearch-php)

![build status](https://github.com/broadway/read-model-elasticsearch/actions/workflows/ci.yml/badge.svg)

## Installation

```
$ composer require broadway/read-model-elasticsearch
```

## Version Matrix

| Elasticsearch Version | broadway/read-model-elasticsearch version |
| --------------------- | ----------------------------------------- |
| 7                     | 0.7                                       |
| 6                     | 0.6                                       |
| 5                     | 0.5                                       |
| 2                     | 0.1                                       |
| 1                     | 0.1                                       |

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
