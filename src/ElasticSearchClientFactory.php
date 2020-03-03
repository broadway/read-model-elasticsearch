<?php

declare(strict_types=1);

/*
 * This file is part of the broadway/read-model-elasticsearch package.
 *
 * (c) Qandidate.com <opensource@qandidate.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Broadway\ReadModel\ElasticSearch;

use Elasticsearch\Client;
use Elasticsearch\ClientBuilder;

class ElasticSearchClientFactory
{
    public function create(array $config): Client
    {
        return ClientBuilder::fromConfig($config);
    }
}
