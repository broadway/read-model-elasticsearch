<?php

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

class ElasticSearchClientFactory
{
    /**
     * @param array $config
     *
     * @return Client
     */
    public function create(array $config)
    {
        if (class_exists('\Elasticsearch\ClientBuilder')) {
            return \Elasticsearch\ClientBuilder::fromConfig($config);
        }

        return new Client($config);
    }
}
