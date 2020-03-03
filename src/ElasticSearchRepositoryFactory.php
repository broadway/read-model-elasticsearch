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

use Broadway\ReadModel\Repository;
use Broadway\ReadModel\RepositoryFactory;
use Broadway\Serializer\Serializer;
use Elasticsearch\Client;

/**
 * Creates Elasticsearch repositories.
 */
class ElasticSearchRepositoryFactory implements RepositoryFactory
{
    private $client;
    private $serializer;

    public function __construct(Client $client, Serializer $serializer)
    {
        $this->client = $client;
        $this->serializer = $serializer;
    }

    /**
     * {@inheritdoc}
     */
    public function create(string $name, string $class, array $notAnalyzedFields = []): Repository
    {
        return new ElasticSearchRepository($this->client, $this->serializer, $name, $class, $notAnalyzedFields);
    }
}
