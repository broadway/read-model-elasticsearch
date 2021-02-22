<?php

declare(strict_types=1);

/*
 * This file is part of the broadway/read-model-elasticsearch package.
 *
 * (c) 2020 Broadway project
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Broadway\ReadModel\ElasticSearch;

use Broadway\ReadModel\Repository;
use Broadway\ReadModel\Testing\RepositoryTestReadModel;
use Broadway\Serializer\Serializer;
use Broadway\Serializer\SimpleInterfaceSerializer;
use Elasticsearch\Client;

/**
 * @group functional
 * @requires extension curl
 */
class AliasingElasticSearchRepositoryTest extends ElasticSearchRepositoryTest
{
    private $client;

    protected function createRepository(): Repository
    {
        $this->client = $this->createClient();
        $this->client->indices()->create(['index' => 'test_index']);
        $this->client->cluster()->health(['index' => 'test_index', 'wait_for_status' => 'yellow', 'timeout' => '10s']);

        return $this->createElasticSearchRepository(
            $this->client,
            new SimpleInterfaceSerializer(),
            'test_index',
            RepositoryTestReadModel::class
        );
    }

    protected function createElasticSearchRepository(
        Client $client,
        Serializer $serializer,
        string $index,
        string $class_type
    ): ElasticSearchRepository {
        return new AliasingElasticSearchRepository($client, $serializer, $index, $class_type);
    }

    /**
     * @test
     */
    public function it_creates_an_index_with_an_alias()
    {
        $type             = 'class';
        $nonAnalyzedTerm  = 'name';
        $alias            = 'test_non_analyzed_index';
        $this->repository = new AliasingElasticSearchRepository(
            $this->client,
            new SimpleInterfaceSerializer(),
            $alias,
            $type,
            array($nonAnalyzedTerm)
        );

        $suffix = uniqid('', false);
        $index  = $alias . $suffix;

        $this->repository->createIndexWithAlias($suffix);
        $this->client->cluster()->health(array('index' => $index, 'wait_for_status' => 'yellow', 'timeout' => '10s'));

        $expectedAlias = [
            $index => [
                'aliases' => [
                    $alias => []
                ]
            ]
        ];

        $this->assertEquals($expectedAlias, $this->client->indices()->getAlias(['name' => $alias]));
    }
}
