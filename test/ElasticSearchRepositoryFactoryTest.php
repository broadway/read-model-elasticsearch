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

use Broadway\Serializer\Serializer;
use Elasticsearch\Client;
use PHPUnit\Framework\TestCase;

class ElasticSearchRepositoryFactoryTest extends TestCase
{
    /**
     * @test
     */
    public function itCreatesAnElasticSearchRepository(): void
    {
        $serializer = $this->getMockBuilder(Serializer::class)
            ->getMock();
        $client = $this->getMockBuilder(Client::class)
            ->disableOriginalConstructor()
            ->getMock();

        $repository = new ElasticSearchRepository($client, $serializer, 'test', 'Class');
        $factory = new ElasticSearchRepositoryFactory($client, $serializer);

        self::assertEquals($repository, $factory->create('test', 'Class'));
    }

    /**
     * @test
     */
    public function itCreatesAnElasticSearchRepositoryContainingIndexMetadata(): void
    {
        $serializer = $this->getMockBuilder(Serializer::class)
            ->getMock();
        $client = $this->getMockBuilder(Client::class)
            ->disableOriginalConstructor()
            ->getMock();

        $repository = new ElasticSearchRepository($client, $serializer, 'test', 'Class', ['id']);
        $factory = new ElasticSearchRepositoryFactory($client, $serializer);

        self::assertEquals($repository, $factory->create('test', 'Class', ['id']));
    }
}
