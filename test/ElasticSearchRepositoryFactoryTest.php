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

use Broadway\Serializer\Serializer;
use Elasticsearch\Client;
use PHPUnit_Framework_TestCase;

class ElasticSearchRepositoryFactoryTest extends PHPUnit_Framework_TestCase
{
    /**
     * @test
     */
    public function it_creates_an_elastic_search_repository()
    {
        $serializer = $this->getMockBuilder(Serializer::class)
            ->getMock();
        $client = $this->getMockBuilder(Client::class)
            ->disableOriginalConstructor()
            ->getMock();

        $repository = new ElasticSearchRepository($client, $serializer, 'test', 'Class');
        $factory    = new ElasticSearchRepositoryFactory($client, $serializer);

        $this->assertEquals($repository, $factory->create('test', 'Class'));
    }

    /**
     * @test
     */
    public function it_creates_an_elastic_search_repository_containing_index_metadata()
    {
        $serializer = $this->getMockBuilder(Serializer::class)
            ->getMock();
        $client = $this->getMockBuilder(Client::class)
            ->disableOriginalConstructor()
            ->getMock();

        $repository = new ElasticSearchRepository($client, $serializer, 'test', 'Class', ['id']);
        $factory    = new ElasticSearchRepositoryFactory($client, $serializer);

        $this->assertEquals($repository, $factory->create('test', 'Class', ['id']));
    }
}
