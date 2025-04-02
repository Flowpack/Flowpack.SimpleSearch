# Flowpack.SimpleSearch

[![Latest Stable Version](https://poser.pugx.org/flowpack/simplesearch/v/stable)](https://packagist.org/packages/flowpack/simplesearch) [![Total Downloads](https://poser.pugx.org/flowpack/simplesearch/downloads)](https://packagist.org/packages/flowpack/simplesearch)

A simple php search engine based on SQLite or MySQL. Performance is acceptable but
decreases quickly with the amount of entries.
Depending on the queries you want to perform a sane upper limit is somewhere around
50000 entries (for SQLite).

This package has no hard dependencies on anything so could be used in any project.

If you look at the code the sqlite storage of properties looks pretty strange but
with SQlite3 the actual storage type is determined per row, so a column can contain
different data types in each row. That should make all those empty rows more or less
acceptable. We are trying to mimic a document database here after all.

## Using MySQL


To use MySQL, switch the implementation for the interfaces in your `Objects.yaml`
and configure the DB connection as needed:

    Flowpack\SimpleSearch\Domain\Service\IndexInterface:
      className: 'Flowpack\SimpleSearch\Domain\Service\MysqlIndex'
    
    Neos\ContentRepository\Search\Search\QueryBuilderInterface:
      className: 'Flowpack\SimpleSearch\ContentRepositoryAdaptor\Search\MysqlQueryBuilder'
    
    Flowpack\SimpleSearch\Domain\Service\MysqlIndex:
      arguments:
        1:
          value: 'Neos_CR'
        2:
          value: 'mysql:host=%env:DATABASE_HOST%;dbname=%env:DATABASE_NAME%;charset=utf8mb4'
      properties:
        username:
          value: '%env:DATABASE_USERNAME%'
        password:
          value: '%env:DATABASE_PASSWORD%'

The `arguments` are the index identifier (can be chosen freely) and the DSN.
