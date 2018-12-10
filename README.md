# Livewire: Power delivery modeling. In your database.

## What is LiveWire?

LiveWire is a postgresql extension that makes managing electrical distribution data painless. It provides functions that make answering the common questions that distribution engineers have easy. Livewire works with your existing data and does not force you to change your data to fit a given schema.

## Requirements and Dependencies

- Postgresql 10
- postgis 2.4
- pgrouting 2.6

## Installation

Clone this repository.
Run:

``` shell
make && sudo make install
```
You may also want to run:

``` shell
make installcheck
```
to run the test suite.


In the database that you want to enable LiveWire in run as a db superuser:

``` SQL
CREATE EXTENSION postgis;
CREATE EXTENSION pgrouting;
CREATE EXTENSION livewire;
```
## Usage

LiveWire groups common data together in a schema. To create a new livewire, use the `lw_initialise` function. The following examples will use the [Navassa dataset] that can be found in the test folder in the repository

The `lw_initialise` functions takes the following arguments:
- lw_name (text) - The name of the schema to be livewired. It will be created if it doesnt exist.
- srid (int) - The ***Spatial Reference ID*** of the dataset.
- tolerance (float) - The tolerance is the maximum distance at which two geometries, even though they do not physically intersect, will be said to do so. This param is optional and the default is 0.

`lw_initialise` makes a schema ready for LiveWire by adding four support tables:
- __lines
- __nodes
- __livewire
- schemaname

For clarity, schemaname above is the name of the schema, so if you initialise a livewire in a schema named `powerflow`, there will be a table called `powerflow.powerflow` Clearly these table names are reserved in the context of any livewire enabled schema.

So lets get the examples going with the [Navassa dataset]

```
SELECT lw_initialise('navassa',3450);
```

The next step is to add the tables that will be participating in the livewire to the config table. This is done with the `lw_addedgeparticpant` function for tables where the geometry type is linestring and `lw_addnodeparticpant` function for point tables. Both functions take two arguments.

The `lw_addedgeparticipant` & `lw_addnodeparticipant` functions take the following arguments:
- lw_name (text) - The name of the livewired schema.
- lw_config (json) - a JSON object containing configuration directives.

The configuration directives are keys in the JSON object.

|Key        | Usage |
------------|----------------------------------------------
schemaname  | The name of the schema where this table lives.
tablename   | The table in question.
primarykey  | Any column that is unique, doesn't have to be a constrained column, but that would help.
geomcolumn  | The column that holds the geometry.
feederid    | Column name that stores the name of the source.
phasecolumn | Column that has phasing data.
phasemap    | an object of with the phase mapping.
sourcequery | a text string containing a where clause that will filter for the source data.
blockquery  | a text string containing a where clause that will mark open points.

Both the blockquery and sourcequery keys are applicable only when used with `lw_addnodeparticipant`.


```SQL
SELECT lw_addnodeparticipant('navassa', $${
  "schemaname":"navassa",						 
  "tablename": "isolating_devices",
  "primarykey":"device_id",
  "geomcolumn": "g",
  "feederid":"feedername",
  "sourcequery": "1=2",
  "blockquery": "status='OPEN'",
  "phasecolumn": "phases",
  "phasemap":{"ABC":"ABC","AB":"AB","AC":"AC","BC":"BC","A":"A","B":"B","C":"C"}
}$$);
```
Once all the tables have been configured, the next step is to generate the shadow network. `lw_generate` is your friend.

The `lw_generate` functions takes one argument:
- lw_name (text) - The name of the livewired schema.

```SQL
SELECT lw_generate('navassa');
```

`lw_generate` may take a while to run depending on the size of the dataset.

After the shadow network is `lw_generate`d, we populate the routing cache with `lw_traceall`.

The `lw_traceall` functions takes one argument:
- lw_name (text) - The name of the livewired schema.

```SQL
SELECT lw_traceall('navassa_shadow');
```

[Navassa dataset]: <https://raw.githubusercontent.com/rhysallister/livewire/master/tests/navassa_data.sql>

