# de-assignment
https://gist.github.com/dixitm20/2feb1d8df0ab6e0ac2cb3f08f7de5962

## Pre-requisites

We use [`batect`](https://batect.dev/) to dockerise the tasks in this exercise. 
`batect` is a lightweight wrapper around Docker that helps to ensure tasks run consistently (across linux, mac windows).
With `batect`, the only dependencies that need to be installed:
* Docker
* Java >= (1.8)

## Run tests

### Run tests
```bash
./batect test
```

## Run style checks
```bash
./batect lint
```

## Job

For each entity_id in the signals dataset, find the item_id with the oldest and newest month_id.
In some cases it may be the same item.
If there are 2 different items with the same month_id then take the item with the lower item_id.
Finally, sum the count of signals for each entity and output as the total_signals.
The correct output should contain 1 row per unique entity_id.

#### Input
Directory with `*.parquet` files with following schema:
```
entity_id: long
item_id: integer
source: integer
month_id: integer
signal_count: integer
```

#### Output
A single `*.parquet` file with following schema:
```
entity_id: long
oldest_item_id: integer
newest_item_id: integer
total_signals: integer
```

#### Run the job

```bash
./batect run 
```