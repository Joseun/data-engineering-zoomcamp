## Week 1 Homework

In this homework we'll prepare the environment 
and practice with Docker and SQL


## Question 1. Knowing docker tags

Run the command to get information on Docker 

```docker --help```

Now run the command to get help on the "docker build" command

Which tag has the following text? - *Write the image ID to the file* 

- `--imageid string`
- `--iidfile string`
- `--idimage string`
- `--idfile string`

## Question 2. Understanding docker first run 

Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash.
Now check the python modules that are installed ( use pip list). 
How many python packages/modules are installed?
docker run -it --entrypoint=bash python:3.9


- 1
- 6
- 3
- 7


# Prepare Postgres

Run Postgres and load data as shown in the videos
We'll use the green taxi trips from January 2019:

```wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz```

You will also need the dataset with zones:

```wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv```

Download this data and put it into Postgres (with jupyter notebooks or with a pipeline)


## Question 3. Count records 

How many taxi trips were totally made on January 15?

Tip: started and finished on 2019-01-15. 

Remember that `lpep_pickup_datetime` and `lpep_dropoff_datetime` columns are in the format timestamp (date and hour+min+sec) and not in date.

- 20689
- 20530
- 17630
- 21090

## Question 4. Largest trip for each day

Which was the day with the largest trip distance
Use the pick up time for your calculations.

- 2019-01-18
- 2019-01-28
- 2019-01-15
- 2019-01-10

## Question 5. The number of passengers

In 2019-01-01 how many trips had 2 and 3 passengers?
 
- 2: 1282 ; 3: 266
- 2: 1532 ; 3: 126
- 2: 1282 ; 3: 254
- 2: 1282 ; 3: 274


## Question 6. Largest tip

For the passengers picked up in the Astoria Zone which was the drop off zone that had the largest tip?
We want the name of the zone, not the id.

Note: it's not a typo, it's `tip` , not `trip`

- Central Park
- Jamaica
- South Ozone Park
- Long Island City/Queens Plaza


## Submitting the solutions

* Form for submitting: [form](https://forms.gle/EjphSkR1b3nsdojv7)
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 26 January (Thursday), 22:00 CET


## Solution

We will publish the solution here

## Answer 1.
docker image build --help

Build an image from a Dockerfile

Options:
      --add-host list           Add a custom host-to-IP mapping (host:ip)
      --build-arg list          Set build-time variables
      --cache-from strings      Images to consider as cache sources
      --cgroup-parent string    Optional parent cgroup for the container
      --compress                Compress the build context using gzip
      --cpu-period int          Limit the CPU CFS (Completely Fair Scheduler) period
      --cpu-quota int           Limit the CPU CFS (Completely Fair Scheduler) quota
  -c, --cpu-shares int          CPU shares (relative weight)
      --cpuset-cpus string      CPUs in which to allow execution (0-3, 0,1)
      --cpuset-mems string      MEMs in which to allow execution (0-3, 0,1)
      --disable-content-trust   Skip image verification (default true)
  -f, --file string             Name of the Dockerfile (Default is 'PATH/Dockerfile')
      --force-rm                Always remove intermediate containers
      --iidfile string          Write the image ID to the file
      --isolation string        Container isolation technology
      --label list              Set metadata for an image
  -m, --memory bytes            Memory limit
      --memory-swap bytes       Swap limit equal to memory plus swap: '-1' to enable
                                unlimited swap
      --network string          Set the networking mode for the RUN instructions
                                during build (default "default")
      --no-cache                Do not use cache when building the image
      --pull                    Always attempt to pull a newer version of the image
  -q, --quiet                   Suppress the build output and print image ID on success
      --rm                      Remove intermediate containers after a successful
                                build (default true)
      --security-opt strings    Security options
      --shm-size bytes          Size of /dev/shm
  -t, --tag list                Name and optionally a tag in the 'name:tag' format
      --target string           Set the target build stage to build.
      --ulimit ulimit           Ulimit options (default [])

## Answer 2
docker run -it --entrypoint=bash python:3.9
pip list

## Answer 3
SELECT
	CAST(lpep_pickup_datetime AS DATE) AS day_pick,
	COUNT(1)
FROM
	green_taxi_data g
WHERE
	CAST(lpep_pickup_datetime AS DATE) = '2019-01-15'
GROUP BY
	day_pick
ORDER BY
	day_pick ASC;

## Answer 4
SELECT
	CAST(lpep_pickup_datetime AS DATE) AS day_pick,
	MAX(trip_distance)
FROM
	green_taxi_data g
GROUP BY
	day_pick
ORDER BY
	max DESC;

## Answer 5
SELECT
	CAST(lpep_pickup_datetime AS DATE) AS day_pick,
	COUNT(trip_distance)
FROM
	green_taxi_data g
WHERE
	CAST(lpep_pickup_datetime AS DATE) = '2019-01-01'
    AND
	passenger_count = 2
GROUP BY
	day_pick

SELECT
	CAST(lpep_pickup_datetime AS DATE) AS day_pick,
	COUNT(trip_distance)
FROM
	green_taxi_data g
WHERE
	CAST(lpep_pickup_datetime AS DATE) = '2019-01-01'
    AND
	passenger_count = 3
GROUP BY
	day_pick

## Answer 6
SELECT
	tip_amount,
	zpu."Zone" AS pick_up,
	zdo."Zone" AS drop_off
FROM
	green_taxi_data g
	JOIN 
		zones zpu
			ON g."PULocationID" = zpu."LocationID"
	JOIN zones zdo
			ON g."DOLocationID" = zdo."LocationID"
WHERE
	zpu."Zone" LIKE 'Astoria'
ORDER BY
    tip_amount DESC