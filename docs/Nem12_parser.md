
## Requirements

**High Input load**

Decision use stream to process line by line or batch
Assume Huge CSVs and multiple CSV files.


**Input types**
	CSV (currently only csv file)


**Generate SQL insert statement**

Use PostgreSQL

Example

meter_readings

TABLE
```
CREATE TABLE STATEMENT:

create table meter_readings (

id uuid default gen_random_uuid() not null,

"nmi" varchar(10) not null,

"timestamp" timestamp not null,

"consumption" numeric not null,

constraint meter_readings_pk primary key (id),

constraint meter_readings_unique_consumption unique ("nmi", "timestamp")

);
```


**Uphold hierarchy info**

200
	300
	400
	500
focus on 200 and 300
skip 400 500
100 900 not important
## Assumptions

Table is already provided and should not be modified

Potential invalid file or corrupted file with wrong value
	Missing values
	Missing interval length

300 is within 24 hrs and the interval length is a division of this 24 hrs (1440 min)
200 indicates interval length

regardless of quality we take the values
missing values we record to a table

("nmi", "timestamp") is the unique identifier

do not skip but show warning if it don't have 100 row to indicate valid nem12 file


-------

## Questions to be answered

**Q1. What is the rationale for the technologies you have decided to use?**

**Q2. What would you have done differently if you had more time?**

**Q3. What is the rationale for the design choices that you have made?**



## Design Decisions


### Language: Python
I know how to write -> no need learning curve
Easy to write -> fast development speed, most people know
Faster load time -> faster test, debug iteration
Code should not be the bottleneck but I/O should be the upsert to database (Database write bottleneck) -> No need for faster efficiency

### Library used

**csv**
process csv
	Fast streaming for csv line by line
	lazy loading lines -> able to handle large csv files

**psycopg3**
for fast process of sql database
	Used Offline mode
	Security from sql injection attacks as sql generated will be safely formatted

### Safe guards corner cases

**Invalid files**

Check 1st row to see if it is nem12 file
Check header `100`
	Don't stop process even if it does not have the header as we can check if the other rows are valid
End of file `900`
	 Don't think need validate this but a valid one should have this


**Failure during processing**

Add checkpointers to be able to continue where it stopped, stucked or failed.
	This shortens subsequent processes in case of failure so there is no need to repeat if checkpointed.
	Possibly extension with versioning or pausing features

**Erroneous rows**

Store the rows to a seperate files to indicate the type of error
Store runnable csv file on erroneous row to fix and reran if needed

### Design pattern choices

#### Adaptor design pattern for csv stream input

	Able to expend input stream types if needed

#### Separate thread for the writer and reader

	Reader don't need to wait for writer to finish writing
	Can write to disk without waiting for CPU to run the reader

#### Chunking of SQL files

	Easier for humans to read and manage
	One huge file is slow to load and read and takes up RAM

#### Batch Insert statements
	Insert should be batch insert to speed up process for huge input data
	Easier to read
	Replaces with new updates to nmi and timestamp unique key pair

**Examples**
```
INSERT INTO meter_readings ("nmi", "timestamp", "consumption")
VALUES
    ('NEMQ123456', '2026-05-24 00:00:00', 1.25),
    ('NEMQ123456', '2026-05-24 00:15:00', 1.42),
    ('NEMQ123456', '2026-05-24 00:30:00', 0.98),
    ('NEMQ987654', '2026-05-24 00:00:00', 2.10),
    ('NEMQ987654', '2026-05-24 00:15:00', 1.85)
ON CONFLICT ("nmi", "timestamp")
DO UPDATE SET
    consumption = EXCLUDED.consumption;

```

#### Minimum batch size is a row of data

This is mainly an accidental feature but I think it is fine to leave it as i dont want the batch size to be too small.
Hence, when set to a value smaller than the row's data size it will use the upper limit of these.

I also think it is cleaner to have at least a full 300 row to be within the same chunk file.
There is also an upper limit to the data size of a single row so i think it is ok in terms of scalability.


## Potential Additions
More features if more clarity is provided

**Make it into an application where user can start or make other controls to it**

**Add a direct connection to db instead of just writing sql files**
	enable direct insert to db


## Done differently

**Do more requirements gathering for more clarity.**
	How high a load expected csv files or can we expect other types of inputs
	Location of files. single or multiple locations

**Do some testing on the checkpointer feature**

**Get more input variety to test**