# NotSoEfficientDB
An implementation of a LSMT (Log Structured Merge Tree) based key value store.
Follows [DDIA][1]'s storage chapter.

### Run on your local
Update the paths where you would like to save the database files
```bash
open src/main/java/com/satvik/satvikdb/utils/SimpleDbConstants.java
open src/main/java/com/satvik/satvikdb/utils/LsmDbConstants.java
```
and update the `ROOT_DIR` variable to match your desired path.

Run `src/test/java/com/satvik/satvikdb/SatvikDbTest.java` and see the files created on your `ROOT_DIR`


# How it works

## LSM Database

### Writes: `key, value`
1. Put the `key, value` pair in the WAL file (Write Ahead Log)
2. Put the tuple in the in-memory `SortedMap`. This data structure is also called `memtable`. This is an LSM based DB, so keys will be in sorted order. 
3. If the size of the map exceeds a certain threshold, dump the values in filesystem as the latest `SSTable` file. Make sure the corresponding indexes are also created (sparse - 1 entry per 100ish records). Clear the in memory map, making it ready for upcoming writes.
4. The indexes store `ByteOffsets` where the value was written, so that we can directly seek at that location.
5. A `ByteOffset` is the location of a byte in the file, 0-indexed 
6. Now as the map already had the values in sorted order, the values in files are also sorted.
7. Run `Compaction` every once in a while, or when the number of files cross a certain threshold. Refer [compaction](#compaction)

The chain of these `SSTables` files is also called Log Structured Merge Tree (LSTM) 

#### Purpose of WAL 
In case the `LsmWriteService` becomes overloaded due to some reason and crashes, 
the outstanding writes will still remain in the WAL. Hence,
each time the `LsmWriteService` is started, it first checks if the WAL is empty.
If not, it first writes the values pending in WAL and then clears it.


### Reads: `key` in `O(logn)`
n are the number of keys present in the database
1. Lookup the in memory `SortedMap`. 
2. If found, return the value.
3. If not, load the latest index file from the file system in memory.
4. Apply a binary search on the index's keys. Find the `left` pointer, just before `key`.
5. If you reach the start with no `left` value, load the previous index file.
6. As soon as you find a hit, load the `ByteOffset` of the `value` 
7. open the corresponding database file and seek from the `ByteOffset` returned by the index file.
8. Start a linear search from here and return the first match with `key`
9. If there's no match for `key`, return null

### Compaction

As our database grows over time, we might have thousands of `SSTable` files created, many of which might have overwritten or old data.

Run the merging process in some defined time interval.
1. loads all the database files
2. Does a `merge K sorted lists` kind of algorithm in a batch of `n` files.
3. Each `RandomAccessFile` is considered as a 'sorted array'.
4. If there are 2 keys with the same name, take the one which is in a latter file. We essentially only want to keep the latest value for a given `key`
5. After the new merged file is created, delete the old files.

Compaction is idempotent, i.e you can run it x number of times, but the result will not affect database reads or writes.

This flow is similar to what [LevelDB](https://github.com/google/leveldb) and [RocksBD](https://github.com/facebook/rocksdb) do.

### How did bloom filters impact the performance?

For `60000` reads and writes, `28` Memtables on disk,
`~3000` entries per file and `~20` entries per index

Without bloom filters:

write: `2155 ms`
read : `309ms`

With bloom filters:

write: `2681ms`
read : `83ms`

~3x read improvement with a little hit on write performance.

## Other implementations - Simple db
This project also includes an implementation of a simple append only database which stores the keys in the order in which they were written. Includes no optimisation.
To see how that works, change the code in main/test file to this
```java
DbService dbService = DbFactory.getDbService(TypesEnum.SIMPLE);
```
## Simple database
### Writes: `key, value`
1. Directly writes into the database file in append mode.
2. corresponding indexes are also created (dense - 1 entry for each key)
3. The index contains the key's `ByteOffset` of `value` in the file.


### Reads: `key` in `O(n)`
1. load the latest index file.
2. Does a linear scan until in encounters the `key`
3. Notes the `ByteOffset` of the corresponding `value`
4. Opens the file, directly seeking to the `ByteOffset` and returns the `value`

## Todo
- [ ] Implement delete feature
- [x] Add bloom filter to filter out missing keys. Currently, if the lookup key is not present in the database, we scan all the `SSTable` indexes. This is not very efficient.
- [ ] Optimise compaction to be `size tiered` / `level tiered`

[1]: https://dataintensive.net/