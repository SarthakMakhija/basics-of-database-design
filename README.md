# basics-of-database-design
Learn the basics of database design, to begin with a Key/Value store

# Understand various aspects of database design including - 
+ Disk access patterns
+ Working of HDDs and SSDs
+ Binary format to represent data
+ Basics of block and page
+ Disk data structures
  + B+Trees
      + Relevance of page
  + LSM Trees
      + Relevance of page
      + Understand probabilistic data structure (Bloom filter)
      + Design a simple bloom filter
  + Concurrency with these data structures
  + Understanding representation of each of these structures on disk
+ Write and read amplification
+ Transactions
+ Write ahead log

# 15th August 2021
+ Current implementation contains a key/value store which represents keys and values as byte arrays
+ Current implementation maintains an in-memory map between key, and the offset of the value present in an append only log
+ Current implementation uses a single append only key/value log file
+ Content of key/value log gets encoded in binary format as key-size,actual key, value-size and actual value
+ Current implementation uses "memory-mapped" file for append only key/value log
+ Current implementation accesses the key/value log file randomly while getting a value, which could result in a page fault if the corresponding page is not already memory mapped
+ Current implementation has NO concept of page, transaction, concurrency, endianness, block or sector alignment, fsync, fadvice
+ Current implementation (re)loads the entire append only key/value log, if the key/value store gets restarted
+ Size of memory mapped file is hardcoded 4096 bytes
