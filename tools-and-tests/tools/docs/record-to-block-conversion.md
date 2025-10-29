# Record Stream to Block Stream Conversion Overview
This is a one time process that will be done to convert historical Hedera record stream files into
block stream files. The end result will be a set of block stream files that will contain the complete blockchain history
of the Hedera network from genesis to the present day.

> **!Important:**
> Some of the steps and commands described here download huge amounts of data from requester pays Google Cloud Storage
> buckets. This can cost $10,000s of dollars in egress fees. Please be very careful before running any of these commands
> and make sure you understand the costs involved.

Before the switch over to block stream files, Hedera nodes produced record stream files that contained transactions and
transaction records. Each record file is considered a "Block", they were produced every 2 seconds on average. Each node
in the network produced its own copy of each record file along with a signature file for proof and later sidecar files
with extra information. In normal operation, all nodes would produce identical record files for each block, sometimes a
node would produce a bad file because of bug or some other network like test net files would be mixed in with mainnet
files. To make a complete history of a block all we need is one good record file for each block along with all nodes
signature files and one copy of each numbered sidecar file. In the command line code these blocks are represented by
[org/hiero/block/tools/records/RecordFileBlock.java](../src/main/java/org/hiero/block/tools/records/RecordFileBlock.java).

The conversion process will convert each one of these record file blocks into a block stream block. The block stream
block will contain the contents of the record file as well as all signature files and sidecar files. The format is
converted but in a lossless way so all information is preserved. The new block stream files can be cryptographically
verified. That verification process is two-step, first computing a block hash by converting the contents to the relevant
version hashing format and then computing block hash. Each of the 3 versions of record files (v2,v5 and v6) have their
own hashing format. Each now block file will have the following items:
- BlockHeader
- RecordStreamFile
- BlockFooter
- BlockProof (containing record file format version and signatures)

# The conversion process is as follows:

## 1) Gather mirror node data needed for conversion
- Download Mirror Node database record file table CSV dumps using the `mirror fetchRecordsCsv` command
- Generate the `data/block_times.bin` file using the `mirror extractBlockTimes` command
- (Optional) Validate the `block_times.bin` file using the `mirror validateBlockTimes` command **(TODO needs updating/fixing)**
- (Optional) Append newer block times using the `mirror addNewerBlockTimes` command **(TODO needs updating/fixing)**
- Generate the `data/day_blocks.json` file using the `mirror extractDayBlock` command

The `data/block_times.bin` file is used to map block numbers to record file timestamps and back. The
`data/day_blocks.json` file has data for each day containing the first and last block numbers and hashes for the day.

## 2) Collect record file day data
- Start with collecting a listing of all files in the bucket into a `files.json` file. This can be done with the command:
```
  nohup rclone lsjson -R --hash --no-mimetype --no-modtime --gcs-user-project <PROJECT> "gcp:hedera-mainnet-streams/recordstreams" > files.json &
  ```
- Convert the huge `files.json` into per-day listing files using the `days split-files-listing` command. The result of
 this command will be a `listingsByDay` directory with hierarchical per-day listing files like
 `listingsByDay/2025/09/01.bin`.

## 3) Download all the record, signature and sidecar files needed for conversion
- Use the `days download-days-v2` command to download all record files, signature files and sidecar files needed for
 conversion. This command will use the mirror node data creates in step 1 and the per-day listing files created in step
 2 to download all needed files into a local directory. It creates one about 1GB .tar.zstd file per day with all needed
 files. This process takes weeks, maybe even months to complete because of the huge amount of data (~30 TBs) and
 trillions of tiny files. because of this you can run download-days-v2 on day ranges at a time in the background, like:
```
nohup jdk-25/bin/java --enable-native-access=ALL-UNNAMED -jar tools-0.21.0-SNAPSHOT-all.jar days download-days-v2 2022 11 15 2025 1 1 &
```
Then run `tail -f nohup.out` to monitor progress.

## 4) Validate the downloaded day files
- Use the `days validate` command to validate the downloaded day files. This command will read each day file,
 recompute blockchain hashes and validate them against the expected values from mirror node. It will print any warnings
 or errors it finds. The process will write a `validateCmdStatus.json` file into the days directory so it can resume if
 interrupted or an error is detected. It is estimated a fill validation will take a few days on a fast machine.
- This validation process is produces an address book history file `addressBookHistory.json` this will be useful later
 when validating converted wrapped block files and will get bundled into block node for use by verification code.

## 5) Finally Convert downloaded day files into wrapped block stream files
> **!Important:**
Days wrap command is not finished yet and is still being worked on.

- Use the `days wrap` command to convert all downloaded day files into wrapped block stream files. This command will
 read each day file, convert each record file block into a wrapped block stream block and write out block stream files
 in standard size chunks zip files like Block Node historic plugin does. The aim is the output files can be directly
 used by Block Node historic plugin without any further processing. It is estimated this process will take a few days
 to a week or more on a fast machine.
- Next will be a verification step, the code for this is still being worked on.

