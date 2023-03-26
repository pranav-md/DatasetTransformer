
# Spark Assessment

Apache Spark application to read CSV and TSV files contained in a AWS S3 bucket and save the the dataset as TSV file after the transformation required.


## Logic of transformation:

Consider input files: file1.csv & file2.tsv

- **For any given key across the entire dataset (so all the files), there exists exactly one value that occurs an odd number of times. E.g. you will see data like this:**

file1.csv:

| randomcol1   | randomcol2   |
| -------------| -------------|
| 1            | 2          |
| 1            | 2          |
| 1            | 2          |
| 2            | 3          |
| 2            | 3          |
| 2            | 2          |
| 4            | 1          |
| 4            | 1          |
| 4            | 1          |
| 5            | 2          |
| 6            | 3          |
| 6            | 3          |

file1.tsv:

| col1   | col2   |
| -------------| -------------|
| 6            | 3          |
| 7            | 1          |


Such that output file supposed to have

- **The first column contains each key exactly once**

- **The second column contains the integer occurring an odd number of times for the key, from the input files**


output.tsv:

| key   | value   |
| ------| --------|
| 1     | 3       |
| 2     | 3       |
| 4     | 1       |
| 5     | 2       |
| 6     | 3       |
| 7     | 1       |


## Application running and deployment

To try out the application run:

```bash
  sbt "run {inputFilePath} {outputFilePath}"
```
sample for input/output file path:
`sparkAssessmentBucket/filePath`

where 

`sparkAssessmentBucket` = bucket name

`filePath` = folder name

To deploy application follow:

```sh
sbt package
```

extract from `sparkassessment_2.13-0.1.jar`

`/target/scala-2.13/`
## Environment Variables

To run this project, you will need to add the following environment variables to your `.aws/credentials` file

`aws_access_key_id` 

`aws_secret_access_key`

Also it is assumed that the AWS user and the bucket provided has permission policy that allows read, create, delete on objects 
required in the bucket.