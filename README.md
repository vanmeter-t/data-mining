# CS234 Final Project: Data Mining Traffic Accident Data

Trevor Van Meter

## Installation

1. Startup a PostgreSQL database instance
2. Execute the cs235_project-1.0-SNAPSHOT.one-jar.jar (see Usage)

## Usage

1. Execute the `cs235_project-1.0-SNAPSHOT.one-jar.jar [0] [1] [2] [3]`:
    1. [0] - PostgreSQL address, port, user (and password if necessary)
        - "localhost:32770/postgres?user=postgres"
    2. [1] - Input data file
        - src/main/resources/Collisions_20092013_SWITRS.csv
    3. [2] - Output location for results
        - out/out.txt
    4. [3] - Output location for grid clustering results in .tsv format
      - out/grid.tsv

```cmd
 java -jar target/cs235_project-1.0-SNAPSHOT.one-jar.jar "localhost:32770/postgres?user=postgres" src/main/resources/Collisions_20092013_SWITRS.csv out/out.txt out/grid.tsv
```


## Output

1. out/out.txt
    - Contains the results for the classifications, and accuracy
2. out/grid.tsv
    - Contains the count and geometry for the grid based clustering
