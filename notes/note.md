# Optimisation of join query over distributed database

## Context

In large-scale database systems, our aim is to enhance query performance through optimized join algorithms. 
Various algorithms exist, each with its own set of advantages and drawbacks.

### Glossary

- **Cluster**: Refers to a database system comprising multiple nodes. It stores data and executes queries across these servers.
- **Node**: Represents a unique server within the database system. Multiple nodes make up the distributed database system and handle query processing.
A node accepts and stores a subset of the database. During query execution, it exchanges data with other nodes to generate a partial result set, which it then sends to the driver.
- **Driver**: Acts as a server responsible for managing other nodes. It dispatches subsets of tables or rows to other nodes and aggregates query results from each node to produce the final result.

### Assumption

- Datas are randomly distributed across the nodes.
- The data quantity is evenly distributed across each node.

> Note:
> On hadoop, the data are split on block of 128Mb of data.

## Join strategy

### Broadcast join

In this join strategy, we distribute one of the two tables across all nodes. 
This allows each node to process the query using its subset of the first table. Subsequently, all the sub-results are collected by the driver and merged together

![Simple md image](simple-bc-join.png)

In this example the table country is broadcast to all other nodes.

The effectiveness of a broadcast join heavily relies on the size of the broadcasted table. When a table with N rows is broadcasted across a system with M nodes, the network needs to transfer N * M rows. Additionally, each node must accommodate these rows in memory and execute a join operation with all the broadcasted table's rows. Assuming the broadcasted table has N rows, the other table within each node has K rows, and the join operation employs a double nested loop, the workload on each node becomes N * K.

### Shuffle join

In this join strategy, we select nodes to execute a join operation over a subset of data. There are various methods to partition the data and distribute it among the nodes.

For example, consider the following relation:

~~~mermaid
erDiagram
    country {
        int id PK
        string name
        int country_id FK
    }
    
    city {
        int id PK
        string name
    }
    
    city }|--|| country : "is part of"
~~~

If we want to perform :

~~~s
SELECT * 
FROM city
INNER JOIN country city.country_id == country.id
~~~

Each node can implement the following algorithm to shuffle they datas:

~~~{r, eval = FALSE}
INPUT :
    M <- Number of node
    T <- Tables in our node
    n <- Name of the column we want to join on
    curr_node <- Current node
    
START :
    for table in T do
        for row in table do
            affected_node = row[n] % M
            if curr_node != affected_node do
                sendRowToNode(affected_node, table.name, row)
                remove(T[table][row])
            end if
        end for
    end for
~~~

![Simple md image](simple-shuffle-join.png)

In the context of minimizing computing time in shuffle join, the optimal data distribution involves avoiding heavy hitters, such as countries with a large number of cities.

## TinyDistribDB

To assess the performance of different join strategies on distributed databases, I developed TinyDistribDB, an in-memory distributed database virtual environment. 
It monitors each event that occurs during the execution of a join query.

### Assumption

- Data is randomly distributed across the nodes.
- The quantity of data is evenly distributed across each node.
- Network workload is defined as the number of rows transferred over the network.
- Node workload is represented by the computational cost of a double nested loop between two tables. This excludes considerations of reading, writing, and I/O operations.
- Cluster workload is represented by the maximum cost among all its nodes. Assuming all nodes are equal and perform the join operation in parallel.

## Experiment

Find here the description of some experiment.

### Exp. 6

Parameters of the experiment :
 - table_a have 50 rows 
 - table_b have 100 rows
 - table_a hold the pk
 - table_b hold the fk
 - 5 nodes are placed in the Cluster

Progress :

We begin by selecting FK columns in table B, with IDs uniformly distributed across the IDs in table A. This simulates a join scenario without heavy hitters.

Subsequently, after each loop iteration, we replace an ID in the FK column with ID 0 (which exists in table A). This simulates the introduction of a growing heavy hitter. Following each loop, we record the workload on the system and visualize the results.

The process is repeated for each join strategy.

Result :

![growing heavy hitter](growing_heavy_hitter.png)

For broadcast join :
 - The cluster and network workload remains constant as the table sizes do not change. Therefore, the broadcasted table over the network remains the same, maintaining a high network workload, while the data within each node remains unchanged throughout the iterations.

For shuffle join :
 - The network workload remains constant because the data are randomly distributed among the nodes, and the total number of rows remains unchanged.
 - As the size of the heavy hitter increases, the cluster workload also increases. This is because the larger the heavy hitter, the greater the number of rows shuffled within the same nodes, leading to an increased workload on these nodes.

For the flow join :
 - Since we can identify which ID is a heavy hitter, we can mark it for the flow join.
 - The cluster workload remains unchanged because the heavy hitter is no longer shuffled or broadcasted; it is only broadcasted in the table_a.
 - The network workload decreases because all the rows referenced by the heavy hitters are no longer exchanged.

## PhoneBill benchmark on g5k

### Result 1

> Note : no bandwidth restriction was applied

||UNIFORM_sigma0.0_mu0.0|LOGNORMAL_sigma1.0_mu3.0|LOGNORMAL_sigma18.0_mu3.0|LOGNORMAL_sigma36.0_mu3.0|
|---|---|---|---|---|
|shuffle 0|40.681870222091675|34.8124258518219|34.05663728713989|34.80178642272949|
|shuffle 1|35.3464560508728|35.56805396080017|33.929023027420044|33.758228063583374|
|shuffle 2|35.139564752578735|35.334702491760254|34.30987739562988|36.0113468170166|
|shuffle mean|37.05596367518107|35.23839410146078|34.09851257006327|34.857120434443154|
|broadcast 0|35.285696268081665|35.273019552230835|34.764941930770874|36.331093311309814|
|broadcast 1|35.697840213775635|35.56048107147217|32.95981025695801|35.167991638183594|
|broadcast 2|34.717456102371216|34.546655893325806|35.28314137458801|35.031708002090454|
|broadcast mean|35.233664194742836|35.126718839009605|34.3359645207723|35.51026431719462|

### Result 2

> Note : with 30M datas on tables history

||UNIFORM_sigma0.0_mu0.0|LOGNORMAL_sigma1.0_mu3.0|LOGNORMAL_sigma18.0_mu3.0|LOGNORMAL_sigma36.0_mu3.0|
|---|---|---|---|---|
|shuffle 0|41.95712375640869|35.85488533973694|37.34940266609192|35.101518392562866|
|shuffle 1|35.82039952278137|34.95063042640686|34.22095060348511|35.46870732307434|
|shuffle 2|35.83522963523865|35.38547420501709|35.915430307388306|35.37380886077881|
|shuffle mean|37.870917638142906|35.39699665705363|35.82859452565511|35.31467819213867|
|broadcast 0|34.993990421295166|35.49893260002136|35.09304165840149|34.6880156993866|
|broadcast 1|34.643205642700195|34.10826396942139|34.862433195114136|34.56446933746338|
|broadcast 2|34.65258312225342|34.594542503356934|34.67771935462952|36.02084708213806|
|broadcast mean|34.763259728749595|34.73391302426656|34.87773140271505|35.091110706329346|
