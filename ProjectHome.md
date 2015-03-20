## Overview ##
Points-to analysis is a compiler technique aimed at identifying which variables/objects are pointed by the pointers of the input program. The results of this compiler phase are useful for program optimization, program verification, debugging and whole program comprehension.

The algorithm presented here is a multi-core, CPU implementation of the context and flow insensitive, inclusion-based points-to analysis described by Ben Hardekopf in his PLDI paper `[1]`. Below we provide some basic information about our parallel analysis, please check out our OOPSLA [paper](http://www.clip.dia.fi.upm.es/~mario/files/res0000030-mendezlojo.pdf) `[2]` for more technical details and experimental results.

You might also want to take a look at our [twin project](http://code.google.com/p/andersengpu/), in which the same algorithm is parallelized on the GPU using CUDA. The GPU implementation is faster for most of the input programs.
  1. B. Hardekopf, C. Lin. _The Ant and the Grasshopper: Fast and Accurate Pointer Analysis for Millions of Lines of Code_. PLDI 2007.
  1. M. Méndez-Lojo, A. Mathew, K. Pingali. _Parallel Inclusion-based Points-to Analysis_. OOPSLA 2010.


## Algorithm ##
The input for the points-to algorithm is the program to be analysed. The output is a map from program pointers to the set of variables they might point to. The analysis proceeds as follows:
  1. Extract pointer statements, which for the C language are _x_=&_y_,_x_=_y_,_x_=`*`_y_, and `*`_x_=_y_.
  1. Create the initial constraint graph. Each node corresponds to a pointer variable, and each edge (_x,y_) indicates that _x_ and _y_ appear together in a statement. The edges are tagged with the type of the statement they come from. For instance, a copy instruction such as _x_=_y_ results on a copy edge from _y_ to _x_.
  1. Iterate over the graph until no _rewrite rule_ needs to be applied. A rewrite rule is just the addition of a new points-to or copy edge to the graph, whenever some precondition is met. You can find more about these preconditions in the paper - for now just assume that there are certain pairs of edges that fire the addition of a third edge to the constraint graph. Variables that need to be processed are kept in a worklist.

The following pseudo-code shows the iterative phase of the points-to algorithm.
```

01 foreach (variable y: worklist)
02   foreach (variable x : copy neighbors of v)
03     apply copy_rule(y,x)
04     // copy rule might add a new points-to edge to x
05     if (points-to edge added to x)
06       worklist.add(x)
07   foreach (variable x : load neighbors of v)
08     apply load_rule(y, x) 
09     // load rule might add a new copy edge to another variable z
10     if (copy edge added to z)
11       worklist.add(z)
12   foreach (variable x : store neighbors of v)
13     apply store_rule(y,x) 
14     // store rule might add a new copy edge to x
15     if (copy edge added to x)
16       worklist.add(x)

```
Eventually, the algorithm will reach a fixpoint (=the worklist is empty), so we can read the solution out of the points-to edges of the graph.

**Example:** A program written in C contains only the statements _b_=&_w_;_b_=&_v_;_a_=_b_;. Because there are three pointer statements and another three variables, we initialize (left-most subfigure below) the constraint graph by adding three nodes and edges. The edges are labeled with their type: p means "points-to", while c means "copy". Then we apply the copy rule (lines 02-06) twice to reach fixpoint (right-most subfigure). From the final graph we can infer that the variables that both _a_ and _b_ might point to are _{v,w}_.

<img src='http://www.clip.dia.fi.upm.es/~mario/images/copy_rule_example.jpg' height='120px' />


## Data Structures ##
There are two key data structures:

  * Unordered Set: the worklist containing the active nodes (i.e., the nodes that violate the precondition of a graph rewrite rule). Ideally, we would like to process the active nodes in topological order. In practice, a FIFO scheduling performs much better because it does not constraint parallelism and it has a more efficient implementation.
  * Multi-Graph: both the copy/load/store relationships and the points-to sets are represented using edges. Therefore, there might be multiple edges between two nodes (variables) in the graph. We only store the outgoing edges (neighbours) of a variable; the internal representation depends on the type of edge.
    * BDD: A Binary Decision Diagram is a directed acyclic graph that s used to represent a boolean formula. In our case, the BDD can be interpreted as a function that for a pair (x,y) returns true iff there exists a points-to edge from x to y. The BDD is used then to store the points-to edges of the variables in the program. In order to support concurrent accesses, we implemented it in terms of a concurrent hash map.
    * Sparse bit vector: A sparse bit vector is a linked list used to represent sparse sets of numbers in an ordered fashion. In our case, we use it for storing the load, store and copy neighbours of a node in the graph. Internally, the sparse bit vector is implemented as a lock-free linked list.


## Parallelism ##
The parallel (inclusion-based) points-to analysis does not require the "full power" of a transactional memory system in order to preserve the sequential semantics, but is not an embarrasingly parallel algorithm either. As long as the multi-graph supports concurrent accesses, there is a guarantee that the parallel analysis will reach the same fixpoint as the sequential implementation. In other words, we can apply the graph rewrite rules in any order and the final result will be identical to that of the sequential code (again, assuming that edges additions and the merging of nodes appear to be atomic). The _parallelism intensity_ of the parallel algorithm is 100%, since all the active nodes in the worklist can be safely processed in parallel at any point during execution.


## Usage ##
The following instructions have been tested on a AMD Phenom II X6 1100T machine running Ubuntu 12.04. Requirements: Ant (>= 1.6), JVM (>= 1.6), Python 2.X.

### Compilation ###
Checkout the source (click on the "Source" link above for precise instructions), and then type this at the root directory:
```
ant dist-app -Dapp=hardekopfPointsTo
```

### Running ###
Invoke
```
./scripts/galois -d -f apps/hardekopfPointsTo/conf/${TEST}.properties -r ${REPS} hardekopfPointsTo.main.Main
```
, where `TEST` is the input program and `REPS` is the number of times we want to run the analysis. For instance, if we want to analyze the `python` interpreter, we would type this at the installation directory:
```
./scripts/galois -d -f apps/hardekopfPointsTo/conf/python.properties -r 1 hardekopfPointsTo.main.Main 
```
The distribution includes many other example programs you can analyze (`input/hardekopfPointsTo` directory). For a complete list of runtime options, please type
```
./scripts/galois --help
```