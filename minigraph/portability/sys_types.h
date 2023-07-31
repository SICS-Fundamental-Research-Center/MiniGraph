#pragma once

#include <sys/types.h>

#include <gflags/gflags.h>

using gid_t = unsigned;
using vid_t = unsigned;
using vdata_t = unsigned;
using edata_t = unsigned;

#define VID_MAX 0XFFFFFFFF
#define VDATA_MAX 0XFFFFFFF
#define EDATA_MAX 0XFFFFFFF
#define GID_MAX 0XFFFFFFF
#define MINIGRAPH_GID_MAX (((unsigned)(-1)) >> 1)
#define ALIGNMENT_FACTOR (double)64.0
#define NUM_NEW_BUCKETS 1


#ifndef HAVE_MODE_T
#define HAVE_MODE_T 1
#endif

// The Windows or Linux headers don't define this anywhere, nor do any of the
// libs that MiniGraph depends on, so define it here.

// Rename events for state machine.
#define LOAD 'L'
#define UNLOAD 'U'
#define NOTHINGCHANGED 'N'
#define CHANGED 'C'
#define AGGREGATE 'A'
#define SHORTCUT 'S'
#define SHORTCUTREAD 'J'
#define FIXPOINT 'F'
#define GOON 'G'

// Rename states for state machine.
#define IDLE 'I'
#define ACTIVE 'A'
#define RT 'R'
#define RTS 'S'
#define RC 'C'
#define TERMINATE 'X'

// Rename states for vertex.
#define VERTEXLABELED 'L'
#define VERTEXUNLABELED 'U'
#define VERTEXSCANNED 'S'
#define VERTEXMATCH 'M'
#define VERTEXDISMATCH 'D'
#define VERTEXACTIVE 'A'
#define VERTEXINACTIVE 'I'

DEFINE_string(i, "", "input path");
DEFINE_string(o, "", "output path");
DEFINE_string(pattern, "", "query graph (edge list in csv)");
DEFINE_bool(tobin, false, "convert the graph to binary format");
DEFINE_bool(frombin, false, "convert the graph of binary format");
DEFINE_string(
    gtype, "csr_bin",
    "two types of edge list files are supported: csr_bin, edge_list_bin");
DEFINE_bool(p, false, "partition input graph");
DEFINE_string(t, "edgelist", "type");
DEFINE_string(in_type, "edgelist", "type");
DEFINE_string(out_type, "edgelist", "type");
DEFINE_string(sep, "seperator", ",");
DEFINE_uint64(n, 1, "the number of fragments");
DEFINE_uint64(vertexes, 0, "the number of vertexes");
DEFINE_uint64(edges, 0, "the number of edges");
DEFINE_uint64(power, 1, "the power of 2");
DEFINE_uint64(lc, 1, "the number of executors in LoadComponent");
DEFINE_uint64(cc, 1, "the number of executors in ComputingComponent");
DEFINE_uint64(dc, 1, "the number of executors in DischargeComponent");
DEFINE_uint64(cores, 4, "the number of cores we used");
DEFINE_uint64(buffer_size, 1, "buffer size");
DEFINE_uint64(niters, 50, "number of iterations for graph-level while loop");
DEFINE_uint64(walks_per_source, 5, "walks per source vertex for random walk");
DEFINE_uint64(inner_niters, 4, "number of iterations for inner while loop");
DEFINE_string(init_model, "val", "init model for vdata of all vertexes");
DEFINE_string(mode, "default", "MiniGraph with entire optimization");
DEFINE_string(partitioner, "edgecut",
              "graph partition solutions include vertexcut, edgecut");
DEFINE_string(scheduler, "FIFO",
              "subgraphs scheduler include FIFO, hash, learned_model");
DEFINE_uint64(init_val, 0, "init value for vdata of all vertexes");
DEFINE_uint64(walsk_per_source, 1, "walks per vertex for random walks application.");
DEFINE_uint64(root, 0, "the id of root vertex");
DEFINE_double(
    a, 0.25,
    "probability of an edge falling into partition a in the R-MAT model");
DEFINE_double(
    b, 0.25,
    "probability of an edge falling into partition b in the R-MAT model");
DEFINE_double(
    c, 0.25,
    "probability of an edge falling into partition c in the R-MAT model");
DEFINE_double(
    d, 0.25,
    "probability of an edge falling into partition d in the R-MAT model");
DEFINE_double(
    x, 0.5,
    "probability of an edge falling into partition b d in the R-MAT model");
DEFINE_double(
    y, 0.5,
    "probability of an edge falling into partition c d in the R-MAT model");
