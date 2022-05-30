#pragma once

#include <sys/types.h>

#include <gflags/gflags.h>

using gid_t = unsigned;
using vid_t = unsigned;
using vdata_t = float;
using edata_t = size_t;

#define VID_MAX (((unsigned)(-1)) >> 1)
#define MINIGRAPH_GID_MAX (((unsigned)(-1)) >> 1)

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
#define FIXPOINT 'F'
#define GOON 'G'

// Rename states for state machine.
#define IDLE 'I'
#define ACTIVE 'A'
#define RT 'R'
#define RTS 'S'
#define RC 'C'
#define TERMINATE 'X'

DEFINE_string(i, "", "input path");
DEFINE_string(o, "", "output path");
DEFINE_bool(tobin, false, "convert the graph to binary format");
DEFINE_bool(p, false, "partition input graph");
DEFINE_string(t, "edgelist", "type");
DEFINE_uint64(n, 1, "the number of fragments");
DEFINE_uint64(vertexes, 1, "the number of vertexes");
DEFINE_uint64(power, 1, "the power of 2");
DEFINE_uint64(edges, 1, "the number of edges");
DEFINE_uint64(lc, 1, "the number of executors in LoadComponent");
DEFINE_uint64(cc, 1, "the number of executors in ComputingComponent");
DEFINE_uint64(dc, 1, "the number of executors in DischargeComponent");
DEFINE_uint64(threads, 4, "the sizeof CPUThreadPool");
DEFINE_uint64(iter, 4, "number of iterations for inner while loop");
DEFINE_string(init_model, "val", "init model for vdata of all vertexes");
DEFINE_uint64(init_val, 0, "init value for vdata of all vertexes");
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
