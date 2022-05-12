#pragma once

#include <gflags/gflags.h>
#include <sys/types.h>

#ifdef _WIN32
#include <basetsd.h>  // @manual
#endif

// using gid_t = unsigned;
// using vid_t = unsigned;
// using vdata_t = unsigned;
// using edata_t = unsigned;
using gid_t = unsigned;
using vid_t = unsigned;
using vdata_t = float;
using edata_t = size_t;

#define VID_MAX (((unsigned)(-1)) >> 1)
#define GID_MAX (((unsigned)(-1)) >> 1)

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
#define FIXPOINT 'F'
#define GOON 'G'

// Rename states for state machine.
#define IDLE 'I'
#define ACTIVE 'A'
#define RT 'R'
#define RC 'C'
#define TERMINATE 'X'

DEFINE_string(i, "", "input path");
DEFINE_string(o, "", "output path");
DEFINE_bool(tobin, false, "convert the graph to binary format");
DEFINE_bool(p, false, "partition input graph");
DEFINE_string(t, "edgelist", "type");
DEFINE_uint64(n, 1, "the number of fragments");
DEFINE_uint64(lc, 1, "the number of executors in LoadComponent");
DEFINE_uint64(cc, 1, "the number of executors in ComputingComponent");
DEFINE_uint64(dc, 1, "the number of executors in DischargeComponent");
DEFINE_uint64(threads, 4, "the sizeof CPUThreadPool");
DEFINE_uint64(iter, 4, "number of iterations for inner while loop");
DEFINE_string(init_model, "val", "init model for vdata of all vertexes");
DEFINE_uint64(init_val, 0, "init value for vdata of all vertexes");
