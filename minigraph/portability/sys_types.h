#pragma once

#include <sys/types.h>

#ifdef _WIN32
#include <basetsd.h>  // @manual
#endif

// using gid_t = unsigned;
// using vid_t = unsigned;
// using vdata_t = unsigned;
// using edata_t = unsigned;
using gid_t = unsigned;
using vid_t = size_t;
using vdata_t = size_t;
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