//
// Created by hsiaoko on 2022/3/18.
//
#pragma once

#include <sys/types.h>

#ifdef _WIN32
#include <basetsd.h>  // @manual
#endif

using gid_t = unsigned;
using vid_t = unsigned;
using vdata_t = unsigned;
using edata_t = unsigned;

#ifndef HAVE_MODE_T
#define HAVE_MODE_T 1
// The Windows headers don't define this anywhere, nor do any of the libs
// that MiniGraph depends on, so define it here.
using mode_t = unsigned int;

#endif
