//
// Created by hsiaoko on 2022/3/18.
//
#pragma once

#include <string>

struct CSRPt {
  std::string vertex_pt;
  std::string meta_in_pt;
  std::string meta_out_pt;
  std::string vdata_pt;
  std::string localid2globalid_pt;
  std::string msg_pt;
};

// reference http://www.cs.cmu.edu/~pbbs/benchmarks/graphIO.html
enum GraphFormat { edge_graph_csv, weight_edge_graph_csv, csr_bin };