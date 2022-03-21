#ifndef MINIGRAPH_MINIGRAPH_SYS_H
#define MINIGRAPH_MINIGRAPH_SYS_H
#include "components/computing_component.h"
#include "components/discharge_component.h"
#include "components/load_component.h"
#include <memory>

namespace minigraph {

template <typename GID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
class MiniGraphSys {
 private:
  std::unique_ptr<components::LoadComponent<GID_T, VID_T, VDATA_T, EDATA_T>>
      load_component_;
  std::unique_ptr<
      components::ComputingComponent<GID_T, VID_T, VDATA_T, EDATA_T>>
      computing_component_;
  std::unique_ptr<
      components::DischargeComponent<GID_T, VID_T, VDATA_T, EDATA_T>>
      discharge_component_;
};

}  // namespace minigraph
#endif  // MINIGRAPH_MINIGRAPH_SYS_H
