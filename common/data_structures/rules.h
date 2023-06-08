#ifndef HMG_COMMON_RULES_H_
#define HMG_COMMON_RULES_H_

#include "yaml-cpp/yaml.h"
#include <string>

class Predicate {
 public:
  // Closure *c1 = nullptr;
  // Closure *c2 = nullptr;
  // vector<Attribute *> c1a;
  // vector<Attribute *> c2a;

  // void resolve_config(YAML::Node config, vector<Closure *> &closures);
};

class Equality : public Predicate {};
class ML : public Predicate {};
class Constant : public Predicate {};
class Conseq : public Predicate {};

class Rule {
  Rule(std::string& rule_file) { std::vector<Equality> equalities; };

 public:
  YAML::Node rule_config_;
};

#endif  // HMG_COMMON_RULES_H_
