//#include "libutil/thread.h"

#include <boost/config.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/version.hpp>
#include <iostream>

using namespace std;

int main() {
  using boost::lexical_cast;
  int a = lexical_cast<int>("123456");
  double b = lexical_cast<double>("123.456");
  std::cout << a << std::endl;
  std::cout << b << std::endl;
  std::cout << "Using Boost "
            << BOOST_VERSION / 100000 << "."  // maj. version
            << BOOST_VERSION / 100 % 1000 << "."  // min. version
            << BOOST_VERSION % 100  // patch version
            << std::endl;
  return 0;
}