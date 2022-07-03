#ifndef RDKAFKA4ESL_PLUGIN_H_
#define RDKAFKA4ESL_PLUGIN_H_

#include <esl/plugin/Registry.h>

namespace rdkafka4esl {

class Plugin final {
public:
	Plugin() = delete;
	static void install(esl::plugin::Registry& registry, const char* data);
};

} /* namespace rdkafka4esl */

#endif /* RDKAFKA4ESL_PLUGIN_H_ */
