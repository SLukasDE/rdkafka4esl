#include <rdkafka4esl/Plugin.h>

#include <esl/plugin/Registry.h>

extern "C" void esl__plugin__library__install(esl::plugin::Registry* registry, const char* data) {
	if(registry != nullptr) {
		rdkafka4esl::Plugin::install(*registry, data);
	}
}
